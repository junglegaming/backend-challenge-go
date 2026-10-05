// Package e2e runs the real application binary as separate operating system
// processes against a real Postgres and a real Keycloak. It proves what an
// in-process test cannot: that coordination and idempotency live in the
// database, not in the memory of one process.
//
// The SQS consumer is not under test here. Each process points at an address
// where nothing listens, so the consumer logs an error every second and the
// HTTP side works normally.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/testutil"
)

var (
	keycloak *testutil.Keycloak
	binary   string
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

// run builds the application once and starts Keycloak once for the package.
func run(m *testing.M) int {
	dir, err := os.MkdirTemp("", "e2e-bin-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "temp dir: %v\n", err)
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()

	if binary, err = buildAPI(dir); err != nil {
		fmt.Fprintf(os.Stderr, "build api: %v\n", err)
		return 1
	}

	ctx := context.Background()
	if keycloak, err = testutil.StartKeycloak(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "start keycloak: %v\n", err)
		return 1
	}
	defer func() { _ = keycloak.Terminate(ctx) }()

	return m.Run()
}

func buildAPI(dir string) (string, error) {
	name := "api"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	out := filepath.Join(dir, name)

	_, thisFile, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(thisFile), "..", "..")

	cmd := exec.Command("go", "build", "-o", out, "./cmd/api")
	cmd.Dir = root
	if output, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("%w: %s", err, output)
	}
	return out, nil
}

// lockedBuffer collects a process's output. exec writes to it from its own
// goroutine, so reads and writes need a lock.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// instance is one running copy of the application.
type instance struct {
	cmd  *exec.Cmd
	logs *lockedBuffer
	base string
	done chan struct{}
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port
}

// startInstance runs the binary as its own process and waits until it serves.
// It is killed when the test ends, so no process is left holding connections.
func startInstance(t *testing.T, dsn string) *instance {
	t.Helper()
	port := freePort(t)

	cmd := exec.Command(binary)
	// An empty working directory means no .env file is read by accident.
	cmd.Dir = t.TempDir()
	cmd.Env = append(os.Environ(),
		"DATABASE_URL="+dsn,
		fmt.Sprintf("HTTP_ADDR=127.0.0.1:%d", port),
		"AUTH_ISSUER="+keycloak.Issuer,
		"AUTH_JWKS_URL="+keycloak.JWKSURL(),
		"SQS_QUEUE_URL=http://127.0.0.1:1/000000000000/wager-transactions.fifo",
		"SQS_ENDPOINT=http://127.0.0.1:1",
		"AWS_REGION=us-east-1",
		"AWS_ACCESS_KEY_ID=test",
		"AWS_SECRET_ACCESS_KEY=test",
	)
	logs := &lockedBuffer{}
	cmd.Stdout, cmd.Stderr = logs, logs

	if err := cmd.Start(); err != nil {
		t.Fatalf("start process: %v", err)
	}
	inst := &instance{cmd: cmd, logs: logs, base: fmt.Sprintf("http://127.0.0.1:%d", port), done: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		close(inst.done)
	}()
	t.Cleanup(func() { inst.kill() })

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-inst.done:
			t.Fatalf("process exited while starting:\n%s", logs.String())
		default:
		}
		resp, err := http.Get(inst.base + "/health/live")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return inst
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("process did not become live:\n%s", logs.String())
	return nil
}

// kill ends the process abruptly, with no graceful shutdown: the equivalent of
// kill -9. It is safe to call more than once.
func (i *instance) kill() {
	_ = i.cmd.Process.Kill()
	<-i.done
}

// cluster is a Postgres plus the processes started against it.
type cluster struct {
	t        *testing.T
	dsn      string
	pool     *pgxpool.Pool
	service  string
	provider string
}

func newCluster(t *testing.T) *cluster {
	t.Helper()
	ctx := context.Background()

	dsn := testutil.StartPostgres(t)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	service, err := testutil.FetchToken(ctx, keycloak.Issuer, "wallet-service", "wallet-service-dev-secret")
	if err != nil {
		t.Fatalf("service token: %v", err)
	}
	provider, err := testutil.FetchToken(ctx, keycloak.Issuer, "provider-a", "provider-a-dev-secret")
	if err != nil {
		t.Fatalf("provider token: %v", err)
	}
	return &cluster{t: t, dsn: dsn, pool: pool, service: service, provider: provider}
}

func (c *cluster) start(n int) []*instance {
	out := make([]*instance, n)
	for i := range out {
		out[i] = startInstance(c.t, c.dsn)
	}
	return out
}

type wallet struct{ id, player string }

type outcome struct {
	HTTPStatus       int
	TransactionID    string `json:"transactionId"`
	Status           string `json:"status"`
	FailureCode      string `json:"failureCode"`
	IdempotentReplay bool   `json:"idempotentReplay"`
	Balance          struct {
		Amount string `json:"amount"`
	} `json:"balance"`
}

// do sends a request and decodes the JSON answer into out, if given.
func do(t *testing.T, method, url, token, key string, body any, out any) int {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Errorf("marshal: %v", err)
			return 0
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Errorf("request: %v", err)
		return 0
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Errorf("%s %s: %v", method, url, err)
		return 0
	}
	defer func() { _ = resp.Body.Close() }()
	if out != nil {
		_ = json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode
}

func (c *cluster) openWallet(inst *instance, amount string) wallet {
	c.t.Helper()
	w := wallet{player: uuid.NewString()}
	var out struct {
		ID string `json:"id"`
	}
	status := do(c.t, http.MethodPost, inst.base+"/wallets", c.service, "", map[string]any{
		"playerId":       w.player,
		"initialBalance": map[string]string{"amount": amount, "currency": "BRL"},
	}, &out)
	if status != http.StatusCreated || out.ID == "" {
		c.t.Fatalf("open wallet: status %d", status)
	}
	w.id = out.ID
	return w
}

// bet submits one BET. The externalTransactionId is the key without its prefix,
// so the same (ext, key) pair is the same operation.
func (c *cluster) bet(inst *instance, w wallet, ext, amount string) outcome {
	c.t.Helper()
	var out outcome
	out.HTTPStatus = do(c.t, http.MethodPost, inst.base+"/wagering/transactions", c.provider, "provider-a:"+ext, map[string]any{
		"providerId":            "provider-a",
		"externalTransactionId": ext,
		"playerId":              w.player,
		"walletId":              w.id,
		"roundId":               "round-1",
		"gameId":                "game-1",
		"kind":                  "BET",
		"money":                 map[string]string{"amount": amount, "currency": "BRL"},
	}, &out)
	return out
}

func (c *cluster) balance(inst *instance, w wallet) string {
	c.t.Helper()
	var out struct {
		Balance struct {
			Amount string `json:"amount"`
		} `json:"balance"`
	}
	if status := do(c.t, http.MethodGet, inst.base+"/wallets/"+w.id, c.service, "", nil, &out); status != http.StatusOK {
		c.t.Fatalf("read wallet: status %d", status)
	}
	return out.Balance.Amount
}

// assertLedgerMatchesBalance fails if any wallet's stored balance differs from
// its ledger: credits minus debits. It reads the database directly.
func (c *cluster) assertLedgerMatchesBalance() {
	c.t.Helper()
	rows, err := c.pool.Query(context.Background(), `
              SELECT w.id::text FROM wallets w
              LEFT JOIN wallet_ledger_entries e ON e.wallet_id = w.id
              GROUP BY w.id, w.balance_minor
              HAVING w.balance_minor <> COALESCE(SUM(CASE e.direction WHEN 'CREDIT' THEN e.amount_minor ELSE -e.amount_minor END), 0)`)
	if err != nil {
		c.t.Fatalf("ledger check: %v", err)
	}
	defer rows.Close()
	var bad []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			c.t.Fatalf("scan: %v", err)
		}
		bad = append(bad, id)
	}
	if len(bad) > 0 {
		c.t.Fatalf("wallets whose balance does not match the ledger: %s", strings.Join(bad, ", "))
	}
}

// Three bets of 80.00 on a balance of 100.00, each sent to a different process.
// Only the database can decide who wins.
//
// One wallet is a race that lasts milliseconds, and the three requests rarely
// overlap, so a single wallet would pass even without the row lock. The test
// opens many wallets and releases every request at the same instant, so the
// reads and writes really do overlap. Without FOR UPDATE this test fails.
func TestThreeProcesses_OnlyOneBetOfEightyIsApplied(t *testing.T) {
	const wallets = 30

	c := newCluster(t)
	nodes := c.start(3)

	ws := make([]wallet, wallets)
	for i := range ws {
		ws[i] = c.openWallet(nodes[0], "100.00")
	}

	results := make([][]outcome, wallets)
	for i := range results {
		results[i] = make([]outcome, len(nodes))
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for wi := range ws {
		for ni := range nodes {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				results[wi][ni] = c.bet(nodes[ni], ws[wi], fmt.Sprintf("bet-%d-%d", wi, ni), "80.00")
			}()
		}
	}
	close(start)
	wg.Wait()

	for wi, w := range ws {
		var processed, rejected int
		for ni, r := range results[wi] {
			switch {
			case r.HTTPStatus == http.StatusCreated && r.Status == "PROCESSED":
				processed++
			case r.HTTPStatus == http.StatusUnprocessableEntity && r.FailureCode == "insufficient_funds":
				rejected++
			default:
				t.Errorf("wallet %d, process %d: unexpected answer %+v", wi, ni, r)
			}
		}
		if processed != 1 || rejected != 2 {
			t.Errorf("wallet %d: processed=%d rejected=%d, want 1 and 2", wi, processed, rejected)
		}
		if got := c.balance(nodes[1], w); got != "20.00" {
			t.Errorf("wallet %d: balance = %s, want 20.00", wi, got)
		}
	}
	c.assertLedgerMatchesBalance()
}

// The same operation, with the same key, sent 50 times across three processes
// at once. Exactly one is the original; the rest are replays.
func TestThreeProcesses_SameKeyFiftyTimesAppliesOnce(t *testing.T) {
	c := newCluster(t)
	nodes := c.start(3)
	w := c.openWallet(nodes[0], "1000.00")

	const total = 50
	results := make([]outcome, total)
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = c.bet(nodes[i%len(nodes)], w, "same-operation", "25.00")
		}()
	}
	wg.Wait()

	var originals, replays int
	first := ""
	for i, r := range results {
		if r.HTTPStatus != http.StatusCreated {
			t.Fatalf("request %d: status %d", i, r.HTTPStatus)
		}
		if first == "" {
			first = r.TransactionID
		}
		if r.TransactionID != first {
			t.Fatalf("request %d saw another operation: %s != %s", i, r.TransactionID, first)
		}
		if r.IdempotentReplay {
			replays++
		} else {
			originals++
		}
	}
	if originals != 1 || replays != total-1 {
		t.Fatalf("originals=%d replays=%d, want 1 and %d", originals, replays, total-1)
	}
	if got := c.balance(nodes[0], w); got != "975.00" {
		t.Fatalf("balance = %s, want 975.00", got)
	}
	c.assertLedgerMatchesBalance()
}

// A process killed without any shutdown loses its memory. The idempotency key
// lives in the database, so a new process answers the same request with the
// stored outcome instead of applying it again.
func TestKilledProcess_NewProcessReplaysTheStoredOutcome(t *testing.T) {
	c := newCluster(t)
	first := startInstance(t, c.dsn)
	w := c.openWallet(first, "1000.00")

	original := c.bet(first, w, "survives-a-crash", "25.00")
	if original.HTTPStatus != http.StatusCreated || original.IdempotentReplay {
		t.Fatalf("original: %+v", original)
	}

	first.kill()
	second := startInstance(t, c.dsn)

	replay := c.bet(second, w, "survives-a-crash", "25.00")
	if replay.HTTPStatus != http.StatusCreated || !replay.IdempotentReplay {
		t.Fatalf("replay: %+v, want a replay", replay)
	}
	if replay.TransactionID != original.TransactionID {
		t.Fatalf("transaction changed across the restart: %s != %s", replay.TransactionID, original.TransactionID)
	}
	if replay.Balance.Amount != "975.00" || c.balance(second, w) != "975.00" {
		t.Fatalf("balance = %s, want 975.00 (debited once)", c.balance(second, w))
	}
	c.assertLedgerMatchesBalance()
}

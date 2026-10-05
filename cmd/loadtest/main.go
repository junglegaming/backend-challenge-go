// Command loadtest drives a running stack (docker compose up) with BET
// operations and reports throughput, latency percentiles, status codes and a
// balance check against what the API said it applied.
//
// Two scenarios:
//
//	many   - operations spread over -wallets wallets (scale: wallets do not contend)
//	single - every operation hits one wallet (contention: FOR UPDATE serialises them)
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	betMinor      = 100 // every BET is 1.00
	initialAmount = "1000000.00"
	initialMinor  = 100_000_000
)

type config struct {
	base        string
	issuer      string
	scenario    string
	wallets     int
	workers     int
	duration    time.Duration
	replayRatio float64
	seed        int64
	outDir      string
	note        string
}

func main() {
	var cfg config
	flag.StringVar(&cfg.base, "base", "http://localhost:8080", "API base URL")
	flag.StringVar(&cfg.issuer, "issuer", "http://localhost:8081/realms/wallet", "Keycloak realm URL")
	flag.StringVar(&cfg.scenario, "scenario", "many", "many or single")
	flag.IntVar(&cfg.wallets, "wallets", 50, "wallets used by the many scenario")
	flag.IntVar(&cfg.workers, "workers", 32, "concurrent clients")
	flag.DurationVar(&cfg.duration, "duration", 60*time.Second, "how long to drive load")
	flag.Float64Var(&cfg.replayRatio, "replay-ratio", 0.1, "share of requests that repeat the previous one (idempotent replays)")
	flag.Int64Var(&cfg.seed, "seed", 1, "random seed, for a repeatable request pattern")
	flag.StringVar(&cfg.outDir, "out", "docs/bench", "directory for the JSON report")
	flag.StringVar(&cfg.note, "note", "", "free text recorded in the report, e.g. the machine")
	flag.Parse()

	if cfg.scenario != "many" && cfg.scenario != "single" {
		fatalf("scenario must be many or single")
	}
	if err := run(cfg); err != nil {
		fatalf("%v", err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "loadtest: "+format+"\n", args...)
	os.Exit(1)
}

// tokens keeps fresh access tokens. They last 5 minutes, so a refresher
// replaces them every minute and a long run never sends an expired one.
type tokens struct {
	provider atomic.Value
	service  atomic.Value
}

func (t *tokens) refresh(hc *http.Client, issuer string) error {
	p, err := fetchToken(hc, issuer, "provider-a", "provider-a-dev-secret")
	if err != nil {
		return err
	}
	s, err := fetchToken(hc, issuer, "wallet-service", "wallet-service-dev-secret")
	if err != nil {
		return err
	}
	t.provider.Store(p)
	t.service.Store(s)
	return nil
}

func fetchToken(hc *http.Client, issuer, client, secret string) (string, error) {
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {client}, "client_secret": {secret}}
	resp, err := hc.PostForm(issuer+"/protocol/openid-connect/token", form)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token for %s: status %d: %s", client, resp.StatusCode, body)
	}
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", err
	}
	return out.AccessToken, nil
}

type wallet struct {
	id       string
	playerID string
	applied  atomic.Int64 // unique BETs the API answered 201 to
}

func run(cfg config) error {
	hc := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.workers * 2,
			MaxIdleConnsPerHost: cfg.workers * 2,
		},
	}

	var tk tokens
	if err := tk.refresh(hc, cfg.issuer); err != nil {
		return fmt.Errorf("get tokens (is the stack up?): %w", err)
	}
	stopRefresh := make(chan struct{})
	go func() {
		tick := time.NewTicker(time.Minute)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				if err := tk.refresh(hc, cfg.issuer); err != nil {
					fmt.Fprintf(os.Stderr, "token refresh failed: %v\n", err)
				}
			case <-stopRefresh:
				return
			}
		}
	}()
	defer close(stopRefresh)

	count := cfg.wallets
	if cfg.scenario == "single" {
		count = 1
	}
	wallets, err := openWallets(hc, cfg.base, &tk, count)
	if err != nil {
		return err
	}
	fmt.Printf("scenario=%s wallets=%d workers=%d duration=%s replay-ratio=%.2f\n",
		cfg.scenario, len(wallets), cfg.workers, cfg.duration, cfg.replayRatio)

	stats := drive(hc, cfg, &tk, wallets)
	stats.print()

	consistency := verify(hc, cfg.base, &tk, wallets)
	fmt.Printf("balance check: %s\n", consistency.summary())

	return writeReport(cfg, stats, consistency)
}

func openWallets(hc *http.Client, base string, tk *tokens, n int) ([]*wallet, error) {
	out := make([]*wallet, 0, n)
	for i := 0; i < n; i++ {
		player := uuid.NewString()
		body, _ := json.Marshal(map[string]any{
			"playerId":       player,
			"initialBalance": map[string]string{"amount": initialAmount, "currency": "BRL"},
		})
		status, resp, err := do(hc, http.MethodPost, base+"/wallets", tk.service.Load().(string), "", body)
		if err != nil || status != http.StatusCreated {
			return nil, fmt.Errorf("open wallet: status %d err %v body %s", status, err, resp)
		}
		var w struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(resp, &w); err != nil {
			return nil, err
		}
		out = append(out, &wallet{id: w.ID, playerID: player})
	}
	return out, nil
}

// do sends one request and returns the status and body.
func do(hc *http.Client, method, target, token, idemKey string, body []byte) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, target, reader)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, b, err
}

type stats struct {
	elapsed   time.Duration
	latencies []time.Duration
	statuses  map[int]int
	errs      map[string]int
}

func drive(hc *http.Client, cfg config, tk *tokens, wallets []*wallet) stats {
	runID := strconv.FormatInt(time.Now().Unix(), 36)
	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		out = stats{statuses: map[int]int{}, errs: map[string]int{}}
	)
	begin := time.Now()
	deadline := begin.Add(cfg.duration)

	for w := 0; w < cfg.workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(cfg.seed), uint64(w))) //nolint:gosec // load generator
			var (
				lat      []time.Duration
				statuses = map[int]int{}
				errs     = map[string]int{}
				seq      int
				lastBody []byte
				lastKey  string
			)
			for time.Now().Before(deadline) {
				replay := lastKey != "" && rng.Float64() < cfg.replayRatio
				var idx int
				var body []byte
				key := lastKey
				if replay {
					body = lastBody
				} else {
					seq++
					idx = rng.IntN(len(wallets))
					ext := fmt.Sprintf("lt-%s-%d-%d", runID, w, seq)
					key = "provider-a:" + ext
					body, _ = json.Marshal(map[string]any{
						"providerId":            "provider-a",
						"externalTransactionId": ext,
						"playerId":              wallets[idx].playerID,
						"walletId":              wallets[idx].id,
						"roundId":               "round-lt",
						"gameId":                "game-lt",
						"kind":                  "BET",
						"money":                 map[string]string{"amount": "1.00", "currency": "BRL"},
					})
				}

				t0 := time.Now()
				status, _, err := do(hc, http.MethodPost, cfg.base+"/wagering/transactions",
					tk.provider.Load().(string), key, body)
				lat = append(lat, time.Since(t0))

				if err != nil {
					errs[classify(err)]++
					continue
				}
				statuses[status]++
				if !replay {
					if status == http.StatusCreated {
						wallets[idx].applied.Add(1)
					}
					lastBody, lastKey = body, key
				}
			}
			mu.Lock()
			defer mu.Unlock()
			out.latencies = append(out.latencies, lat...)
			for k, v := range statuses {
				out.statuses[k] += v
			}
			for k, v := range errs {
				out.errs[k] += v
			}
		}()
	}
	wg.Wait()
	out.elapsed = time.Since(begin)
	sort.Slice(out.latencies, func(i, j int) bool { return out.latencies[i] < out.latencies[j] })
	return out
}

func classify(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "Client.Timeout") || strings.Contains(msg, "timeout"):
		return "client_timeout"
	case strings.Contains(msg, "refused"):
		return "connection_refused"
	case strings.Contains(msg, "reset") || strings.Contains(msg, "forcibly"):
		return "connection_reset"
	default:
		return "transport_error"
	}
}

func (s stats) percentile(p float64) time.Duration {
	if len(s.latencies) == 0 {
		return 0
	}
	i := int(float64(len(s.latencies)) * p)
	if i >= len(s.latencies) {
		i = len(s.latencies) - 1
	}
	return s.latencies[i]
}

func (s stats) rps() float64 { return float64(len(s.latencies)) / s.elapsed.Seconds() }

func (s stats) print() {
	round := func(d time.Duration) time.Duration { return d.Round(100 * time.Microsecond) }
	fmt.Printf("requests=%d rps=%.0f p50=%s p95=%s p99=%s max=%s\nstatus=%v transport_errors=%v\n",
		len(s.latencies), s.rps(), round(s.percentile(0.50)), round(s.percentile(0.95)),
		round(s.percentile(0.99)), round(s.percentile(1)), s.statuses, s.errs)
}

type consistency struct {
	Passed   bool     `json:"passed"`
	Wallets  int      `json:"wallets_checked"`
	Mismatch []string `json:"mismatches,omitempty"`
}

func (c consistency) summary() string {
	if c.Passed {
		return fmt.Sprintf("OK (%d wallets match 1000000.00 minus the 201 answers)", c.Wallets)
	}
	return fmt.Sprintf("MISMATCH in %d of %d wallets: %v", len(c.Mismatch), c.Wallets, c.Mismatch)
}

// verify compares each wallet's balance with the initial amount minus the BETs
// the API acknowledged. A request that timed out on the client may still have
// been applied, so a mismatch there is reported, not hidden.
func verify(hc *http.Client, base string, tk *tokens, wallets []*wallet) consistency {
	c := consistency{Passed: true, Wallets: len(wallets)}
	for _, w := range wallets {
		status, body, err := do(hc, http.MethodGet, base+"/wallets/"+w.id, tk.service.Load().(string), "", nil)
		if err != nil || status != http.StatusOK {
			c.Passed = false
			c.Mismatch = append(c.Mismatch, fmt.Sprintf("%s: read failed (%d %v)", w.id, status, err))
			continue
		}
		var out struct {
			Balance struct {
				Amount string `json:"amount"`
			} `json:"balance"`
		}
		if err := json.Unmarshal(body, &out); err != nil {
			c.Passed = false
			c.Mismatch = append(c.Mismatch, fmt.Sprintf("%s: %v", w.id, err))
			continue
		}
		got, err := strconv.ParseInt(strings.Replace(out.Balance.Amount, ".", "", 1), 10, 64)
		if err != nil {
			c.Passed = false
			c.Mismatch = append(c.Mismatch, fmt.Sprintf("%s: bad amount %q", w.id, out.Balance.Amount))
			continue
		}
		want := int64(initialMinor) - w.applied.Load()*betMinor
		if got != want {
			c.Passed = false
			c.Mismatch = append(c.Mismatch, fmt.Sprintf("%s: balance %d, expected %d", w.id, got, want))
		}
	}
	return c
}

type report struct {
	Timestamp   time.Time   `json:"timestamp"`
	Scenario    string      `json:"scenario"`
	Environment environment `json:"environment"`
	Params      params      `json:"params"`
	Result      result      `json:"result"`
	Consistency consistency `json:"consistency"`
	NotMeasured []string    `json:"not_measured"`
}

type environment struct {
	GoVersion string `json:"go_version"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	CPUs      int    `json:"cpus"`
	Note      string `json:"note,omitempty"`
}

type params struct {
	Base        string  `json:"base"`
	Wallets     int     `json:"wallets"`
	Workers     int     `json:"workers"`
	Duration    string  `json:"duration"`
	ReplayRatio float64 `json:"replay_ratio"`
	Seed        int64   `json:"seed"`
}

type result struct {
	Requests        int            `json:"requests"`
	DurationMS      int64          `json:"duration_ms"`
	RPS             float64        `json:"rps"`
	P50Ms           float64        `json:"p50_ms"`
	P95Ms           float64        `json:"p95_ms"`
	P99Ms           float64        `json:"p99_ms"`
	MaxMs           float64        `json:"max_ms"`
	Statuses        map[string]int `json:"statuses"`
	Conflicts       int            `json:"conflicts_409_422"`
	TransportErrors map[string]int `json:"transport_errors"`
}

func writeReport(cfg config, s stats, c consistency) error {
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	statuses := map[string]int{}
	for code, n := range s.statuses {
		statuses[strconv.Itoa(code)] = n
	}
	walletsUsed := cfg.wallets
	if cfg.scenario == "single" {
		walletsUsed = 1
	}

	r := report{
		Timestamp: time.Now().UTC(),
		Scenario:  cfg.scenario,
		Environment: environment{
			GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH,
			CPUs: runtime.NumCPU(), Note: cfg.note,
		},
		Params: params{
			Base: cfg.base, Wallets: walletsUsed, Workers: cfg.workers,
			Duration: cfg.duration.String(), ReplayRatio: cfg.replayRatio, Seed: cfg.seed,
		},
		Result: result{
			Requests: len(s.latencies), DurationMS: s.elapsed.Milliseconds(), RPS: s.rps(),
			P50Ms: ms(s.percentile(0.50)), P95Ms: ms(s.percentile(0.95)),
			P99Ms: ms(s.percentile(0.99)), MaxMs: ms(s.percentile(1)),
			Statuses: statuses, Conflicts: s.statuses[409] + s.statuses[422],
			TransportErrors: s.errs,
		},
		Consistency: c,
		NotMeasured: []string{"outbox delay: there is no outbox publisher"},
	}

	if err := os.MkdirAll(cfg.outDir, 0o755); err != nil {
		return err
	}
	name := fmt.Sprintf("%s-%d.json", cfg.scenario, time.Now().Unix())
	raw, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(cfg.outDir, name)
	fmt.Printf("report: %s\n", path)
	return os.WriteFile(path, raw, 0o644)
}

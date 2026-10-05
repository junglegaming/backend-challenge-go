package postgres

import (
	"context"
	"errors"
	"net"

	"github.com/jackc/pgx/v5/pgconn"
)

// IsUnavailable reports whether err means the database could not be reached or
// is shutting down, as opposed to the request being wrong. Such a failure is
// transient: the same request may succeed a moment later, and nothing was
// applied, because the transaction never committed.
//
// It covers a timeout waiting for a connection, a failed connect (refused,
// unresolvable host), a broken network, and the server's own connection-class
// (08), resource (53) and operator-intervention (57) errors, such as a
// shutdown in progress.
func IsUnavailable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && len(pgErr.Code) >= 2 {
		switch pgErr.Code[:2] {
		case "08", "53", "57":
			return true
		}
	}
	return false
}

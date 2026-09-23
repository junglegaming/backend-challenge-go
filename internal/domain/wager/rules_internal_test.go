package wager

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Decide only reaches decideMovement for a ROLLBACK once its reference is
// known, so the fail-closed branches of directionOf are exercised directly.
func TestDirectionOfFailsClosed(t *testing.T) {
	t.Parallel()
	_, ok := directionOf(KindRollback, nil)
	assert.False(t, ok, "a ROLLBACK without its reference has no direction")
	_, ok = directionOf(Kind("BOGUS"), nil)
	assert.False(t, ok, "an unknown kind never moves money")
	_, ok = directionOf(KindLoss, nil)
	assert.False(t, ok)
}

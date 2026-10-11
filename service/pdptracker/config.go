package pdptracker

import (
	"time"

	"github.com/cockroachdb/errors"
)

// defaultConfirmationDepth is the number of chain heads an event must trail
// the chain tip before its effects become externally visible. Filecoin
// epochs are ~30s; 10 epochs covers short reorgs with ~5 minutes of lag.
const defaultConfirmationDepth uint64 = 10

type PDPConfig struct {
	PollingInterval time.Duration
	// ConfirmationDepth is the number of confirmations (chain blocks)
	// required before indexed events are applied to derived state.
	ConfirmationDepth uint64
}

func (c PDPConfig) Validate() error {
	if c.PollingInterval <= 0 {
		return errors.New("pdp polling interval must be greater than 0")
	}
	return nil
}

// confirmationDepth returns the configured depth or the default.
func (c PDPConfig) confirmationDepth() uint64 {
	if c.ConfirmationDepth == 0 {
		return defaultConfirmationDepth
	}
	return c.ConfirmationDepth
}

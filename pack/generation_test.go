package pack

import (
	"testing"

	"github.com/data-preservation-programs/singularity/model"
	"github.com/data-preservation-programs/singularity/util/testutil"
	"gorm.io/gorm"
)

// pinJobGenerationsForTest mirrors the worker claim-time generation pin for
// tests that call Pack directly with fixture jobs.
func pinJobGenerationsForTest(t *testing.T, db *gorm.DB, job *model.Job) {
	t.Helper()
	testutil.PinJobGenerations(t, db, job)
}

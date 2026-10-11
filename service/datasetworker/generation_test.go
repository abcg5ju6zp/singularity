package datasetworker

import (
	"testing"

	"github.com/data-preservation-programs/singularity/model"
	"github.com/data-preservation-programs/singularity/util/testutil"
	"gorm.io/gorm"
)

// pinDagJobGenerations mirrors the worker claim-time generation pin for
// daggen tests that invoke ExportDag directly with fixture jobs.
func pinDagJobGenerations(t *testing.T, db *gorm.DB, job *model.Job) {
	t.Helper()
	testutil.PinJobGenerations(t, db, job)
}

package storagesystem_test

import (
	"context"
	"testing"

	"github.com/data-preservation-programs/singularity/model"
	"github.com/data-preservation-programs/singularity/storagesystem"
	"github.com/data-preservation-programs/singularity/util/testutil"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func loadPinTestJob(t *testing.T, db *gorm.DB, jobID model.JobID) model.Job {
	var job model.Job
	require.NoError(t, db.Preload("Attachment.Storage").First(&job, jobID).Error)
	return job
}

// TR-4/5: first claim pins the active generation; every later attempt
// (delayed retry, process restart) keeps using that same generation even
// after the storage has activated a newer one.
func TestJobGenerationPinning(t *testing.T) {
	testutil.All(t, func(ctx context.Context, t *testing.T, db *gorm.DB) {
		v1Dir := t.TempDir()
		v2Dir := t.TempDir()
		storage := testutil.CreateStorageWithGeneration(t, db, model.Storage{
			Name: "pin-source", Type: "local", Path: v1Dir,
		})

		prep := model.Preparation{Name: "pin-prep"}
		require.NoError(t, db.Create(&prep).Error)
		attachment := model.SourceAttachment{
			PreparationID:       prep.ID,
			StorageID:           storage.ID,
			StorageGenerationID: storage.ActiveGenerationID,
		}
		require.NoError(t, db.Create(&attachment).Error)

		job := model.Job{Type: model.Scan, State: model.Ready, AttachmentID: &attachment.ID}
		require.NoError(t, db.Create(&job).Error)

		// first claim: pins the active generation and rewrites the
		// in-memory storage to the immutable snapshot
		claimed := loadPinTestJob(t, db, job.ID)
		require.NoError(t, storagesystem.PinJobSourceGeneration(ctx, db, &claimed))
		require.NotNil(t, claimed.SourceGenerationID)
		require.Equal(t, *storage.ActiveGenerationID, *claimed.SourceGenerationID)
		require.NotNil(t, claimed.Attachment.Storage.GenerationID)
		require.Equal(t, v1Dir, claimed.Attachment.Storage.Path)

		// pin survives a restart: reload from the database and check durability
		restarted := loadPinTestJob(t, db, job.ID)
		require.Equal(t, *storage.ActiveGenerationID, *restarted.SourceGenerationID)

		// storage activates a new generation pointing at a different directory
		var v1 model.StorageGeneration
		require.NoError(t, db.First(&v1, *storage.ActiveGenerationID).Error)
		v2 := model.StorageGeneration{
			StorageID: storage.ID, Version: v1.Version + 1, Name: v1.Name,
			Type: v1.Type, Path: v2Dir, Config: v1.Config, ClientConfig: v1.ClientConfig,
		}
		require.NoError(t, db.Create(&v2).Error)
		require.NoError(t, db.Model(&model.Storage{}).Where("id = ?", storage.ID).
			Updates(map[string]any{"active_generation_id": v2.ID, "path": v2Dir}).Error)

		// a delayed retry of the SAME job must keep the original generation
		retried := loadPinTestJob(t, db, job.ID)
		require.NoError(t, storagesystem.PinJobSourceGeneration(ctx, db, &retried))
		require.Equal(t, v1.ID, *retried.SourceGenerationID)
		require.NotNil(t, retried.Attachment.Storage.GenerationID)
		require.Equal(t, v1.ID, *retried.Attachment.Storage.GenerationID)
		require.Equal(t, v1Dir, retried.Attachment.Storage.Path)

		// a brand new job pins the now-active generation
		job2 := model.Job{Type: model.Scan, State: model.Ready, AttachmentID: &attachment.ID}
		require.NoError(t, db.Create(&job2).Error)
		fresh := loadPinTestJob(t, db, job2.ID)
		require.NoError(t, storagesystem.PinJobSourceGeneration(ctx, db, &fresh))
		require.Equal(t, v2.ID, *fresh.SourceGenerationID)
		require.Equal(t, v2Dir, fresh.Attachment.Storage.Path)
	})
}

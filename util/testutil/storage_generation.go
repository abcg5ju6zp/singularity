package testutil

import (
	"context"
	"testing"

	"github.com/data-preservation-programs/singularity/model"
	"github.com/data-preservation-programs/singularity/storagesystem"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// CreateStorageWithGeneration persists a fixture storage together with its
// active v1 generation, the same shape the storage create handler produces.
func CreateStorageWithGeneration(t *testing.T, db *gorm.DB, storage model.Storage) model.Storage {
	t.Helper()
	require.NoError(t, db.Create(&storage).Error)
	generation := model.StorageGeneration{
		StorageID:    storage.ID,
		Version:      1,
		Name:         storage.Name,
		Type:         storage.Type,
		Path:         storage.Path,
		Config:       storage.Config,
		ClientConfig: storage.ClientConfig,
	}
	require.NoError(t, db.Create(&generation).Error)
	require.NoError(t, db.Model(&model.Storage{}).Where("id = ?", storage.ID).
		Update("active_generation_id", generation.ID).Error)
	storage.ActiveGenerationID = &generation.ID
	storage.GenerationID = &generation.ID
	return storage
}

// EnsureActiveGeneration makes sure a fixture storage (inserted directly) has
// an active generation, creating its v1 snapshot if needed.
func EnsureActiveGeneration(t *testing.T, db *gorm.DB, storageID model.StorageID) model.StorageGeneration {
	t.Helper()
	var storage model.Storage
	require.NoError(t, db.First(&storage, storageID).Error)
	if storage.ActiveGenerationID != nil {
		var generation model.StorageGeneration
		require.NoError(t, db.First(&generation, *storage.ActiveGenerationID).Error)
		return generation
	}
	var maxVersion uint32
	require.NoError(t, db.Model(&model.StorageGeneration{}).Where("storage_id = ?", storageID).
		Select("COALESCE(MAX(version), 0)").Scan(&maxVersion).Error)
	generation := model.StorageGeneration{
		StorageID:    storageID,
		Version:      maxVersion + 1,
		Name:         storage.Name,
		Type:         storage.Type,
		Path:         storage.Path,
		Config:       storage.Config,
		ClientConfig: storage.ClientConfig,
	}
	require.NoError(t, db.Create(&generation).Error)
	require.NoError(t, db.Model(&model.Storage{}).Where("id = ?", storageID).
		Update("active_generation_id", generation.ID).Error)
	return generation
}

// PinJobGenerations mirrors what the worker claim transaction does for tests
// that invoke scan/pack/daggen directly: every storage the job touches gets an
// active generation, the source generation is pinned on the job and the
// in-memory snapshots are replaced so the operation builds its rclone handlers
// from immutable configuration.
func PinJobGenerations(t *testing.T, db *gorm.DB, job *model.Job) {
	t.Helper()
	require.NotNil(t, job.Attachment, "job must have Attachment preloaded")
	EnsureActiveGeneration(t, db, job.Attachment.StorageID)
	require.NoError(t, storagesystem.PinJobSourceGeneration(context.Background(), db, job))
	if job.Attachment.Preparation != nil && len(job.Attachment.Preparation.OutputStorages) > 0 {
		snapshots, err := storagesystem.ActiveSnapshots(context.Background(), db, job.Attachment.Preparation.OutputStorages)
		require.NoError(t, err)
		job.Attachment.Preparation.OutputStorages = snapshots
	}
}

package storage

import (
	"context"
	"testing"

	"github.com/data-preservation-programs/singularity/handler/handlererror"
	"github.com/data-preservation-programs/singularity/model"
	"github.com/data-preservation-programs/singularity/storagesystem"
	"github.com/data-preservation-programs/singularity/util/testutil"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestStorageGenerationLifecycle(t *testing.T) {
	t.Run("create activates v1", func(t *testing.T) {
		testutil.All(t, func(ctx context.Context, t *testing.T, db *gorm.DB) {
			dir := t.TempDir()
			storage, err := Default.CreateStorageHandler(ctx, db, "local",
				CreateRequest{"", "gen-v1", dir, nil, model.ClientConfig{}})
			require.NoError(t, err)
			require.NotNil(t, storage.ActiveGenerationID)

			var generation model.StorageGeneration
			require.NoError(t, db.First(&generation, *storage.ActiveGenerationID).Error)
			require.Equal(t, storage.ID, generation.StorageID)
			require.EqualValues(t, 1, generation.Version)
			require.Equal(t, "gen-v1", generation.Name)
		})
	})

	t.Run("update appends and atomically activates a new generation", func(t *testing.T) {
		testutil.All(t, func(ctx context.Context, t *testing.T, db *gorm.DB) {
			dir := t.TempDir()
			storage, err := Default.CreateStorageHandler(ctx, db, "local",
				CreateRequest{"", "gen-upd", dir, map[string]string{"copy_links": "false"}, model.ClientConfig{}})
			require.NoError(t, err)
			v1ID := *storage.ActiveGenerationID

			// pin v1 through an attachment so it remains referenced after
			// the update and must be retained, not garbage collected
			prep := model.Preparation{Name: "gen-upd-prep"}
			require.NoError(t, db.Create(&prep).Error)
			require.NoError(t, db.Create(&model.SourceAttachment{
				PreparationID:       prep.ID,
				StorageID:           storage.ID,
				StorageGenerationID: storage.ActiveGenerationID,
			}).Error)

			updated, err := Default.UpdateStorageHandler(ctx, db, "gen-upd",
				UpdateRequest{Config: map[string]string{"copy_links": "true"}})
			require.NoError(t, err)
			require.NotNil(t, updated.ActiveGenerationID)
			require.NotEqual(t, v1ID, *updated.ActiveGenerationID)

			// old generation is retained untouched
			var v1, v2 model.StorageGeneration
			require.NoError(t, db.First(&v1, v1ID).Error)
			require.Equal(t, "false", v1.Config["copy_links"])
			require.NoError(t, db.First(&v2, *updated.ActiveGenerationID).Error)
			require.Equal(t, "true", v2.Config["copy_links"])
			require.Equal(t, v1.Version+1, v2.Version)

			// the storages row mirrors the new active generation
			var mirrored model.Storage
			require.NoError(t, db.First(&mirrored, storage.ID).Error)
			require.Equal(t, *updated.ActiveGenerationID, *mirrored.ActiveGenerationID)
			require.Equal(t, "true", mirrored.Config["copy_links"])

			// the old snapshot remains usable through the resolver
			oldSnapshot, err := storagesystem.GenerationSnapshot(ctx, db, v1ID)
			require.NoError(t, err)
			require.Equal(t, "false", oldSnapshot.Config["copy_links"])
		})
	})

	t.Run("failed validation keeps the old generation active and usable", func(t *testing.T) {
		testutil.All(t, func(ctx context.Context, t *testing.T, db *gorm.DB) {
			dir := t.TempDir()
			storage, err := Default.CreateStorageHandler(ctx, db, "local",
				CreateRequest{"", "gen-fail", dir, nil, model.ClientConfig{}})
			require.NoError(t, err)
			v1ID := *storage.ActiveGenerationID

			_, err = Default.UpdateStorageHandler(ctx, db, "gen-fail",
				UpdateRequest{Config: map[string]string{"copy_links": "not-a-bool"}})
			require.ErrorIs(t, err, handlererror.ErrInvalidParameter)

			var count int64
			require.NoError(t, db.Model(&model.StorageGeneration{}).Where("storage_id = ?", storage.ID).Count(&count).Error)
			require.EqualValues(t, 1, count, "no generation may be added on failed activation")

			var refreshed model.Storage
			require.NoError(t, db.First(&refreshed, storage.ID).Error)
			require.Equal(t, v1ID, *refreshed.ActiveGenerationID)
		})
	})

	t.Run("rename does not change generation identity", func(t *testing.T) {
		testutil.All(t, func(ctx context.Context, t *testing.T, db *gorm.DB) {
			dir := t.TempDir()
			storage, err := Default.CreateStorageHandler(ctx, db, "local",
				CreateRequest{"", "old-name", dir, nil, model.ClientConfig{}})
			require.NoError(t, err)
			genID := *storage.ActiveGenerationID

			_, err = Default.RenameStorageHandler(ctx, db, "old-name", RenameRequest{Name: "new-name"})
			require.NoError(t, err)

			var generation model.StorageGeneration
			require.NoError(t, db.First(&generation, genID).Error)
			require.Equal(t, genID, *storage.ActiveGenerationID)
			require.Equal(t, "old-name", generation.Name)

			var renamed model.Storage
			require.NoError(t, db.First(&renamed, storage.ID).Error)
			require.Equal(t, "new-name", renamed.Name)
			require.Equal(t, genID, *renamed.ActiveGenerationID)
		})
	})

	t.Run("remove is refused while any generation is referenced", func(t *testing.T) {
		testutil.All(t, func(ctx context.Context, t *testing.T, db *gorm.DB) {
			storage := testutil.CreateStorageWithGeneration(t, db, model.Storage{
				Name: "gen-remove", Type: "local", Path: t.TempDir(),
			})

			prep := model.Preparation{Name: "gen-remove-prep"}
			require.NoError(t, db.Create(&prep).Error)
			attachment := model.SourceAttachment{
				PreparationID:       prep.ID,
				StorageID:           storage.ID,
				StorageGenerationID: storage.ActiveGenerationID,
			}
			require.NoError(t, db.Create(&attachment).Error)

			err := Default.RemoveHandler(ctx, db, "gen-remove")
			require.ErrorIs(t, err, handlererror.ErrInvalidParameter)

			// even after the attachment is gone, a car pin keeps the storage alive
			require.NoError(t, db.Delete(&model.SourceAttachment{}, attachment.ID).Error)
			car := model.Car{
				PieceCID:            model.CID(testutil.TestCid),
				StorageID:           &storage.ID,
				StorageGenerationID: storage.ActiveGenerationID,
				StoragePath:         "x.car",
			}
			require.NoError(t, db.Create(&car).Error)
			err = Default.RemoveHandler(ctx, db, "gen-remove")
			require.ErrorIs(t, err, handlererror.ErrInvalidParameter)
		})
	})

	t.Run("remove succeeds once generations are unreferenced", func(t *testing.T) {
		testutil.All(t, func(ctx context.Context, t *testing.T, db *gorm.DB) {
			dir := t.TempDir()
			storage, err := Default.CreateStorageHandler(ctx, db, "local",
				CreateRequest{"", "gen-clean", dir, nil, model.ClientConfig{}})
			require.NoError(t, err)
			// update once: unreferenced v1 gets cleaned up, v2 is active
			_, err = Default.UpdateStorageHandler(ctx, db, "gen-clean",
				UpdateRequest{Config: map[string]string{"copy_links": "true"}})
			require.NoError(t, err)

			require.NoError(t, Default.RemoveHandler(ctx, db, "gen-clean"))

			var storageCount, genCount int64
			db.Model(&model.Storage{}).Where("id = ?", storage.ID).Count(&storageCount)
			db.Model(&model.StorageGeneration{}).Where("storage_id = ?", storage.ID).Count(&genCount)
			require.Zero(t, storageCount)
			require.Zero(t, genCount)
		})
	})
}

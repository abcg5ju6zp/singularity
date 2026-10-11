package dataprep

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/data-preservation-programs/singularity/model"
	"github.com/data-preservation-programs/singularity/util/testutil"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// TR-7.3: physical CAR deletion targets the generation the car was written
// to; after the storage moves to a new directory, the old object is removed
// there and the new directory is untouched.
func TestDeletePieceHandler_DeletesFromPinnedGeneration(t *testing.T) {
	testutil.All(t, func(ctx context.Context, t *testing.T, db *gorm.DB) {
		v1Dir := t.TempDir()
		v2Dir := t.TempDir()
		storage := testutil.CreateStorageWithGeneration(t, db,
			model.Storage{Name: "delete-gen-output", Type: "local", Path: v1Dir})

		prep := model.Preparation{Name: "delete-gen-prep"}
		require.NoError(t, db.Create(&prep).Error)
		attachment := model.SourceAttachment{
			PreparationID:       prep.ID,
			StorageID:           storage.ID,
			StorageGenerationID: storage.ActiveGenerationID,
		}
		require.NoError(t, db.Create(&attachment).Error)

		carName := "obsolete.car"
		require.NoError(t, os.WriteFile(filepath.Join(v1Dir, carName), []byte("old car"), 0644))
		pieceCID := testPieceCID("delete-from-pinned-generation")
		car := model.Car{
			PieceCID:            model.CID(pieceCID),
			PreparationID:       &prep.ID,
			AttachmentID:        &attachment.ID,
			PieceType:           model.DataPiece,
			StorageID:           &storage.ID,
			StorageGenerationID: storage.ActiveGenerationID,
			StoragePath:         carName,
		}
		require.NoError(t, db.Create(&car).Error)

		// storage moves away to an empty directory
		var previous model.StorageGeneration
		require.NoError(t, db.First(&previous, *storage.ActiveGenerationID).Error)
		v2 := model.StorageGeneration{
			StorageID: storage.ID, Version: previous.Version + 1,
			Name: previous.Name, Type: previous.Type, Path: v2Dir,
			Config: previous.Config, ClientConfig: previous.ClientConfig,
		}
		require.NoError(t, db.Create(&v2).Error)
		require.NoError(t, db.Model(&model.Storage{}).Where("id = ?", storage.ID).Updates(map[string]any{
			"active_generation_id": v2.ID, "path": v2Dir,
		}).Error)

		require.NoError(t, Default.DeletePieceHandler(ctx, db, "delete-gen-prep", pieceCID.String(),
			DeletePieceRequest{DeleteCar: true}))

		_, err := os.Stat(filepath.Join(v1Dir, carName))
		require.True(t, os.IsNotExist(err), "CAR must be removed from the old generation directory")
		v2Entries, err := os.ReadDir(v2Dir)
		require.NoError(t, err)
		require.Empty(t, v2Entries)

		var count int64
		require.NoError(t, db.Model(&model.Car{}).Where("id = ?", car.ID).Count(&count).Error)
		require.Zero(t, count)
	})
}

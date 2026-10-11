package healthcheck

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/data-preservation-programs/singularity/model"
	"github.com/data-preservation-programs/singularity/util/testutil"
	"github.com/ipfs/boxo/util"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func healthTestPieceCID(seed string) cid.Cid {
	return cid.NewCidV1(cid.FilCommitmentUnsealed, util.Hash([]byte(seed)))
}

func setupHealthStorage(t *testing.T, db *gorm.DB, name, fileName, content string) (model.Storage, string) {
	t.Helper()
	tmp := t.TempDir()
	carFile := filepath.Join(tmp, fileName)
	require.NoError(t, os.WriteFile(carFile, []byte(content), 0o644))
	storage := model.Storage{Name: name, Type: "local", Path: tmp}
	require.NoError(t, db.Create(&storage).Error)
	return storage, carFile
}

// TestHealthCheck_ParksStaleDeletionAsPending verifies a deleting lease from
// a dead owner becomes queryable pending after the staleness threshold.
func TestHealthCheck_ParksStaleDeletionAsPending(t *testing.T) {
	testutil.All(t, func(ctx context.Context, t *testing.T, db *gorm.DB) {
		storage, carFile := setupHealthStorage(t, db, "hc-stale", "stale.car", "data")
		prep := model.Preparation{Name: "hc-stale-prep", OutputStorages: []model.Storage{storage}}
		require.NoError(t, db.Create(&prep).Error)

		pieceCID := healthTestPieceCID("stale")
		car := model.Car{
			PieceCID:      model.CID(pieceCID),
			PreparationID: &prep.ID,
			StorageID:     &storage.ID,
			StoragePath:   "stale.car",
			PieceType:     model.DataPiece,
		}
		require.NoError(t, db.Create(&car).Error)
		staleAt := time.Now().UTC().Add(-10 * time.Minute)
		require.NoError(t, db.Model(&model.Car{}).Where("id = ?", car.ID).Updates(map[string]any{
			"delete_state":       model.PieceDeletionDeleting,
			"delete_owner_token": "dead-process:1",
			"delete_locked_at":   staleAt,
			"delete_manifest": &model.PieceDeletionManifest{
				FrozenAt:      staleAt,
				DeleteCar:     true,
				PieceType:     model.DataPiece,
				PreparationID: &prep.ID,
				StorageID:     &storage.ID,
				StoragePath:   "stale.car",
			},
		}).Error)

		HealthCheckCleanup(ctx, db)

		var updated model.Car
		require.NoError(t, db.First(&updated, car.ID).Error)
		require.Equal(t, model.PieceDeletionPending, updated.DeleteState)
		require.Contains(t, updated.DeleteError, "interrupted")
		// Attached pending pieces wait for an explicit admin retry.
		_, err := os.Stat(carFile)
		require.NoError(t, err)
	})
}

// TestHealthCheck_LiveOrphanDeletionSurvivesReaper verifies a fresh deleting
// lease (owner possibly still alive) is neither parked nor reaped even after
// the preparation row is gone.
func TestHealthCheck_LiveOrphanDeletionSurvivesReaper(t *testing.T) {
	testutil.All(t, func(ctx context.Context, t *testing.T, db *gorm.DB) {
		storage, carFile := setupHealthStorage(t, db, "hc-live", "live.car", "data")

		pieceCID := healthTestPieceCID("live")
		car := model.Car{
			PieceCID:    model.CID(pieceCID),
			StorageID:   &storage.ID,
			StoragePath: "live.car",
			PieceType:   model.DataPiece,
		}
		require.NoError(t, db.Create(&car).Error)
		require.NoError(t, db.Model(&model.Car{}).Where("id = ?", car.ID).Updates(map[string]any{
			"delete_state":       model.PieceDeletionDeleting,
			"delete_owner_token": "other-instance:2",
			"delete_locked_at":   time.Now().UTC(),
			"preparation_id":     nil,
			"delete_manifest": &model.PieceDeletionManifest{
				FrozenAt:    time.Now().UTC(),
				DeleteCar:   true,
				PieceType:   model.DataPiece,
				StorageID:   &storage.ID,
				StoragePath: "live.car",
			},
		}).Error)

		HealthCheckCleanup(ctx, db)

		var updated model.Car
		require.NoError(t, db.First(&updated, car.ID).Error)
		require.Equal(t, model.PieceDeletionDeleting, updated.DeleteState)
		_, err := os.Stat(carFile)
		require.NoError(t, err)
	})
}

// TestHealthCheck_CompletesOrphanedPendingDeletion verifies the healthcheck
// becomes the owner for pending pieces whose preparation was removed
// concurrently: external object removed, indexes dropped, no orphans.
func TestHealthCheck_CompletesOrphanedPendingDeletion(t *testing.T) {
	testutil.All(t, func(ctx context.Context, t *testing.T, db *gorm.DB) {
		storage, carFile := setupHealthStorage(t, db, "hc-orphan", "orphan.car", "data")

		pieceCID := healthTestPieceCID("orphan")
		car := model.Car{
			PieceCID:    model.CID(pieceCID),
			StorageID:   &storage.ID,
			StoragePath: "orphan.car",
			PieceType:   model.DataPiece,
		}
		require.NoError(t, db.Create(&car).Error)
		require.NoError(t, db.Create(&model.CarBlock{CarID: &car.ID, CarOffset: 1}).Error)
		require.NoError(t, db.Model(&model.Car{}).Where("id = ?", car.ID).Updates(map[string]any{
			"delete_state":   model.PieceDeletionPending,
			"preparation_id": nil,
			"delete_error":   "database phase failed: simulated",
			"delete_manifest": &model.PieceDeletionManifest{
				FrozenAt:      time.Now().UTC(),
				DeleteCar:     true,
				PieceType:     model.DataPiece,
				StorageID:     &storage.ID,
				StoragePath:   "orphan.car",
				CarBlockCount: 1,
			},
		}).Error)

		HealthCheckCleanup(ctx, db)

		var cars, blocks int64
		db.Model(&model.Car{}).Count(&cars)
		require.Zero(t, cars)
		db.Model(&model.CarBlock{}).Count(&blocks)
		require.Zero(t, blocks)
		_, err := os.Stat(carFile)
		require.True(t, os.IsNotExist(err))
	})
}

// TestHealthCheck_UnresumablePendingStaysQueryable verifies a pending orphan
// whose storage can no longer be reached is kept (queryable) rather than
// reaped, so the storage object can never become untrackable.
func TestHealthCheck_UnresumablePendingStaysQueryable(t *testing.T) {
	testutil.All(t, func(ctx context.Context, t *testing.T, db *gorm.DB) {
		// Create then remove the storage row: the car's storage_id FK is SET
		// NULL but the frozen manifest still names the now-missing storage.
		storage, _ := setupHealthStorage(t, db, "hc-missing", "x.car", "data")
		missingStorageID := storage.ID
		require.NoError(t, db.Delete(&model.Storage{}, storage.ID).Error)

		pieceCID := healthTestPieceCID("unresumable")
		car := model.Car{
			PieceCID:    model.CID(pieceCID),
			StoragePath: "gone.car",
			PieceType:   model.DataPiece,
		}
		require.NoError(t, db.Create(&car).Error)
		require.NoError(t, db.Model(&model.Car{}).Where("id = ?", car.ID).Updates(map[string]any{
			"delete_state":   model.PieceDeletionPending,
			"preparation_id": nil,
			"delete_error":   "previous failure",
			"delete_manifest": &model.PieceDeletionManifest{
				FrozenAt:    time.Now().UTC(),
				DeleteCar:   true,
				PieceType:   model.DataPiece,
				StorageID:   &missingStorageID,
				StoragePath: "gone.car",
			},
		}).Error)

		HealthCheckCleanup(ctx, db)

		var updated model.Car
		require.NoError(t, db.First(&updated, car.ID).Error)
		require.Equal(t, model.PieceDeletionPending, updated.DeleteState)
		require.NotEmpty(t, updated.DeleteError)
	})
}

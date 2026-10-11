package dataprep

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/data-preservation-programs/singularity/handler/handlererror"
	"github.com/data-preservation-programs/singularity/model"
	"github.com/data-preservation-programs/singularity/storagesystem"
	"github.com/data-preservation-programs/singularity/util/testutil"
	"github.com/gotidy/ptr"
	"github.com/ipfs/go-cid"
	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// controllableObjectStore wraps the real local rclone handler so tests can
// make Remove fail, block, or delete out-of-band while Check still reports
// real object facts.
type controllableObjectStore struct {
	inner       objectStore
	removeHook  func(remote string) error
	removeCount *atomic.Int32
}

func (s *controllableObjectStore) Check(ctx context.Context, path string) (fs.DirEntry, error) {
	return s.inner.Check(ctx, path)
}

func (s *controllableObjectStore) Remove(ctx context.Context, obj fs.Object) error {
	s.removeCount.Add(1)
	if s.removeHook != nil {
		if err := s.removeHook(obj.Remote()); err != nil {
			return err
		}
	}
	return s.inner.Remove(ctx, obj)
}

// installControllableStore redirects the object store factory at a
// controllable wrapper over a real local storage backend.
func installControllableStore(t *testing.T, db *gorm.DB, removeCount *atomic.Int32, hook func(remote string) error) {
	t.Helper()
	original := newPieceObjectStore
	newPieceObjectStore = func(ctx context.Context, gdb *gorm.DB, storageID model.StorageID) (objectStore, error) {
		var storage model.Storage
		if err := gdb.First(&storage, storageID).Error; err != nil {
			return nil, err
		}
		handler, err := storagesystem.NewRCloneHandler(ctx, storage)
		if err != nil {
			return nil, err
		}
		return &controllableObjectStore{inner: handler, removeHook: hook, removeCount: removeCount}, nil
	}
	t.Cleanup(func() { newPieceObjectStore = original })
}

func setupDeletablePiece(t *testing.T, db *gorm.DB, pathName string, withBlock bool, withDeal bool) (model.Preparation, model.Storage, model.Car, cid.Cid, string) {
	t.Helper()
	tmp := t.TempDir()
	carFile := filepath.Join(tmp, pathName)
	require.NoError(t, os.WriteFile(carFile, []byte("car data"), 0o644))

	storage := model.Storage{Name: "output-" + pathName, Type: "local", Path: tmp}
	require.NoError(t, db.Create(&storage).Error)

	prep := model.Preparation{
		Name:           "test-prep-" + pathName,
		OutputStorages: []model.Storage{storage},
	}
	require.NoError(t, db.Create(&prep).Error)

	pieceCID := testPieceCID("engine-" + pathName)
	car := model.Car{
		PieceCID:      model.CID(pieceCID),
		PreparationID: &prep.ID,
		StorageID:     &storage.ID,
		StoragePath:   pathName,
		PieceType:     model.DataPiece,
	}
	require.NoError(t, db.Create(&car).Error)

	if withBlock {
		require.NoError(t, db.Create(&model.CarBlock{CarID: &car.ID, CarOffset: 7}).Error)
	}
	if withDeal {
		require.NoError(t, db.Create(&model.Actor{ID: "f9999", Address: "f9999"}).Error)
		require.NoError(t, db.Create(&model.Deal{
			PieceCID: model.CID(pieceCID),
			Provider: "f05678",
			ClientID: "f9999",
		}).Error)
	}
	return prep, storage, car, pieceCID, carFile
}

func seedDeletionState(t *testing.T, db *gorm.DB, car model.Car, state model.PieceDeletionState,
	owner string, lockedAt *time.Time, manifest *model.PieceDeletionManifest, deleteError string,
) {
	t.Helper()
	updates := map[string]any{
		"delete_state":       state,
		"delete_owner_token": owner,
		"delete_locked_at":   lockedAt,
		"delete_error":       deleteError,
	}
	if manifest != nil {
		updates["delete_manifest"] = manifest
	}
	require.NoError(t, db.Model(&model.Car{}).Where("id = ?", car.ID).Updates(updates).Error)
}

func manifestFor(storage model.Storage, car model.Car) *model.PieceDeletionManifest {
	return &model.PieceDeletionManifest{
		FrozenAt:      time.Now().UTC(),
		DeleteCar:     true,
		PieceType:     model.DataPiece,
		PreparationID: car.PreparationID,
		StorageID:     &storage.ID,
		StoragePath:   car.StoragePath,
		CarBlockCount: 1,
	}
}

// TestPieceDeletion_RemoveFailureParksPendingAndRetrySucceeds verifies the
// core invariant: an external CAR removal that leaves the object present
// parks the piece in queryable pending state with indexes intact; once the
// object is gone, retrying completes the deletion using the frozen manifest.
func TestPieceDeletion_RemoveFailureParksPendingAndRetrySucceeds(t *testing.T) {
	testutil.All(t, func(ctx context.Context, t *testing.T, db *gorm.DB) {
		prep, _, car, pieceCID, carFile := setupDeletablePiece(t, db, "pending.car", true, true)

		var removeCount atomic.Int32
		installControllableStore(t, db, &removeCount, func(remote string) error {
			return errors.New("simulated storage failure")
		})

		err := Default.DeletePieceHandler(ctx, db, prep.Name, pieceCID.String(), DeletePieceRequest{DeleteCar: true, Force: true})
		require.Error(t, err)
		require.ErrorContains(t, err, "pending")

		// External object and database indexes are both still present.
		_, err = os.Stat(carFile)
		require.NoError(t, err)

		var frozen model.Car
		require.NoError(t, db.First(&frozen, car.ID).Error)
		require.Equal(t, model.PieceDeletionPending, frozen.DeleteState)
		require.Contains(t, frozen.DeleteError, "still present")
		require.NotNil(t, frozen.DeleteManifest)
		require.Equal(t, "pending.car", frozen.DeleteManifest.StoragePath)
		require.Equal(t, int64(1), frozen.DeleteManifest.CarBlockCount)
		require.Len(t, frozen.DeleteManifest.DealIDs, 1)

		var blockCount int64
		db.Model(&model.CarBlock{}).Where("car_id = ?", car.ID).Count(&blockCount)
		require.Equal(t, int64(1), blockCount)

		// Object gets removed out-of-band (e.g. by an operator); retry reads
		// the object fact (absent) and finalizes per the original manifest.
		require.NoError(t, os.Remove(carFile))
		err = Default.DeletePieceHandler(ctx, db, prep.Name, pieceCID.String(), DeletePieceRequest{DeleteCar: true, Force: true})
		require.NoError(t, err)

		var cars int64
		db.Model(&model.Car{}).Count(&cars)
		require.Zero(t, cars)
		blockCount = 0
		db.Model(&model.CarBlock{}).Count(&blockCount)
		require.Zero(t, blockCount)
	})
}

// TestPieceDeletion_UncertainRemoveButObjectGone verifies that an ambiguous
// Remove response followed by a successful fact check (object absent) does
// not orphan either the object or the record.
func TestPieceDeletion_UncertainRemoveButObjectGone(t *testing.T) {
	testutil.All(t, func(ctx context.Context, t *testing.T, db *gorm.DB) {
		prep, storage, _, pieceCID, carFile := setupDeletablePiece(t, db, "uncertain.car", false, false)

		var removeCount atomic.Int32
		installControllableStore(t, db, &removeCount, func(remote string) error {
			// Simulate a timeout after the deletion actually happened.
			require.NoError(t, os.Remove(filepath.Join(storage.Path, remote)))
			return errors.New("simulated network timeout")
		})

		err := Default.DeletePieceHandler(ctx, db, prep.Name, pieceCID.String(), DeletePieceRequest{DeleteCar: true})
		require.NoError(t, err)

		_, err = os.Stat(carFile)
		require.True(t, os.IsNotExist(err))
		var cars int64
		db.Model(&model.Car{}).Count(&cars)
		require.Zero(t, cars)
		require.Equal(t, int32(1), removeCount.Load())
	})
}

// TestPieceDeletion_NotAFileObjectParksPending verifies a path that resolves
// to a directory is parked pending rather than half-deleted.
func TestPieceDeletion_NotAFileObjectParksPending(t *testing.T) {
	testutil.All(t, func(ctx context.Context, t *testing.T, db *gorm.DB) {
		tmp := t.TempDir()
		require.NoError(t, os.Mkdir(filepath.Join(tmp, "subdir"), 0o755))

		storage := model.Storage{Name: "output-dir", Type: "local", Path: tmp}
		require.NoError(t, db.Create(&storage).Error)
		prep := model.Preparation{Name: "prep-dir", OutputStorages: []model.Storage{storage}}
		require.NoError(t, db.Create(&prep).Error)

		pieceCID := testPieceCID("dir-piece")
		car := model.Car{
			PieceCID:      model.CID(pieceCID),
			PreparationID: &prep.ID,
			StorageID:     &storage.ID,
			StoragePath:   "subdir",
			PieceType:     model.DataPiece,
		}
		require.NoError(t, db.Create(&car).Error)

		err := Default.DeletePieceHandler(ctx, db, prep.Name, pieceCID.String(), DeletePieceRequest{DeleteCar: true})
		require.Error(t, err)

		var frozen model.Car
		require.NoError(t, db.First(&frozen, car.ID).Error)
		require.Equal(t, model.PieceDeletionPending, frozen.DeleteState)
		// Local backends reject NewObject on a directory as a Check error;
		// either way the deletion is safely parked rather than half-done.
		require.Contains(t, frozen.DeleteError, "subdir")
	})
}

// TestPieceDeletion_LiveOwnerConflict verifies a duplicate deletion while
// another owner holds a fresh lease is rejected with a conflict.
func TestPieceDeletion_LiveOwnerConflict(t *testing.T) {
	testutil.All(t, func(ctx context.Context, t *testing.T, db *gorm.DB) {
		prep, storage, car, pieceCID, carFile := setupDeletablePiece(t, db, "locked.car", false, false)
		now := time.Now().UTC()
		seedDeletionState(t, db, car, model.PieceDeletionDeleting,
			"another-instance:deadbeef", &now, manifestFor(storage, car), "")

		err := Default.DeletePieceHandler(ctx, db, prep.Name, pieceCID.String(), DeletePieceRequest{DeleteCar: true})
		require.ErrorIs(t, err, handlererror.ErrConflict)
		require.ErrorContains(t, err, "already in progress")

		var frozen model.Car
		require.NoError(t, db.First(&frozen, car.ID).Error)
		require.Equal(t, model.PieceDeletionDeleting, frozen.DeleteState)
		require.Equal(t, "another-instance:deadbeef", frozen.DeleteOwnerToken)
		_, err = os.Stat(carFile)
		require.NoError(t, err)
	})
}

// TestPieceDeletion_StaleLeaseTakeover simulates a process restart: the
// previous owner's lease is stale and a new caller takes over, reusing the
// frozen manifest to finish the deletion.
func TestPieceDeletion_StaleLeaseTakeover(t *testing.T) {
	testutil.All(t, func(ctx context.Context, t *testing.T, db *gorm.DB) {
		prep, storage, car, pieceCID, carFile := setupDeletablePiece(t, db, "stale.car", true, false)
		staleAt := time.Now().UTC().Add(-10 * time.Minute)
		seedDeletionState(t, db, car, model.PieceDeletionDeleting,
			"dead-process:beef", &staleAt, manifestFor(storage, car), "")

		err := Default.DeletePieceHandler(ctx, db, prep.Name, pieceCID.String(), DeletePieceRequest{DeleteCar: false})
		require.NoError(t, err)

		// Manifest said delete the external object, so the takeover does it
		// regardless of this call's DeleteCar flag ("original manifest").
		_, err = os.Stat(carFile)
		require.True(t, os.IsNotExist(err))
		var cars, blocks int64
		db.Model(&model.Car{}).Count(&cars)
		require.Zero(t, cars)
		db.Model(&model.CarBlock{}).Count(&blocks)
		require.Zero(t, blocks)
	})
}

// TestPieceDeletion_ResumePendingAfterDatabaseFailure simulates the reverse
// failure mode: the object is already gone but the database phase failed and
// the piece was parked pending. Resuming completes the database phase.
func TestPieceDeletion_ResumePendingAfterDatabaseFailure(t *testing.T) {
	testutil.All(t, func(ctx context.Context, t *testing.T, db *gorm.DB) {
		prep, storage, car, pieceCID, carFile := setupDeletablePiece(t, db, "dbfail.car", true, false)
		require.NoError(t, os.Remove(carFile))

		seedDeletionState(t, db, car, model.PieceDeletionPending, "", nil, manifestFor(storage, car),
			"database phase failed: simulated")

		err := Default.DeletePieceHandler(ctx, db, prep.Name, pieceCID.String(), DeletePieceRequest{DeleteCar: true})
		require.NoError(t, err)

		var cars, blocks int64
		db.Model(&model.Car{}).Count(&cars)
		require.Zero(t, cars)
		db.Model(&model.CarBlock{}).Count(&blocks)
		require.Zero(t, blocks)
	})
}

// TestPieceDeletion_ConcurrentDuplicateSingleOwner verifies that two
// simultaneous delete calls result in exactly one external removal: one call
// succeeds and the other gets not-found, never a double owner.
func TestPieceDeletion_ConcurrentDuplicateSingleOwner(t *testing.T) {
	testutil.All(t, func(ctx context.Context, t *testing.T, db *gorm.DB) {
		prep, _, _, pieceCID, carFile := setupDeletablePiece(t, db, "concurrent.car", false, false)

		var removeCount atomic.Int32
		var hookMu sync.Mutex
		installControllableStore(t, db, &removeCount, func(remote string) error {
			// Hold the first (and only) removal long enough for the duplicate
			// request to queue up behind the in-process owner lock.
			hookMu.Lock()
			defer hookMu.Unlock()
			time.Sleep(150 * time.Millisecond)
			return nil
		})

		var wg sync.WaitGroup
		errs := make([]error, 2)
		wg.Add(2)
		go func() {
			defer wg.Done()
			errs[0] = Default.DeletePieceHandler(ctx, db, prep.Name, pieceCID.String(), DeletePieceRequest{DeleteCar: true})
		}()
		go func() {
			defer wg.Done()
			errs[1] = Default.DeletePieceHandler(ctx, db, prep.Name, pieceCID.String(), DeletePieceRequest{DeleteCar: true})
		}()
		wg.Wait()

		require.NotEqual(t, errs[0] == nil, errs[1] == nil, "exactly one call must succeed")
		successIdx := 0
		if errs[1] == nil {
			successIdx = 1
		}
		require.NoError(t, errs[successIdx])
		require.ErrorIs(t, errs[1-successIdx], handlererror.ErrNotFound)
		require.Equal(t, int32(1), removeCount.Load())

		_, err := os.Stat(carFile)
		require.True(t, os.IsNotExist(err))
		var cars int64
		db.Model(&model.Car{}).Count(&cars)
		require.Zero(t, cars)
	})
}

// TestPieceDeletion_PendingVisibleToListPieces verifies a parked piece stays
// queryable through the list pieces API with its deletion state exposed.
func TestPieceDeletion_PendingVisibleToListPieces(t *testing.T) {
	testutil.All(t, func(ctx context.Context, t *testing.T, db *gorm.DB) {
		prep, storage, car, _, _ := setupDeletablePiece(t, db, "visible.car", false, false)
		// ListPieces requires at least one source attachment.
		require.NoError(t, db.Create(&model.SourceAttachment{PreparationID: prep.ID, StorageID: storage.ID}).Error)
		seedDeletionState(t, db, car, model.PieceDeletionPending, "", nil, manifestFor(storage, car),
			"failed to delete: simulated")

		pieces, err := Default.ListPiecesHandler(ctx, db, prep.Name)
		require.NoError(t, err)
		var found *model.Car
		for _, group := range pieces {
			for i := range group.Pieces {
				if group.Pieces[i].ID == car.ID {
					found = &group.Pieces[i]
				}
			}
		}
		require.NotNil(t, found)
		require.Equal(t, model.PieceDeletionPending, found.DeleteState)
		require.Contains(t, found.DeleteError, "simulated")
	})
}

// TestPieceDeletion_DataPieceResetRunsFromManifest verifies the historical
// data-piece reset semantics still hold on the manifest-driven finalize.
func TestPieceDeletion_DataPieceResetRunsFromManifest(t *testing.T) {
	testutil.All(t, func(ctx context.Context, t *testing.T, db *gorm.DB) {
		prep := model.Preparation{Name: "reset-manifest-prep", SourceStorages: []model.Storage{{}}}
		require.NoError(t, db.Create(&prep).Error)

		job := model.Job{AttachmentID: ptr.Of(model.SourceAttachmentID(1)), State: model.Complete}
		require.NoError(t, db.Create(&job).Error)

		pieceCID := testPieceCID("manifest-reset")
		car := model.Car{
			PieceCID:      model.CID(pieceCID),
			PreparationID: &prep.ID,
			JobID:         &job.ID,
			PieceType:     model.DataPiece,
		}
		require.NoError(t, db.Create(&car).Error)

		file := model.File{AttachmentID: ptr.Of(model.SourceAttachmentID(1)), Path: "t.txt", Size: 10}
		require.NoError(t, db.Create(&file).Error)
		fileRange := model.FileRange{FileID: file.ID, JobID: &job.ID, Offset: 0, Length: 10}
		require.NoError(t, db.Create(&fileRange).Error)

		require.NoError(t, ExecutePieceDeletion(ctx, db, car.ID, DeletePieceRequest{DeleteCar: false}))

		var fr model.FileRange
		require.NoError(t, db.First(&fr, fileRange.ID).Error)
		require.Nil(t, fr.JobID)
		var cars int64
		db.Model(&model.Car{}).Count(&cars)
		require.Zero(t, cars)
	})
}

// TestPieceDeletion_CheckOtherErrorParksPending verifies a non-not-found
// Check error (credentials, network) parks pending instead of warning and
// orphaning the storage object.
func TestPieceDeletion_CheckOtherErrorParksPending(t *testing.T) {
	testutil.All(t, func(ctx context.Context, t *testing.T, db *gorm.DB) {
		prep, _, car, pieceCID, carFile := setupDeletablePiece(t, db, "checkerr.car", false, false)

		var removeCount atomic.Int32
		original := newPieceObjectStore
		newPieceObjectStore = func(ctx context.Context, gdb *gorm.DB, storageID model.StorageID) (objectStore, error) {
			return &erroringStore{checkErr: errors.New("storage unavailable"), removeCount: &removeCount}, nil
		}
		t.Cleanup(func() { newPieceObjectStore = original })

		err := Default.DeletePieceHandler(ctx, db, prep.Name, pieceCID.String(), DeletePieceRequest{DeleteCar: true})
		require.Error(t, err)

		var frozen model.Car
		require.NoError(t, db.First(&frozen, car.ID).Error)
		require.Equal(t, model.PieceDeletionPending, frozen.DeleteState)
		require.Contains(t, frozen.DeleteError, "failed to check")
		_, err = os.Stat(carFile)
		require.NoError(t, err)
		require.Zero(t, removeCount.Load())
	})
}

type erroringStore struct {
	checkErr    error
	removeCount *atomic.Int32
}

func (s *erroringStore) Check(context.Context, string) (fs.DirEntry, error) {
	return nil, s.checkErr
}

func (s *erroringStore) Remove(context.Context, fs.Object) error {
	s.removeCount.Add(1)
	return nil
}

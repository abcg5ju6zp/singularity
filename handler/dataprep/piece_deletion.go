package dataprep

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/data-preservation-programs/singularity/database"
	"github.com/data-preservation-programs/singularity/handler/handlererror"
	"github.com/data-preservation-programs/singularity/model"
	"github.com/data-preservation-programs/singularity/storagesystem"
	"github.com/google/uuid"
	"github.com/rclone/rclone/fs"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// pieceDeletionStaleThreshold is the lease duration for an in-progress piece
// deletion. It mirrors the worker staleness threshold: once DeleteLockedAt is
// older than this the owning process is presumed dead (or the request was
// lost) and another owner may take over using the frozen manifest.
const pieceDeletionStaleThreshold = 5 * time.Minute

// OrphanedPendingResumeLimit bounds how many orphaned pending pieces a single
// healthcheck cycle advances.
const OrphanedPendingResumeLimit = 20

// objectStore is the storage seam used while deleting an external CAR file.
// storagesystem.Handler satisfies it; tests may replace newPieceObjectStore
// with a factory returning a fake implementation.
type objectStore interface {
	Check(ctx context.Context, path string) (fs.DirEntry, error)
	Remove(ctx context.Context, obj fs.Object) error
}

// newPieceObjectStore builds the storage backend for the external CAR. The
// storage is loaded fresh from the database (rather than trusted from the
// frozen car row) so deletion still works after concurrent preparation
// removal detaches associations.
var newPieceObjectStore = func(ctx context.Context, db *gorm.DB, storageID model.StorageID) (objectStore, error) {
	var storage model.Storage
	err := db.WithContext(ctx).First(&storage, storageID).Error
	if err != nil {
		return nil, errors.WithStack(err)
	}
	handler, err := storagesystem.NewRCloneHandler(ctx, storage)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return handler, nil
}

var (
	processOwnerOnce  sync.Once
	processOwnerToken string
)

// currentOwnerToken returns the stable per-process owner identifier. It is
// prefixed with the database instance id so ownership can be attributed in
// logs across multiple deployments sharing nothing but the process UUID.
func currentOwnerToken(ctx context.Context, db *gorm.DB) string {
	processOwnerOnce.Do(func() {
		instance := "unknown"
		var global model.Global
		err := db.WithContext(ctx).Where("key = ?", "instance_id").First(&global).Error
		if err == nil && global.Value != "" {
			instance = global.Value
		}
		processOwnerToken = instance + ":" + uuid.NewString()
	})
	return processOwnerToken
}

// carDeletionLocks serializes deletion attempts for the same car inside one
// process. Cross-process serialization is provided by the row lock in
// acquirePieceDeletion; this map mainly fails duplicate requests fast
// without waiting on the database.
var carDeletionLocks sync.Map

func withCarDeletionLock(carID model.CarID, fn func() error) error {
	mu, _ := carDeletionLocks.LoadOrStore(carID, &sync.Mutex{})
	mu.(*sync.Mutex).Lock()
	defer mu.(*sync.Mutex).Unlock()
	return fn()
}

// HasFrozenPiece reports whether any car with the given piece CID is frozen
// in a deleting/pending state. Reference-producing flows (deal scheduling,
// manual deals, AddPiece) call this to prevent new references from entering
// a piece that is being deleted.
func HasFrozenPiece(ctx context.Context, db *gorm.DB, pieceCID model.CID) (bool, error) {
	var count int64
	err := db.WithContext(ctx).Model(&model.Car{}).
		Where("piece_cid = ? AND delete_state <> ?", pieceCID, model.PieceDeletionNone).
		Count(&count).Error
	if err != nil {
		return false, errors.WithStack(err)
	}
	return count > 0, nil
}

// freezePieceDeletion snapshots the deletion generation (the deals, car
// blocks, file range reset key, directory reset key and external storage
// location) and marks the car as deleting. The snapshot is the only source
// of truth for the rest of the attempt, so concurrent preparation removal
// (SET NULL cascades) cannot change what the deletion acts on.
func freezePieceDeletion(tx *gorm.DB, car *model.Car, request DeletePieceRequest, now time.Time) (*model.PieceDeletionManifest, error) {
	var deals []model.Deal
	err := tx.Where("piece_cid = ?", car.PieceCID).Find(&deals).Error
	if err != nil {
		return nil, errors.Wrap(err, "failed to freeze deals")
	}
	if len(deals) > 0 && !request.Force {
		return nil, errors.Wrapf(handlererror.ErrInvalidParameter,
			"piece has %d deals; use --force to delete anyway", len(deals))
	}

	var carBlockCount int64
	err = tx.Model(&model.CarBlock{}).Where("car_id = ?", car.ID).Count(&carBlockCount).Error
	if err != nil {
		return nil, errors.Wrap(err, "failed to freeze car blocks")
	}

	manifest := &model.PieceDeletionManifest{
		FrozenAt:      now,
		DeleteCar:     request.DeleteCar,
		PieceType:     car.PieceType,
		PreparationID: car.PreparationID,
		StorageID:     car.StorageID,
		StoragePath:   car.StoragePath,
		JobID:         car.JobID,
		AttachmentID:  car.AttachmentID,
		CarBlockCount: carBlockCount,
	}
	manifest.DealIDs = make([]model.DealID, 0, len(deals))
	for _, deal := range deals {
		manifest.DealIDs = append(manifest.DealIDs, deal.ID)
	}
	return manifest, nil
}

// acquirePieceDeletion locks the car row and either freezes a new deletion
// generation or takes over an existing one.
//
//   - state == "": requires allowFreeze (the HTTP/CLI handler); freezes the
//     manifest and applies the non-force Deal rejection.
//   - state == deleting: the same live owner gets a conflict; a stale lock
//     (owner presumed dead, e.g. after a process restart) is taken over and
//     the original manifest is reused.
//   - state == pending: any caller may resume immediately using the original
//     manifest; the request flags from the new call are intentionally
//     ignored ("continue per the original manifest").
func acquirePieceDeletion(
	ctx context.Context,
	db *gorm.DB,
	carID model.CarID,
	request DeletePieceRequest,
	allowFreeze bool,
) (*model.PieceDeletionManifest, error) {
	owner := currentOwnerToken(ctx, db)
	now := time.Now().UTC()

	var manifest *model.PieceDeletionManifest
	err := database.DoRetry(ctx, func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			var car model.Car
			// SELECT ... FOR UPDATE on postgres; the clause is stripped by
			// the sqlite driver, where whole-database write locks serialize.
			err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&car, carID).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// A duplicate request queued behind the owner can reach here
				// after the owner already removed the row.
				return errors.Wrapf(handlererror.ErrNotFound, "piece with car_id %d not found", carID)
			}
			if err != nil {
				return errors.WithStack(err)
			}

			updates := map[string]any{
				"delete_state":       model.PieceDeletionDeleting,
				"delete_owner_token": owner,
				"delete_locked_at":   now,
				"delete_error":       "",
			}

			switch car.DeleteState {
			case model.PieceDeletionNone:
				if !allowFreeze {
					return errors.Wrapf(handlererror.ErrConflict,
						"piece %d is not frozen for deletion", carID)
				}
				m, err := freezePieceDeletion(tx, &car, request, now)
				if err != nil {
					return err
				}
				updates["delete_manifest"] = m
				manifest = m
			case model.PieceDeletionDeleting:
				live := car.DeleteLockedAt != nil &&
					now.Sub(*car.DeleteLockedAt) < pieceDeletionStaleThreshold
				if car.DeleteOwnerToken == owner || live {
					return errors.Wrapf(handlererror.ErrConflict,
						"piece deletion is already in progress (car_id=%d)", carID)
				}
				if car.DeleteManifest == nil {
					return errors.Wrapf(handlererror.ErrConflict,
						"stale piece deletion without manifest cannot be resumed (car_id=%d)", carID)
				}
				logger.Warnw("taking over stale piece deletion",
					"car_id", car.ID, "previous_owner", car.DeleteOwnerToken,
					"locked_at", safeLockedAt(car.DeleteLockedAt), "new_owner", owner)
				manifest = car.DeleteManifest
			case model.PieceDeletionPending:
				if car.DeleteManifest == nil {
					return errors.Errorf("pending piece deletion without manifest cannot be resumed (car_id=%d)", carID)
				}
				logger.Infow("resuming pending piece deletion",
					"car_id", carID, "previous_error", car.DeleteError, "owner", owner)
				updates["delete_manifest"] = car.DeleteManifest
				manifest = car.DeleteManifest
			default:
				return errors.Errorf("unknown piece deletion state %q (car_id=%d)", car.DeleteState, carID)
			}

			return errors.WithStack(tx.Model(&model.Car{}).Where("id = ?", carID).Updates(updates).Error)
		})
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return manifest, nil
}

// isObjectMissing reports whether an rclone error means the object does not
// exist (the desired end state of deletion).
func isObjectMissing(err error) bool {
	return errors.Is(err, fs.ErrorObjectNotFound) || errors.Is(err, fs.ErrorDirNotFound)
}

// safeLockedAt returns a logger-safe value for a possibly nil lock timestamp:
// a nil *time.Time handed to the sugared logger can panic through its
// fmt.Stringer element type.
func safeLockedAt(t *time.Time) any {
	if t == nil {
		return nil
	}
	return *t
}

// confirmExternalObjectDeleted drives the external CAR removal to a confirmed
// fact. It returns true when the object is known to be absent; otherwise it
// returns false together with a human-readable reason explaining why the
// deletion must remain pending.
//
// When Remove returns an uncertain error the object is re-checked: an object
// that no longer exists is treated as a successful delete instead of
// orphaning the database record, while an object that still exists blocks
// the database phase instead of orphaning the storage object.
func confirmExternalObjectDeleted(ctx context.Context, db *gorm.DB, carID model.CarID, manifest *model.PieceDeletionManifest) (bool, string) {
	if !manifest.DeleteCar || manifest.StorageID == nil || manifest.StoragePath == "" {
		return true, ""
	}

	store, err := newPieceObjectStore(ctx, db, *manifest.StorageID)
	if err != nil {
		return false, "failed to connect to storage: " + err.Error()
	}

	entry, err := store.Check(ctx, manifest.StoragePath)
	if err != nil {
		if isObjectMissing(err) {
			logger.Warnw("CAR file already absent from storage, continuing with database cleanup",
				"car_id", carID, "storage_id", *manifest.StorageID, "path", manifest.StoragePath)
			return true, ""
		}
		return false, "failed to check CAR file " + manifest.StoragePath + ": " + err.Error()
	}

	obj, ok := entry.(fs.Object)
	if !ok {
		return false, manifest.StoragePath + " is not a file object"
	}

	err = store.Remove(ctx, obj)
	if err == nil {
		logger.Infow("deleted CAR file from storage",
			"car_id", carID, "storage_id", *manifest.StorageID, "path", manifest.StoragePath)
		return true, ""
	}

	// The remove response is uncertain (timeout, connection reset, ...).
	// Read the object fact instead of trusting the error: only proceed when
	// the object is confirmed gone.
	logger.Warnw("CAR removal returned an error, verifying object fact",
		"car_id", carID, "path", manifest.StoragePath, "err", err)
	retryEntry, retryErr := store.Check(ctx, manifest.StoragePath)
	if retryErr != nil {
		if isObjectMissing(retryErr) {
			logger.Warnw("CAR removal uncertain but object confirmed absent",
				"car_id", carID, "path", manifest.StoragePath)
			return true, ""
		}
		return false, "failed to delete CAR file " + manifest.StoragePath + " (" + err.Error() +
			"); object state unreadable: " + retryErr.Error()
	}
	if _, ok := retryEntry.(fs.Object); ok {
		return false, "failed to delete CAR file " + manifest.StoragePath + ": object still present: " + err.Error()
	}
	return false, "failed to delete CAR file " + manifest.StoragePath + ": path is no longer a file object: " + err.Error()
}

// finalizePieceDeletion performs the database phase against the frozen
// generation: the piece-type-specific resets (unchanged historical
// semantics), car_blocks removal and the car record removal, all in one
// transaction. The lock heartbeat is refreshed first so a slow finalize can
// not be mistaken for a dead owner.
func finalizePieceDeletion(ctx context.Context, db *gorm.DB, carID model.CarID, manifest *model.PieceDeletionManifest) error {
	return database.DoRetry(ctx, func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Model(&model.Car{}).Where("id = ?", carID).
				Update("delete_locked_at", time.Now().UTC()).Error; err != nil {
				return errors.Wrap(err, "failed to refresh piece deletion lock")
			}

			switch manifest.PieceType {
			case model.DataPiece:
				if manifest.JobID != nil {
					if err := tx.Model(&model.FileRange{}).
						Where("job_id = ?", *manifest.JobID).
						Update("job_id", nil).Error; err != nil {
						return errors.Wrap(err, "failed to reset file ranges")
					}
					logger.Infow("reset file ranges for re-packing", "job_id", *manifest.JobID)
				}
			case model.DagPiece:
				if manifest.AttachmentID != nil {
					if err := tx.Model(&model.Directory{}).
						Where("attachment_id = ?", *manifest.AttachmentID).
						Update("exported", false).Error; err != nil {
						return errors.Wrap(err, "failed to reset directory export flags")
					}
					logger.Infow("reset directories for DAG re-generation", "attachment_id", *manifest.AttachmentID)
				}
			}

			result := tx.Where("car_id = ?", carID).Delete(&model.CarBlock{})
			if result.Error != nil {
				return errors.Wrap(result.Error, "failed to delete car blocks")
			}
			logger.Infow("deleted car blocks", "car_id", carID,
				"count", result.RowsAffected, "frozen_count", manifest.CarBlockCount)

			// Delete the frozen generation only; if the row is no longer in
			// deleting state something else took over and we must back off.
			result = tx.Where("id = ? AND delete_state = ?", carID, model.PieceDeletionDeleting).
				Delete(&model.Car{})
			if result.Error != nil {
				return errors.Wrap(result.Error, "failed to delete car record")
			}
			if result.RowsAffected == 0 {
				return errors.Wrapf(handlererror.ErrConflict,
					"piece deletion ownership changed during finalize (car_id=%d)", carID)
			}
			return nil
		})
	})
}

// markPieceDeletionPending parks a deletion that cannot be completed. The car
// and its car_blocks indexes are intentionally kept so the piece stays
// queryable and the frozen manifest can drive a later resume.
func markPieceDeletionPending(ctx context.Context, db *gorm.DB, carID model.CarID, reason string) {
	err := database.DoRetry(ctx, func() error {
		return db.Model(&model.Car{}).Where("id = ?", carID).Updates(map[string]any{
			"delete_state":       model.PieceDeletionPending,
			"delete_owner_token": "",
			"delete_locked_at":   nil,
			"delete_error":       reason,
		}).Error
	})
	if err != nil {
		logger.Errorw("failed to park piece deletion as pending", "car_id", carID, "reason", reason, "err", err)
	}
}

// advancePieceDeletion runs the external-then-database phases for an already
// acquired deletion. Any failure parks the piece as pending with its manifest
// intact and returns an error; success removes the piece entirely.
func advancePieceDeletion(ctx context.Context, db *gorm.DB, carID model.CarID, manifest *model.PieceDeletionManifest) error {
	if absent, reason := confirmExternalObjectDeleted(ctx, db, carID, manifest); !absent {
		markPieceDeletionPending(ctx, db, carID, reason)
		return errors.Errorf("piece deletion parked as pending (car_id=%d): %s", carID, reason)
	}

	err := finalizePieceDeletion(ctx, db, carID, manifest)
	if err != nil {
		// The external object is confirmed absent; keep the manifest so a
		// retry skips straight back to the database phase.
		markPieceDeletionPending(ctx, db, carID, "database phase failed: "+err.Error())
		return errors.WithStack(err)
	}

	logger.Infow("successfully deleted piece",
		"car_id", carID, "frozen_deals", len(manifest.DealIDs),
		"piece_type", manifest.PieceType, "delete_car", manifest.DeleteCar)
	return nil
}

// ExecutePieceDeletion is the HTTP/CLI entry after the car row has been
// located. It freezes a new generation (or resumes an existing one) and runs
// the deletion to completion or to a queryable pending state.
func ExecutePieceDeletion(
	ctx context.Context,
	db *gorm.DB,
	carID model.CarID,
	request DeletePieceRequest,
) error {
	return withCarDeletionLock(carID, func() error {
		manifest, err := acquirePieceDeletion(ctx, db, carID, request, true)
		if err != nil {
			return errors.WithStack(err)
		}
		return advancePieceDeletion(ctx, db, carID, manifest)
	})
}

// ResumePieceDeletion resumes an already frozen piece deletion (pending, or a
// stale deleting lease after a restart) using its stored manifest. It is used
// for duplicate/retry requests that already hold the car id and by the
// healthcheck for pieces whose preparation was removed concurrently.
func ResumePieceDeletion(ctx context.Context, db *gorm.DB, carID model.CarID) error {
	return withCarDeletionLock(carID, func() error {
		manifest, err := acquirePieceDeletion(ctx, db, carID, DeletePieceRequest{}, false)
		if err != nil {
			return errors.WithStack(err)
		}
		return advancePieceDeletion(ctx, db, carID, manifest)
	})
}

// ReconcileStalePieceDeletions converts deleting leases older than the
// staleness threshold to pending. A crashed or timed-out owner can never
// block the piece forever: after reconciliation it is queryable as pending
// and resumable.
func ReconcileStalePieceDeletions(ctx context.Context, db *gorm.DB, now time.Time) (int, error) {
	cutoff := now.Add(-pieceDeletionStaleThreshold)
	var staleCars []model.Car
	err := db.WithContext(ctx).
		Where("delete_state = ? AND delete_locked_at < ?", model.PieceDeletionDeleting, cutoff).
		Find(&staleCars).Error
	if err != nil {
		return 0, errors.Wrap(err, "failed to find stale piece deletions")
	}

	reconciled := 0
	for i := range staleCars {
		carID := staleCars[i].ID
		err := database.DoRetry(ctx, func() error {
			return db.Transaction(func(tx *gorm.DB) error {
				var car model.Car
				err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&car, carID).Error
				if err != nil {
					return errors.WithStack(err)
				}
				if car.DeleteState != model.PieceDeletionDeleting ||
					car.DeleteLockedAt == nil ||
					!car.DeleteLockedAt.Before(cutoff) {
					return nil
				}
				result := tx.Model(&model.Car{}).Where("id = ?", carID).Updates(map[string]any{
					"delete_state":       model.PieceDeletionPending,
					"delete_owner_token": "",
					"delete_locked_at":   nil,
					"delete_error":       "previous deletion was interrupted, retry required",
				})
				if result.Error != nil {
					return errors.WithStack(result.Error)
				}
				if result.RowsAffected > 0 {
					logger.Warnw("stale piece deletion parked as pending", "car_id", carID,
						"previous_owner", car.DeleteOwnerToken, "locked_at", safeLockedAt(car.DeleteLockedAt))
				}
				return nil
			})
		})
		if err != nil {
			return reconciled, errors.Wrapf(err, "failed to reconcile stale piece deletion %d", carID)
		}
		reconciled++
	}
	return reconciled, nil
}

// ResumeOrphanedPendingPieceDeletions advances pending pieces whose
// preparation no longer exists (SET NULL cascade), because for those there is
// no preparation-scoped API call that could retry. This guarantees that a
// preparation removal racing with a piece deletion still leaves exactly one
// owner finishing the work, with neither orphaned storage objects nor ghost
// database rows.
func ResumeOrphanedPendingPieceDeletions(ctx context.Context, db *gorm.DB, limit int) (int, error) {
	var cars []model.Car
	err := db.WithContext(ctx).
		Where("preparation_id IS NULL AND delete_state = ?", model.PieceDeletionPending).
		Limit(limit).
		Find(&cars).Error
	if err != nil {
		return 0, errors.Wrap(err, "failed to find orphaned pending piece deletions")
	}

	advanced := 0
	for i := range cars {
		if ctx.Err() != nil {
			return advanced, errors.WithStack(ctx.Err())
		}
		err := ResumePieceDeletion(ctx, db, cars[i].ID)
		if err != nil {
			logger.Errorw("failed to resume orphaned pending piece deletion",
				"car_id", cars[i].ID, "error", cars[i].DeleteError, "err", err)
			continue
		}
		advanced++
	}
	return advanced, nil
}

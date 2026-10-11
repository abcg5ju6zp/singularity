package storagesystem

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/data-preservation-programs/singularity/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// This file implements storage configuration generations.
//
// A storage update never mutates an existing generation: it validates the new
// backend, appends a new immutable StorageGeneration and atomically flips the
// storage's active_generation_id pointer. Operations resolve their backend
// through one of the Snapshot* helpers instead of reading the live storages
// row:
//
//   - new operations pin the generation active when they start,
//   - already claimed jobs, readers and service requests keep the generation
//     they first pinned until they finish,
//   - delayed retries and restarts recover the same pin from the database.
//
// When a storage has no generation rows at all (legacy rows or test fixtures
// that bypass the storage handlers), the helpers transparently fall back to
// the storages row itself, so all existing rclone backends keep working.

var ErrGenerationNotFound = errors.New("storage generation not found")

func withContext(ctx context.Context, db *gorm.DB) *gorm.DB {
	return db.WithContext(ctx)
}

// GetGeneration fetches a single immutable generation.
func GetGeneration(ctx context.Context, db *gorm.DB, id model.StorageGenerationID) (model.StorageGeneration, error) {
	var generation model.StorageGeneration
	err := withContext(ctx, db).First(&generation, id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return model.StorageGeneration{}, errors.Wrapf(ErrGenerationNotFound, "generation %d", id)
	}
	return generation, errors.WithStack(err)
}

// ActiveGeneration returns the generation new operations must pin. It returns
// gorm.ErrRecordNotFound (possibly wrapped) when the storage has no
// active generation, in which case callers fall back to the storages row.
func ActiveGeneration(ctx context.Context, db *gorm.DB, storageID model.StorageID) (model.StorageGeneration, error) {
	var generation model.StorageGeneration
	err := withContext(ctx, db).
		Joins("JOIN storages ON storages.active_generation_id = storage_generations.id").
		Where("storages.id = ?", storageID).
		First(&generation).Error
	return generation, errors.WithStack(err)
}

// GenerationSnapshot resolves a specific generation to a model.Storage value
// usable with NewRCloneHandler.
func GenerationSnapshot(ctx context.Context, db *gorm.DB, id model.StorageGenerationID) (model.Storage, error) {
	generation, err := GetGeneration(ctx, db, id)
	if err != nil {
		return model.Storage{}, err
	}
	return generation.Snapshot(), nil
}

// ActiveSnapshot resolves the active generation for a storage. Storages
// without any generations fall back to the live storages row.
func ActiveSnapshot(ctx context.Context, db *gorm.DB, storageID model.StorageID) (model.Storage, error) {
	generation, err := ActiveGeneration(ctx, db, storageID)
	if err == nil {
		return generation.Snapshot(), nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return model.Storage{}, err
	}

	var storage model.Storage
	err = withContext(ctx, db).First(&storage, storageID).Error
	return storage, errors.WithStack(err)
}

// SnapshotByRef resolves the backend snapshot a persisted reference points at.
// A pinned generation always wins; a nil pin (legacy row) resolves to the
// active generation of the referenced storage instead.
func SnapshotByRef(ctx context.Context, db *gorm.DB, storageID model.StorageID, generationID *model.StorageGenerationID) (model.Storage, error) {
	if generationID != nil {
		return GenerationSnapshot(ctx, db, *generationID)
	}
	return ActiveSnapshot(ctx, db, storageID)
}

// ActiveSnapshots resolves the active generation for every given storage, in
// the same order. Empty input returns nil.
func ActiveSnapshots(ctx context.Context, db *gorm.DB, storages []model.Storage) ([]model.Storage, error) {
	if len(storages) == 0 {
		return nil, nil
	}
	snapshots := make([]model.Storage, 0, len(storages))
	for _, storage := range storages {
		snapshot, err := ActiveSnapshot(ctx, db, storage.ID)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to resolve active generation for storage %d", storage.ID)
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, nil
}

// ClaimJobGeneration pins the source generation a job must run under, and
// must be called INSIDE the job-claim transaction while the job row is locked.
//
// Behaviour:
//   - a job that already carries a pin (a retry, or a job re-claimed after a
//     worker crash/restart) always keeps that exact generation;
//   - a first claim takes a row lock on the storage before reading the active
//     generation, which serializes against generation activation: the
//     activation order is the single source of truth and a concurrent update
//     can neither rewrite an older job's pin nor delete a generation the job
//     is about to pin;
//   - storages without generations (test fixtures) return nil and leave the
//     caller's preloaded live storage in place, without persisting a pin.
func ClaimJobGeneration(ctx context.Context, tx *gorm.DB, job *model.Job) (*model.StorageGeneration, error) {
	if job.Attachment == nil {
		if job.AttachmentID == nil {
			return nil, errors.New("cannot pin generation: job has no attachment")
		}
		var attachment model.SourceAttachment
		if err := tx.WithContext(ctx).First(&attachment, *job.AttachmentID).Error; err != nil {
			return nil, errors.WithStack(err)
		}
		job.Attachment = &attachment
	}

	if job.SourceGenerationID != nil {
		generation, err := GetGeneration(ctx, tx, *job.SourceGenerationID)
		if err != nil {
			return nil, err
		}
		return &generation, nil
	}

	var storage model.Storage
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&storage, job.Attachment.StorageID).Error
	if err != nil {
		return nil, errors.WithStack(err)
	}

	generation, err := ActiveGeneration(ctx, tx, job.Attachment.StorageID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	err = tx.Model(&model.Job{}).
		Where("id = ? AND source_generation_id IS NULL", job.ID).
		Update("source_generation_id", generation.ID).Error
	if err != nil {
		return nil, errors.WithStack(err)
	}
	job.SourceGenerationID = &generation.ID
	return &generation, nil
}

// ApplyJobSnapshot replaces the in-memory job attachment storage with the
// pinned generation snapshot, so downstream code (scan, pack) builds rclone
// handlers against immutable configuration. A nil generation leaves the
// preloaded storages row untouched.
func ApplyJobSnapshot(job *model.Job, generation *model.StorageGeneration) {
	if generation == nil || job == nil || job.Attachment == nil {
		return
	}
	snapshot := generation.Snapshot()
	job.Attachment.Storage = &snapshot
}

// PinJobSourceGeneration is the standalone equivalent of ClaimJobGeneration
// for operations started outside the worker claim loop (e.g. the manual
// "pack now" API): it opens its own serialized transaction, pins the active
// generation if the job is unpinned and applies the snapshot in memory.
func PinJobSourceGeneration(ctx context.Context, db *gorm.DB, job *model.Job) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		generation, err := ClaimJobGeneration(ctx, tx, job)
		if err != nil {
			return err
		}
		if generation != nil {
			snapshot := generation.Snapshot()
			job.Attachment.Storage = &snapshot
		}
		return nil
	})
}

// GenerationHasReferences reports whether any durable operation still points
// at the generation: attachments, jobs, scanned files or written cars.
// In-process readers are not counted -- they already hold the snapshot in
// memory and never re-read the row, so deleting the row cannot break them.
func GenerationHasReferences(ctx context.Context, db *gorm.DB, generationID model.StorageGenerationID) (bool, error) {
	db = db.WithContext(ctx)
	checks := []func() *gorm.DB{
		func() *gorm.DB {
			return db.Model(&model.SourceAttachment{}).Where("storage_generation_id = ?", generationID)
		},
		func() *gorm.DB {
			return db.Model(&model.OutputAttachment{}).Where("storage_generation_id = ?", generationID)
		},
		func() *gorm.DB { return db.Model(&model.Job{}).Where("source_generation_id = ?", generationID) },
		func() *gorm.DB { return db.Model(&model.File{}).Where("storage_generation_id = ?", generationID) },
		func() *gorm.DB { return db.Model(&model.Car{}).Where("storage_generation_id = ?", generationID) },
	}
	for _, query := range checks {
		var count int64
		if err := query().Count(&count).Error; err != nil {
			return false, errors.WithStack(err)
		}
		if count > 0 {
			return true, nil
		}
	}
	return false, nil
}

// DeleteUnreferencedGenerations removes generations of a storage that no
// longer have any durable reference and are not the active generation. The
// superseded generation is retained while jobs, files or cars still use
// it. Must be called while holding the storage row lock.
func DeleteUnreferencedGenerations(ctx context.Context, db *gorm.DB, storageID model.StorageID) error {
	var generations []model.StorageGeneration
	err := db.WithContext(ctx).
		Joins("JOIN storages ON storages.id = storage_generations.storage_id").
		Where("storage_generations.storage_id = ? AND (storages.active_generation_id IS NULL OR storages.active_generation_id <> storage_generations.id)", storageID).
		Find(&generations).Error
	if err != nil {
		return errors.WithStack(err)
	}
	for _, generation := range generations {
		inUse, err := GenerationHasReferences(ctx, db, generation.ID)
		if err != nil {
			return err
		}
		if inUse {
			continue
		}
		if err := db.Delete(&model.StorageGeneration{}, generation.ID).Error; err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// StorageHasGenerationReferences reports whether a storage cannot be
// deleted: either it is still attached to a preparation, or any of its
// generations is referenced by jobs, scanned files or written cars. Raw
// rows predating the pin columns still count through direct storage_id
// relationships so deletion cannot strand data.
func StorageHasGenerationReferences(ctx context.Context, db *gorm.DB, storageID model.StorageID) (bool, error) {
	db = db.WithContext(ctx)
	checks := []struct {
		table string
		where string
		join  string
	}{
		{"source_attachments", "ref.storage_id = ?", ""},
		{"output_attachments", "ref.storage_id = ?", ""},
		{"cars", "ref.storage_id = ?", ""},
		{"jobs", "gen.storage_id = ?", "JOIN storage_generations gen ON gen.id = ref.source_generation_id"},
		{"files", "gen.storage_id = ?", "JOIN storage_generations gen ON gen.id = ref.storage_generation_id"},
	}
	for _, check := range checks {
		var count int64
		query := db.Table(check.table + " AS ref")
		if check.join != "" {
			query = query.Joins(check.join)
		}
		err := query.Where(check.where, storageID).Count(&count).Error
		if err != nil {
			return false, errors.WithStack(err)
		}
		if count > 0 {
			return true, nil
		}
	}
	return false, nil
}

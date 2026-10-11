package model

import (
	"time"

	"github.com/cockroachdb/errors"
	"gorm.io/gorm"
)

type StorageGenerationID uint32

// StorageGeneration is an immutable snapshot of a Storage's backend
// configuration (type, path, rclone config and client config) at the moment a
// generation was activated.
//
// Every storage update validates the new backend and then appends a new
// generation that is atomically marked active; existing generations are never
// modified. Long lived operations (jobs, readers, downloads, deal submissions)
// pin the generation they started with and keep using it until they finish, so
// a concurrent storage update can never silently switch a running operation to
// a different backend or credential.
//
// A generation may only be deleted after no row in any other table references
// it. Renaming the parent storage does not change the generation identity.
type StorageGeneration struct {
	ID StorageGenerationID `gorm:"primaryKey" json:"id"`

	CreatedAt time.Time `json:"createdAt"`

	// StorageID identifies the storage identity this generation belongs to.
	StorageID StorageID `gorm:"uniqueIndex:idx_storage_generation_version,priority:1;index;not null" json:"storageId"`
	Storage   *Storage  `gorm:"foreignKey:StorageID;constraint:OnDelete:RESTRICT" json:"storage,omitempty" swaggerignore:"true"`

	// Version is the monotonically increasing, per-storage generation number.
	// Activation order of concurrent updates is determined solely by Version.
	Version uint32 `gorm:"uniqueIndex:idx_storage_generation_version,priority:2" json:"version"`

	// Name snapshots the storage name at activation time. Renaming the storage
	// never changes the identity (IDs) of existing generations; later generations
	// simply carry the new name.
	Name string `json:"name"`

	Type         string       `json:"type"`
	Path         string       `json:"path"`
	Config       ConfigMap    `gorm:"type:JSON" json:"config"`
	ClientConfig ClientConfig `gorm:"type:JSON" json:"clientConfig"`
}

// Snapshot returns the generation as a model.Storage value that can be passed
// to storagesystem.NewRCloneHandler.
func (g StorageGeneration) Snapshot() Storage {
	id := g.ID
	return Storage{
		ID:           g.StorageID,
		Name:         g.Name,
		CreatedAt:    g.CreatedAt,
		Type:         g.Type,
		Path:         g.Path,
		Config:       g.Config,
		ClientConfig: g.ClientConfig,
		GenerationID: &id,
	}
}

// backfillStorageGenerations introduces the generations feature on databases
// created before it existed. Every storage is snapshotted as its active "v1"
// generation and all existing references (attachments, jobs, scanned files,
// written cars) are pinned to that snapshot. Idempotent: only missing
// generations and NULL pins are touched, so re-running AutoMigrate is safe.
func backfillStorageGenerations(db *gorm.DB) error {
	if !db.Migrator().HasTable(&StorageGeneration{}) {
		return nil
	}

	var storages []Storage
	if err := db.Find(&storages).Error; err != nil {
		return errors.Wrap(err, "failed to list storages")
	}

	for _, storage := range storages {
		var generation StorageGeneration
		if storage.ActiveGenerationID != nil {
			err := db.First(&generation, *storage.ActiveGenerationID).Error
			if err == nil {
				// already generation-aware; still backfill any NULL refs below
			} else if errors.Is(err, gorm.ErrRecordNotFound) {
				storage.ActiveGenerationID = nil
			} else {
				return errors.Wrapf(err, "failed to look up active generation for storage %d", storage.ID)
			}
		}
		if storage.ActiveGenerationID == nil {
			var maxVersion uint32
			_ = db.Model(&StorageGeneration{}).Where("storage_id = ?", storage.ID).
				Select("COALESCE(MAX(version), 0)").Scan(&maxVersion).Error
			generation = StorageGeneration{
				StorageID:    storage.ID,
				Version:      maxVersion + 1,
				Name:         storage.Name,
				Type:         storage.Type,
				Path:         storage.Path,
				Config:       storage.Config,
				ClientConfig: storage.ClientConfig,
			}
			if err := db.Create(&generation).Error; err != nil {
				return errors.Wrapf(err, "failed to create initial generation for storage %d", storage.ID)
			}
			if err := db.Model(&Storage{}).Where("id = ?", storage.ID).
				Update("active_generation_id", generation.ID).Error; err != nil {
				return errors.Wrapf(err, "failed to activate initial generation for storage %d", storage.ID)
			}
		}

		genID := generation.ID
		if err := db.Model(&SourceAttachment{}).
			Where("storage_id = ? AND storage_generation_id IS NULL", storage.ID).
			Update("storage_generation_id", genID).Error; err != nil {
			return errors.Wrap(err, "failed to backfill source attachment generations")
		}
		if err := db.Model(&OutputAttachment{}).
			Where("storage_id = ? AND storage_generation_id IS NULL", storage.ID).
			Update("storage_generation_id", genID).Error; err != nil {
			return errors.Wrap(err, "failed to backfill output attachment generations")
		}
		if err := db.Model(&File{}).
			Where("storage_generation_id IS NULL AND attachment_id IN (?)",
				db.Model(&SourceAttachment{}).Select("id").Where("storage_id = ?", storage.ID)).
			Update("storage_generation_id", genID).Error; err != nil {
			return errors.Wrap(err, "failed to backfill file generations")
		}
		if err := db.Model(&Job{}).
			Where("source_generation_id IS NULL AND attachment_id IN (?)",
				db.Model(&SourceAttachment{}).Select("id").Where("storage_id = ?", storage.ID)).
			Update("source_generation_id", genID).Error; err != nil {
			return errors.Wrap(err, "failed to backfill job generations")
		}
		if err := db.Model(&Car{}).
			Where("storage_id = ? AND storage_generation_id IS NULL", storage.ID).
			Update("storage_generation_id", genID).Error; err != nil {
			return errors.Wrap(err, "failed to backfill car generations")
		}
	}

	return nil
}

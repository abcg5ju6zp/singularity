package storage

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/data-preservation-programs/singularity/database"
	"github.com/data-preservation-programs/singularity/handler/handlererror"
	"github.com/data-preservation-programs/singularity/model"
	"github.com/data-preservation-programs/singularity/storagesystem"
	"gorm.io/gorm"
)

// RemoveHandler deletes the storage entry with the specified name from the database.
//
// A storage can only be removed if none of its generations is still referenced
// by attachments, jobs, scanned files or written cars -- a delete is therefore
// refused not just while the storage is attached to a preparation, but also
// when orphaned jobs/files/CARs from deleted preparations still pin one of its
// generations. The check and the deletion run in one transaction.
//
// Parameters:
//   - ctx: The context for database transactions and other operations.
//   - db: A pointer to the gorm.DB instance representing the database connection.
//   - name: The ID or name of the storage entry to be deleted.
//
// Returns:
//   - An error, if any occurred during the operation. Returns a specific error
//     if the storage is still in use or if the storage does not exist.
func (DefaultHandler) RemoveHandler(
	ctx context.Context,
	db *gorm.DB,
	name string,
) error {
	db = db.WithContext(ctx)
	err := database.DoRetry(ctx, func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			var storage model.Storage
			err := storage.FindByIDOrName(tx, name)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errors.Wrapf(handlererror.ErrNotFound, "storage %s does not exist", name)
			}
			if err != nil {
				return errors.WithStack(err)
			}

			inUse, err := storagesystem.StorageHasGenerationReferences(ctx, tx, storage.ID)
			if err != nil {
				return errors.WithStack(err)
			}
			if inUse {
				return errors.Wrapf(handlererror.ErrInvalidParameter, "storage %s is still in use", name)
			}

			// all generations are unreferenced; remove them before the
			// storage row (the generations -> storage FK is RESTRICT)
			if err := tx.Where("storage_id = ?", storage.ID).
				Delete(&model.StorageGeneration{}).Error; err != nil {
				return errors.WithStack(err)
			}

			if err := tx.Delete(&storage).Error; err != nil {
				return errors.WithStack(err)
			}
			return nil
		})
	})
	return errors.WithStack(err)
}

// @ID RemoveStorage
// @Summary Remove a storage
// @Tags Storage
// @Param name path string true "Storage ID or name"
// @Success 204
// @Failure 400 {object} api.HTTPError
// @Failure 500 {object} api.HTTPError
// @Router /storage/{name} [delete]
func _() {}

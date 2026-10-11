package dataprep

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/data-preservation-programs/singularity/handler/handlererror"
	"github.com/data-preservation-programs/singularity/model"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-log/v2"
	"gorm.io/gorm"
)

var logger = log.Logger("dataprep")

// DeletePieceRequest contains options for deleting a piece from a preparation.
type DeletePieceRequest struct {
	DeleteCar bool `json:"deleteCar"` // Delete the physical CAR file from storage (default: true)
	Force     bool `json:"force"`     // Delete even if deals reference this piece
}

// DeletePieceHandler deletes a piece (CAR) from a preparation.
//
// Deletion runs as a frozen, owned generation to keep the external CAR file
// and the database indexes from diverging:
//  1. The piece's deals, car_blocks, file range / directory reset keys and
//     external storage location are snapshotted into a manifest and the car
//     is marked "deleting" with an owner lease. While non-empty, no new deal
//     or CAR removal may target the piece.
//  2. The external CAR file is removed; an uncertain response is resolved by
//     re-reading the object fact. Inline pieces and DeleteCar=false skip this.
//  3. The database phase performs the piece-type-specific resets and removes
//     the car_blocks and car record.
//
// If any step cannot be confirmed, the piece remains queryable in "pending"
// state with its indexes and manifest intact; invoking this handler again (or
// the healthcheck for orphaned pieces) resumes from the original manifest. A
// deletion already owned by a live caller returns a conflict.
//
// Piece-type reset semantics are unchanged:
//   - Data pieces: resets file_ranges.job_id to NULL so ranges can be re-packed
//   - DAG pieces: resets directories.exported to false so DAG can be re-generated
//
// Parameters:
//   - ctx: The context for database transactions and other operations.
//   - db: A pointer to the gorm.DB instance representing the database connection.
//   - prepName: The ID or name for the desired Preparation record.
//   - pieceCIDStr: The piece CID (CommP) identifying the piece to delete.
//   - request: Options controlling deletion behavior.
//
// Returns:
//   - An error if the piece doesn't exist, belongs to a different preparation,
//     has active deals (without Force flag), is already being deleted by
//     another owner, or deletion cannot be confirmed (left pending).
func (DefaultHandler) DeletePieceHandler(
	ctx context.Context,
	db *gorm.DB,
	prepName string,
	pieceCIDStr string,
	request DeletePieceRequest,
) error {
	db = db.WithContext(ctx)

	// 1. Parse and validate piece CID
	pieceCID, err := cid.Parse(pieceCIDStr)
	if err != nil {
		return errors.Join(handlererror.ErrInvalidParameter, errors.Wrapf(err, "invalid piece CID %s", pieceCIDStr))
	}
	if pieceCID.Type() != cid.FilCommitmentUnsealed {
		return errors.Wrap(handlererror.ErrInvalidParameter, "piece CID must be commp")
	}

	// 2. Find preparation
	var preparation model.Preparation
	err = preparation.FindByIDOrName(db, prepName)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return errors.Wrapf(handlererror.ErrNotFound, "preparation '%s' does not exist", prepName)
	}
	if err != nil {
		return errors.WithStack(err)
	}

	// 3. Find car by piece_cid AND preparation_id. A car already frozen in
	// deleting/pending state is found the same way so retries resume it.
	var car model.Car
	err = db.Preload("Storage").
		Where("piece_cid = ? AND preparation_id = ?", model.CID(pieceCID), preparation.ID).
		First(&car).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return errors.Wrapf(handlererror.ErrNotFound, "piece '%s' not found in preparation '%s'", pieceCIDStr, prepName)
	}
	if err != nil {
		return errors.WithStack(err)
	}

	// 4. Freeze + own the deletion generation, then confirm the external
	// object and finalize the database phase (see piece_deletion.go).
	return ExecutePieceDeletion(ctx, db, car.ID, request)
}

// @ID DeletePiece
// @Summary Delete a piece from a preparation
// @Description Deletes a piece (CAR) and its associated records. For data pieces, resets file ranges
// @Description to allow re-packing. For DAG pieces, resets directory export flags for re-generation.
// @Description Unconfirmed deletions leave the piece queryable in "pending" state; repeat calls resume.
// @Tags Piece
// @Accept json
// @Produce json
// @Param id path string true "Preparation ID or name"
// @Param piece_cid path string true "Piece CID"
// @Param request body DeletePieceRequest true "Delete options"
// @Success 204 "No Content"
// @Failure 400 {object} api.HTTPError
// @Failure 404 {object} api.HTTPError
// @Failure 409 {object} api.HTTPError
// @Failure 500 {object} api.HTTPError
// @Router /preparation/{id}/piece/{piece_cid} [delete]
func _() {}

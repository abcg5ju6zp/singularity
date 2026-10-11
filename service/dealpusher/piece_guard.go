package dealpusher

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/data-preservation-programs/singularity/model"
	"github.com/ipfs/go-cid"
	"gorm.io/gorm"
)

// frozenPieceCIDSet returns the piece CIDs (as strings) that currently belong
// to a car frozen in deleting/pending state. Deal scheduling consults it
// right before on-chain submission and database persistence so that no new
// deal reference enters a piece while it is being deleted, even if the piece
// was frozen after the scheduling query selected it.
func frozenPieceCIDSet(ctx context.Context, db *gorm.DB, pieceCIDs []model.CID) (map[string]struct{}, error) {
	frozen := make(map[string]struct{})
	if len(pieceCIDs) == 0 {
		return frozen, nil
	}
	var cids []model.CID
	err := db.WithContext(ctx).Model(&model.Car{}).
		Where("piece_cid IN ? AND delete_state <> ?", pieceCIDs, model.PieceDeletionNone).
		Distinct().
		Pluck("piece_cid", &cids).Error
	if err != nil {
		return nil, errors.WithStack(err)
	}
	for _, c := range cids {
		frozen[cid.Cid(c).String()] = struct{}{}
	}
	return frozen, nil
}

// filterFrozenCars drops cars whose piece is frozen. It returns the survivors
// and logs which pieces were skipped.
func filterFrozenCars(db *gorm.DB, ctx context.Context, cars []model.Car, scheduleID model.ScheduleID) ([]model.Car, error) {
	pieceCIDs := make([]model.CID, len(cars))
	for i, car := range cars {
		pieceCIDs[i] = car.PieceCID
	}
	frozen, err := frozenPieceCIDSet(ctx, db, pieceCIDs)
	if err != nil {
		return nil, err
	}
	if len(frozen) == 0 {
		return cars, nil
	}
	survivors := make([]model.Car, 0, len(cars))
	for _, car := range cars {
		if _, ok := frozen[cid.Cid(car.PieceCID).String()]; ok {
			Logger.Warnw("skipping piece frozen for deletion",
				"schedule_id", scheduleID, "piece_cid", cid.Cid(car.PieceCID).String())
			continue
		}
		survivors = append(survivors, car)
	}
	return survivors, nil
}

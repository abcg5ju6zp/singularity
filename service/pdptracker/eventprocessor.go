package pdptracker

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/data-preservation-programs/singularity/database"
	"github.com/data-preservation-programs/singularity/model"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gotidy/ptr"
	"github.com/ipfs/go-cid"
	"gorm.io/gorm"
)

// idempotent — safe to re-process after reorgs
func processNewEvents(ctx context.Context, db *gorm.DB, rpcClient *ChainPDPClient) error {
	// process in dependency order
	if err := processDataSetCreated(ctx, db, rpcClient); err != nil {
		return errors.Wrap(err, "processing DataSetCreated")
	}
	if err := processSPChanged(ctx, db); err != nil {
		return errors.Wrap(err, "processing StorageProviderChanged")
	}
	if err := processPiecesChanged(ctx, db, rpcClient); err != nil {
		return errors.Wrap(err, "processing PiecesAdded/Removed")
	}
	if err := processNextProvingPeriod(ctx, db); err != nil {
		return errors.Wrap(err, "processing NextProvingPeriod")
	}
	if err := processPossessionProven(ctx, db); err != nil {
		return errors.Wrap(err, "processing PossessionProven")
	}
	if err := processDataSetDeleted(ctx, db); err != nil {
		return errors.Wrap(err, "processing DataSetDeleted")
	}
	return nil
}

// eventKey uniquely identifies a shovel inbox row within a table
type eventKey struct {
	BlockNum int64 `gorm:"column:block_num"`
	TxIdx    int   `gorm:"column:tx_idx"`
	LogIdx   int   `gorm:"column:log_idx"`
}

type inboxRow interface {
	setID() uint64
	key() eventKey
}

// processInbox reads all pending rows in deterministic order, processes each
// via fn, then deletes only the exact rows that succeeded (by event key).
// failed rows are retained for retry on next cycle.
func processInbox[R inboxRow](db *gorm.DB, query, table string, fn func(R) error) error {
	var rows []R
	if err := db.Raw(query).Scan(&rows).Error; err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	var successKeys []eventKey
	for _, r := range rows {
		if err := fn(r); err != nil {
			Logger.Errorw("inbox processing failed", "table", table, "setId", r.setID(), "error", err)
		} else {
			successKeys = append(successKeys, r.key())
		}
	}

	return deleteByKeys(db, table, successKeys)
}

// deleteByKeys removes exactly the rows identified by their event keys.
// table is interpolated into sql — pass literals only.
func deleteByKeys(db *gorm.DB, table string, keys []eventKey) error {
	if len(keys) == 0 {
		return nil
	}
	placeholders := make([]string, len(keys))
	args := make([]interface{}, 0, len(keys)*3)
	for i, k := range keys {
		placeholders[i] = "(?, ?, ?)"
		args = append(args, k.BlockNum, k.TxIdx, k.LogIdx)
	}
	return db.Exec(
		"DELETE FROM "+table+" WHERE (block_num, tx_idx, log_idx) IN ("+strings.Join(placeholders, ", ")+")",
		args...,
	).Error
}

type dataSetCreatedRow struct {
	BlockNum        int64  `gorm:"column:block_num"`
	TxIdx           int    `gorm:"column:tx_idx"`
	LogIdx          int    `gorm:"column:log_idx"`
	SetID_          uint64 `gorm:"column:set_id"`
	StorageProvider []byte `gorm:"column:storage_provider"`
}

func (r dataSetCreatedRow) setID() uint64 { return r.SetID_ }
func (r dataSetCreatedRow) key() eventKey { return eventKey{r.BlockNum, r.TxIdx, r.LogIdx} }

func processDataSetCreated(ctx context.Context, db *gorm.DB, rpcClient *ChainPDPClient) error {
	return processInbox(db,
		"SELECT set_id, storage_provider, block_num, tx_idx, log_idx FROM pdp_dataset_created ORDER BY block_num, tx_idx, log_idx",
		"pdp_dataset_created",
		func(r dataSetCreatedRow) error {
			return applyDataSetCreated(ctx, db, rpcClient, r.SetID_, r.BlockNum, r.StorageProvider)
		},
	)
}

// applyDataSetCreated materializes the PDPProofSet for a DataSetCreated log.
func applyDataSetCreated(ctx context.Context, db *gorm.DB, rpcClient *ChainPDPClient, setID uint64, createdBlock int64, storageProvider []byte) error {
	listener, err := rpcClient.GetDataSetListener(ctx, setID)
	if err != nil {
		return errors.Wrapf(err, "getDataSetListener for set %d", setID)
	}

	clientAddr, err := commonToDelegatedAddress(listener)
	if err != nil {
		return errors.Wrap(err, "converting listener address")
	}

	providerAddr, err := commonToDelegatedAddress(common.BytesToAddress(storageProvider))
	if err != nil {
		return errors.Wrap(err, "converting provider address")
	}

	ps := model.PDPProofSet{
		SetID:         setID,
		ClientAddress: clientAddr.String(),
		Provider:      providerAddr.String(),
		CreatedBlock:  createdBlock,
	}

	return database.DoRetry(ctx, func() error {
		return db.Where("set_id = ?", setID).Attrs(ps).FirstOrCreate(&model.PDPProofSet{}).Error
	})
}

func processPiecesChanged(ctx context.Context, db *gorm.DB, rpcClient *ChainPDPClient) error {
	type row struct {
		BlockNum int64  `gorm:"column:block_num"`
		TxIdx    int    `gorm:"column:tx_idx"`
		LogIdx   int    `gorm:"column:log_idx"`
		SetID    uint64 `gorm:"column:set_id"`
	}

	var addedRows, removedRows []row
	if err := db.Raw("SELECT set_id, block_num, tx_idx, log_idx FROM pdp_pieces_added ORDER BY block_num, tx_idx, log_idx").Scan(&addedRows).Error; err != nil {
		return err
	}
	if err := db.Raw("SELECT set_id, block_num, tx_idx, log_idx FROM pdp_pieces_removed ORDER BY block_num, tx_idx, log_idx").Scan(&removedRows).Error; err != nil {
		return err
	}

	setIDs := make(map[uint64]struct{})
	for _, r := range addedRows {
		setIDs[r.SetID] = struct{}{}
	}
	for _, r := range removedRows {
		setIDs[r.SetID] = struct{}{}
	}

	if len(setIDs) == 0 {
		return nil
	}

	failed := make(map[uint64]struct{})
	for id := range setIDs {
		if err := reconcileProofSetPieces(ctx, db, rpcClient, id); err != nil {
			Logger.Errorw("failed to reconcile pieces", "setId", id, "error", err)
			failed[id] = struct{}{}
		}
	}

	var addedKeys, removedKeys []eventKey
	for _, r := range addedRows {
		if _, ok := failed[r.SetID]; !ok {
			addedKeys = append(addedKeys, eventKey{r.BlockNum, r.TxIdx, r.LogIdx})
		}
	}
	for _, r := range removedRows {
		if _, ok := failed[r.SetID]; !ok {
			removedKeys = append(removedKeys, eventKey{r.BlockNum, r.TxIdx, r.LogIdx})
		}
	}

	if err := deleteByKeys(db, "pdp_pieces_added", addedKeys); err != nil {
		return err
	}
	return deleteByKeys(db, "pdp_pieces_removed", removedKeys)
}

func reconcileProofSetPieces(ctx context.Context, db *gorm.DB, rpcClient *ChainPDPClient, setID uint64) error {
	var ps model.PDPProofSet
	if err := db.Where("set_id = ?", setID).First(&ps).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// proof set not yet materialized locally; DataSetCreated may
			// still be pending retry — retain inbox rows for next cycle
			return errors.Errorf("proof set %d not found, retaining piece events", setID)
		}
		return err
	}
	if ps.Deleted {
		Logger.Debugw("ignoring piece events for deleted proof set", "setId", setID)
		return nil
	}

	var actor model.Actor
	if err := db.Where("address = ?", ps.ClientAddress).First(&actor).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			Logger.Debugw("pieces changed for untracked client", "setId", setID, "client", ps.ClientAddress)
			return nil
		}
		return err
	}

	var walletID *uint
	var wallet model.Wallet
	if err := db.Where("actor_id = ?", actor.ID).First(&wallet).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return errors.Wrapf(err, "failed to look up wallet for actor %s", actor.ID)
		}
	} else {
		walletID = &wallet.ID
	}

	pieces, err := rpcClient.GetActivePieces(ctx, setID)
	if err != nil {
		return errors.Wrapf(err, "getActivePieces for set %d", setID)
	}

	activeCIDs := make(map[string]cid.Cid, len(pieces))
	for _, c := range pieces {
		if c != cid.Undef {
			activeCIDs[c.String()] = c
		}
	}

	now := time.Now()
	initialState := model.DealPublished
	if ps.IsLive {
		initialState = model.DealActive
	}

	var hadErrors bool

	for _, pieceCID := range activeCIDs {
		modelCID := model.CID(pieceCID)
		err = database.DoRetry(ctx, func() error {
			var existing model.Deal
			result := db.Where("proof_set_id = ? AND piece_cid = ? AND deal_type = ?",
				setID, modelCID, model.DealTypePDP).First(&existing)
			if result.Error == nil {
				if existing.State == model.DealExpired {
					return db.Model(&model.Deal{}).Where("id = ?", existing.ID).
						Update("state", initialState).Error
				}
				return nil
			}
			if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return result.Error
			}
			return db.Create(&model.Deal{
				DealType:       model.DealTypePDP,
				State:          initialState,
				ClientID:       actor.ID,
				WalletID:       walletID,
				Provider:       ps.Provider,
				PieceCID:       modelCID,
				ProofSetID:     ptr.Of(setID),
				ProofSetLive:   ptr.Of(ps.IsLive),
				LastVerifiedAt: ptr.Of(now),
			}).Error
		})
		if err != nil {
			Logger.Errorw("failed to upsert deal", "setId", setID, "pieceCid", pieceCID, "error", err)
			hadErrors = true
		}
	}

	var existingDeals []model.Deal
	if err := db.Where("proof_set_id = ? AND deal_type = ? AND state != ?",
		setID, model.DealTypePDP, model.DealExpired).Find(&existingDeals).Error; err != nil {
		return err
	}

	for _, deal := range existingDeals {
		if _, ok := activeCIDs[deal.PieceCID.String()]; !ok {
			err = database.DoRetry(ctx, func() error {
				return db.Model(&model.Deal{}).Where("id = ?", deal.ID).
					Update("state", model.DealExpired).Error
			})
			if err != nil {
				Logger.Errorw("failed to expire removed deal", "dealId", deal.ID, "error", err)
				hadErrors = true
			}
		}
	}

	if hadErrors {
		return errors.Errorf("partial reconciliation failure for proof set %d", setID)
	}
	return nil
}

type nextProvingPeriodRow struct {
	BlockNum       int64  `gorm:"column:block_num"`
	TxIdx          int    `gorm:"column:tx_idx"`
	LogIdx         int    `gorm:"column:log_idx"`
	SetID_         uint64 `gorm:"column:set_id"`
	ChallengeEpoch int64  `gorm:"column:challenge_epoch"`
}

func (r nextProvingPeriodRow) setID() uint64 { return r.SetID_ }
func (r nextProvingPeriodRow) key() eventKey { return eventKey{r.BlockNum, r.TxIdx, r.LogIdx} }

func processNextProvingPeriod(ctx context.Context, db *gorm.DB) error {
	return processInbox(db,
		"SELECT set_id, challenge_epoch, block_num, tx_idx, log_idx FROM pdp_next_proving_period ORDER BY block_num, tx_idx, log_idx",
		"pdp_next_proving_period",
		func(r nextProvingPeriodRow) error {
			return applyNextProvingPeriod(ctx, db, r.SetID_, r.ChallengeEpoch)
		},
	)
}

// applyNextProvingPeriod updates the challenge epoch on a proof set and its
// deals. Missing proof set is an error so the caller retains the event.
func applyNextProvingPeriod(ctx context.Context, db *gorm.DB, setID uint64, challengeEpoch int64) error {
	if challengeEpoch > math.MaxInt32 || challengeEpoch < math.MinInt32 {
		return errors.Errorf("challenge epoch %d overflows int32", challengeEpoch)
	}
	epoch32 := int32(challengeEpoch)

	var rowsAffected int64
	if err := database.DoRetry(ctx, func() error {
		result := db.Model(&model.PDPProofSet{}).Where("set_id = ?", setID).
			Update("challenge_epoch", challengeEpoch)
		rowsAffected = result.RowsAffected
		return result.Error
	}); err != nil {
		return err
	}
	if rowsAffected == 0 {
		return errors.Errorf("proof set %d not found, retaining event", setID)
	}

	return database.DoRetry(ctx, func() error {
		return db.Model(&model.Deal{}).Where("proof_set_id = ? AND deal_type = ?",
			setID, model.DealTypePDP).
			Update("next_challenge_epoch", epoch32).Error
	})
}

type possessionProvenRow struct {
	BlockNum int64  `gorm:"column:block_num"`
	TxIdx    int    `gorm:"column:tx_idx"`
	LogIdx   int    `gorm:"column:log_idx"`
	SetID_   uint64 `gorm:"column:set_id"`
}

func (r possessionProvenRow) setID() uint64 { return r.SetID_ }
func (r possessionProvenRow) key() eventKey { return eventKey{r.BlockNum, r.TxIdx, r.LogIdx} }

func processPossessionProven(ctx context.Context, db *gorm.DB) error {
	now := time.Now()
	return processInbox(db,
		"SELECT set_id, block_num, tx_idx, log_idx FROM pdp_possession_proven ORDER BY block_num, tx_idx, log_idx",
		"pdp_possession_proven",
		func(r possessionProvenRow) error {
			return applyPossessionProven(ctx, db, r.SetID_, now)
		},
	)
}

// applyPossessionProven marks the proof set live and activates its
// non-expired deals, stamping LastVerifiedAt.
func applyPossessionProven(ctx context.Context, db *gorm.DB, setID uint64, now time.Time) error {
	var rowsAffected int64
	if err := database.DoRetry(ctx, func() error {
		result := db.Model(&model.PDPProofSet{}).Where("set_id = ?", setID).
			Update("is_live", true)
		rowsAffected = result.RowsAffected
		return result.Error
	}); err != nil {
		return err
	}
	if rowsAffected == 0 {
		return errors.Errorf("proof set %d not found, retaining event", setID)
	}

	// only activate non-expired deals; expired deals (from piece removal
	// or dataset deletion) must not be resurrected by a later proof
	return database.DoRetry(ctx, func() error {
		return db.Model(&model.Deal{}).
			Where("proof_set_id = ? AND deal_type = ? AND state != ?",
				setID, model.DealTypePDP, model.DealExpired).
			Updates(map[string]any{
				"proof_set_live":   true,
				"state":            model.DealActive,
				"last_verified_at": now,
			}).Error
	})
}

type dataSetDeletedRow struct {
	BlockNum int64  `gorm:"column:block_num"`
	TxIdx    int    `gorm:"column:tx_idx"`
	LogIdx   int    `gorm:"column:log_idx"`
	SetID_   uint64 `gorm:"column:set_id"`
}

func (r dataSetDeletedRow) setID() uint64 { return r.SetID_ }
func (r dataSetDeletedRow) key() eventKey { return eventKey{r.BlockNum, r.TxIdx, r.LogIdx} }

func processDataSetDeleted(ctx context.Context, db *gorm.DB) error {
	return processInbox(db,
		"SELECT set_id, block_num, tx_idx, log_idx FROM pdp_dataset_deleted ORDER BY block_num, tx_idx, log_idx",
		"pdp_dataset_deleted",
		func(r dataSetDeletedRow) error {
			return applyDataSetDeleted(ctx, db, r.SetID_)
		},
	)
}

// applyDataSetDeleted marks the proof set deleted and expires its deals.
func applyDataSetDeleted(ctx context.Context, db *gorm.DB, setID uint64) error {
	var rowsAffected int64
	if err := database.DoRetry(ctx, func() error {
		result := db.Model(&model.PDPProofSet{}).Where("set_id = ?", setID).
			Update("deleted", true)
		rowsAffected = result.RowsAffected
		return result.Error
	}); err != nil {
		return err
	}
	if rowsAffected == 0 {
		return errors.Errorf("proof set %d not found, retaining event", setID)
	}

	return database.DoRetry(ctx, func() error {
		return db.Model(&model.Deal{}).Where("proof_set_id = ? AND deal_type = ?",
			setID, model.DealTypePDP).
			Update("state", model.DealExpired).Error
	})
}

type spChangedRow struct {
	BlockNum int64  `gorm:"column:block_num"`
	TxIdx    int    `gorm:"column:tx_idx"`
	LogIdx   int    `gorm:"column:log_idx"`
	SetID_   uint64 `gorm:"column:set_id"`
	NewSP    []byte `gorm:"column:new_sp"`
}

func (r spChangedRow) setID() uint64 { return r.SetID_ }
func (r spChangedRow) key() eventKey { return eventKey{r.BlockNum, r.TxIdx, r.LogIdx} }

func processSPChanged(ctx context.Context, db *gorm.DB) error {
	return processInbox(db,
		"SELECT set_id, new_sp, block_num, tx_idx, log_idx FROM pdp_sp_changed ORDER BY block_num, tx_idx, log_idx",
		"pdp_sp_changed",
		func(r spChangedRow) error {
			return applySPChanged(ctx, db, r.SetID_, r.NewSP)
		},
	)
}

// applySPChanged moves the proof set and its deals to the new storage
// provider and records the transfer handoff state.
func applySPChanged(ctx context.Context, db *gorm.DB, setID uint64, newSP []byte) error {
	newAddr, err := commonToDelegatedAddress(common.BytesToAddress(newSP))
	if err != nil {
		return errors.Wrap(err, "converting SP address")
	}

	var rowsAffected int64
	if err := database.DoRetry(ctx, func() error {
		result := db.Model(&model.PDPProofSet{}).Where("set_id = ?", setID).
			Updates(map[string]any{
				"provider":      newAddr.String(),
				"handoff_state": model.ProofSetTransferred,
			})
		rowsAffected = result.RowsAffected
		return result.Error
	}); err != nil {
		return err
	}
	if rowsAffected == 0 {
		return errors.Errorf("proof set %d not found, retaining event", setID)
	}

	return database.DoRetry(ctx, func() error {
		return db.Model(&model.Deal{}).Where("proof_set_id = ? AND deal_type = ?",
			setID, model.DealTypePDP).
			Update("provider", newAddr.String()).Error
	})
}

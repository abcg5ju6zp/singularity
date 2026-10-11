package pdptracker

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/data-preservation-programs/singularity/model"
	"github.com/ethereum/go-ethereum/common"
	"gorm.io/gorm"
)

// Reorg-aware event pipeline.
//
// Shovel only retains rows for blocks it currently considers canonical: on a
// reorg it deletes fact rows at the forked heights and re-inserts rows for
// the new chain. The processor therefore keeps its OWN durable copy of every
// inbox row (pdp_event_records), applies only blocks that trail the chain tip
// by ConfirmationDepth, records a (height, hash, log position) checkpoint per
// applied block, and snapshots every derived row before it changes. When a
// checkpoint hash stops matching canonical chain, snapshots are restored
// down to the nearest common checkpoint and canonical events replayed.
// Derived visibility is blocked for the duration of the recovery.

const (
	syncStateRowID = uint64(1)
	// maxCheckpointScan bounds the common-checkpoint RPC walk before the
	// tracker gives up and requires an explicit full resync.
	maxCheckpointScan = 10000
	checkpointBatch   = 500
	inboxLockTimeout  = "10s"
)

// inboxTable maps a shovel fact table to the event type stored in the
// processor-owned event ledger.
type inboxTable struct {
	name string
	typ  model.PDPEventType
}

var inboxTables = []inboxTable{
	{"pdp_dataset_created", model.PDPEventDataSetCreated},
	{"pdp_sp_changed", model.PDPEventSPChanged},
	{"pdp_pieces_added", model.PDPEventPiecesAdded},
	{"pdp_pieces_removed", model.PDPEventPiecesRemoved},
	{"pdp_next_proving_period", model.PDPEventNextProvingPeriod},
	{"pdp_possession_proven", model.PDPEventPossessionProven},
	{"pdp_dataset_deleted", model.PDPEventDataSetDeleted},
}

// ErrNoCommonCheckpoint means no stored checkpoint matches the canonical
// chain; the tracker stays blocked until an explicit full resync.
var ErrNoCommonCheckpoint = errors.New("no common checkpoint with canonical chain; full resync required")

// SyncStatus is the queryable recovery state exposed to operators / APIs.
type SyncStatus struct {
	Phase            model.PDPSyncPhase
	Blocked          bool
	CommonNum        *int64
	UndoNext         *int64
	LastAppliedBlock *int64
	Reason           string
}

// GetSyncStatus returns the current sync/recovery state. Blocked is true
// while derived PDP state may temporarily disagree with the canonical chain
// (rollback or replay in progress); callers should not expose derived PDP
// state while blocked.
func GetSyncStatus(ctx context.Context, db *gorm.DB) (SyncStatus, error) {
	var st model.PDPSyncState
	err := db.WithContext(ctx).Where("id = ?", syncStateRowID).First(&st).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return SyncStatus{Phase: model.PDPSyncPhaseSyncing}, nil
	}
	if err != nil {
		return SyncStatus{}, errors.Wrap(err, "loading pdp sync state")
	}

	status := SyncStatus{
		Phase:     defaultSyncPhase(st.Phase),
		Blocked:   st.Phase == model.PDPSyncPhaseRollingBack,
		CommonNum: st.CommonNum,
		UndoNext:  st.UndoNext,
		Reason:    st.Reason,
	}
	var cp model.PDPChainCheckpoint
	if err := db.WithContext(ctx).Order("block_num DESC").First(&cp).Error; err == nil {
		num := cp.BlockNum
		status.LastAppliedBlock = &num
	}
	return status, nil
}

func defaultSyncPhase(p model.PDPSyncPhase) model.PDPSyncPhase {
	if p == "" {
		return model.PDPSyncPhaseSyncing
	}
	return p
}

// runReorgAwareSync is one processing cycle: ingest, detect reorgs against
// the last checkpoint, and apply (or recover) confirmed blocks.
func runReorgAwareSync(ctx context.Context, db *gorm.DB, client *ChainPDPClient, depth uint64) error {
	if db.Dialector.Name() != "postgres" {
		return errors.New("PDP reorg-aware tracking requires PostgreSQL")
	}

	if err := ensureSyncState(ctx, db); err != nil {
		return err
	}

	var st model.PDPSyncState
	if err := db.WithContext(ctx).Where("id = ?", syncStateRowID).First(&st).Error; err != nil {
		return errors.Wrap(err, "loading pdp sync state")
	}
	if defaultSyncPhase(st.Phase) == model.PDPSyncPhaseRollingBack {
		return continueRecovery(ctx, db, client, depth, &st)
	}

	headNum, _, err := client.BlockHeader(ctx, -1)
	if err != nil {
		return errors.Wrap(err, "fetching chain tip")
	}
	if err := ingestInboxEvents(ctx, db, client); err != nil {
		return err
	}

	cp, err := latestCheckpoint(ctx, db)
	if err != nil {
		return err
	}
	if cp != nil {
		_, hash, err := client.BlockHeader(ctx, cp.BlockNum)
		if err != nil {
			return errors.Wrapf(err, "fetching canonical hash for block %d", cp.BlockNum)
		}
		if !bytes.Equal(hash.Bytes(), cp.BlockHash) {
			Logger.Warnw("reorg detected at applied checkpoint",
				"block", cp.BlockNum,
				"appliedHash", common.BytesToHash(cp.BlockHash).Hex(),
				"canonicalHash", hash.Hex())
			common, err := findCommonCheckpoint(ctx, db, client, cp.BlockNum)
			if err != nil {
				_ = markUnrecoverable(ctx, db, err)
				return err
			}
			if err := beginRollback(ctx, db, client, cp.BlockNum, common); err != nil {
				return err
			}
			return runReorgAwareSync(ctx, db, client, depth)
		}
	}

	confirmedNum := confirmedHead(headNum, depth)
	if confirmedNum < 0 {
		return nil
	}
	start, err := nextApplyStart(ctx, db, cp)
	if err != nil {
		return err
	}
	if start > confirmedNum {
		return nil
	}
	_, err = applyForward(ctx, db, client, start, confirmedNum)
	return err
}

func confirmedHead(headNum uint64, depth uint64) int64 {
	if uint64(headNum) <= depth {
		return -1
	}
	return int64(headNum - depth)
}

func ensureSyncState(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var count int64
		if err := tx.Model(&model.PDPSyncState{}).Where("id = ?", syncStateRowID).Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return nil
		}
		return tx.Create(&model.PDPSyncState{
			ID:        syncStateRowID,
			Phase:     model.PDPSyncPhaseSyncing,
			UpdatedAt: time.Now(),
		}).Error
	})
}

func latestCheckpoint(ctx context.Context, db *gorm.DB) (*model.PDPChainCheckpoint, error) {
	var cp model.PDPChainCheckpoint
	err := db.WithContext(ctx).Order("block_num DESC").First(&cp).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "loading latest checkpoint")
	}
	return &cp, nil
}

// nextApplyStart returns the first block to apply: one past the latest
// checkpoint, or the oldest pending block when no checkpoint exists yet.
func nextApplyStart(ctx context.Context, db *gorm.DB, cp *model.PDPChainCheckpoint) (int64, error) {
	if cp != nil {
		return cp.BlockNum + 1, nil
	}
	var minBlock *int64
	if err := db.WithContext(ctx).Model(&model.PDPEventRecord{}).
		Where("status = ?", model.PDPEventPending).
		Select("MIN(block_num)").Scan(&minBlock).Error; err != nil {
		return 0, errors.Wrap(err, "finding first pending event")
	}
	if minBlock == nil {
		// nothing pending and no checkpoint: nothing to start from; return
		// a value above every head so the caller no-ops.
		return 1 << 62, nil
	}
	return *minBlock, nil
}

// ---------------------------------------------------------------------------
// Inbox ingestion
// ---------------------------------------------------------------------------

// ingestInboxEvents copies every shovel inbox row into the durable event
// ledger and removes each copied row from the inbox in the same transaction.
// Shovel writes are blocked (SHARE table lock) for the duration so a row can
// never be deleted without first landing in the ledger. Rows whose block
// hash is missing (indexed before the upgrade) are backfilled via RPC.
func ingestInboxEvents(ctx context.Context, db *gorm.DB, client *ChainPDPClient) error {
	numSet := make(map[int64]struct{})
	for _, it := range inboxTables {
		nums, err := inboxNullHashBlocks(ctx, db, it.name)
		if err != nil {
			return err
		}
		for _, n := range nums {
			numSet[n] = struct{}{}
		}
	}

	hashByNum := make(map[int64][]byte, len(numSet))
	for n := range numSet {
		_, hash, err := client.BlockHeader(ctx, n)
		if err != nil {
			return errors.Wrapf(err, "backfilling block hash for block %d", n)
		}
		hashByNum[n] = hash.Bytes()
	}

	for _, it := range inboxTables {
		if err := ingestTable(ctx, db, it, hashByNum); err != nil {
			return errors.Wrapf(err, "ingesting %s", it.name)
		}
	}
	return nil
}

func inboxNullHashBlocks(ctx context.Context, db *gorm.DB, table string) ([]int64, error) {
	var nums []int64
	err := db.WithContext(ctx).Raw(
		"SELECT DISTINCT block_num FROM " + table + " WHERE block_hash IS NULL ORDER BY block_num",
	).Scan(&nums).Error
	return nums, err
}

func ingestTable(ctx context.Context, db *gorm.DB, it inboxTable, hashByNum map[int64][]byte) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SET LOCAL lock_timeout = '" + inboxLockTimeout + "'").Error; err != nil {
			return err
		}
		if err := tx.Exec("LOCK TABLE " + it.name + " IN SHARE MODE").Error; err != nil {
			return errors.Wrap(err, "acquiring inbox table lock")
		}

		for n, h := range hashByNum {
			if err := tx.Exec(
				"UPDATE "+it.name+" SET block_hash = ? WHERE block_num = ? AND block_hash IS NULL",
				h, n,
			).Error; err != nil {
				return errors.Wrapf(err, "backfilling block hash at %d", n)
			}
		}

		var unresolved int64
		if err := tx.Raw(
			"SELECT COUNT(*) FROM " + it.name + " WHERE block_hash IS NULL",
		).Scan(&unresolved).Error; err != nil {
			return err
		}
		if unresolved > 0 {
			// rows from blocks we couldn't hash yet; leave everything in
			// place and retry next cycle.
			return errors.Errorf("%d rows in %s awaiting block hash backfill", unresolved, it.name)
		}

		if err := tx.Exec(ingestInsertSQL(it.name, it.typ)).Error; err != nil {
			return errors.Wrap(err, "copying inbox rows into event ledger")
		}
		return tx.Exec("DELETE FROM " + it.name + " WHERE block_hash IS NOT NULL").Error
	})
}

// ingestInsertSQL returns the per-table INSERT...SELECT. Table and event
// names are static literals from inboxTables; no user input is interpolated.
func ingestInsertSQL(table string, typ model.PDPEventType) string {
	conflict := `
		ON CONFLICT (event_type, block_num, block_hash, tx_idx, log_idx) DO NOTHING`
	prefix := `
		INSERT INTO pdp_event_records (
			event_type, block_num, block_hash, tx_idx, log_idx, set_id,
			storage_provider, new_sp, challenge_epoch, leaf_count, deleted_leaf_count,
			status, created_at
		)
		SELECT '` + string(typ) + `', block_num, block_hash, tx_idx, log_idx, set_id, `

	var selectCols string
	switch typ {
	case model.PDPEventDataSetCreated:
		selectCols = `storage_provider, NULL, NULL, NULL, NULL,`
	case model.PDPEventSPChanged:
		selectCols = `NULL, new_sp, NULL, NULL, NULL,`
	case model.PDPEventNextProvingPeriod:
		selectCols = `NULL, NULL, challenge_epoch, leaf_count, NULL,`
	case model.PDPEventDataSetDeleted:
		selectCols = `NULL, NULL, NULL, NULL, deleted_leaf_count,`
	default:
		selectCols = `NULL, NULL, NULL, NULL, NULL,`
	}

	return prefix + selectCols + `
			'` + string(model.PDPEventPending) + `', NOW()
		FROM ` + table + conflict
}

// ---------------------------------------------------------------------------
// Reorg detection and rollback bookkeeping
// ---------------------------------------------------------------------------

// findCommonCheckpoint walks stored checkpoints newest-first and returns the
// first whose hash still matches the canonical chain.
func findCommonCheckpoint(ctx context.Context, db *gorm.DB, client *ChainPDPClient, fromNum int64) (*model.PDPChainCheckpoint, error) {
	cursor := fromNum
	for scanned := 0; scanned < maxCheckpointScan; {
		var batch []model.PDPChainCheckpoint
		err := db.WithContext(ctx).
			Where("block_num <= ?", cursor).
			Order("block_num DESC").Limit(checkpointBatch).
			Find(&batch).Error
		if err != nil {
			return nil, errors.Wrap(err, "scanning checkpoints")
		}
		if len(batch) == 0 {
			return nil, ErrNoCommonCheckpoint
		}
		for i := range batch {
			cp := batch[i]
			_, hash, err := client.BlockHeader(ctx, cp.BlockNum)
			if err != nil {
				return nil, errors.Wrapf(err, "fetching canonical hash for block %d", cp.BlockNum)
			}
			if bytes.Equal(hash.Bytes(), cp.BlockHash) {
				return &cp, nil
			}
		}
		scanned += len(batch)
		if len(batch) < checkpointBatch {
			return nil, ErrNoCommonCheckpoint
		}
		cursor = batch[len(batch)-1].BlockNum - 1
	}
	return nil, ErrNoCommonCheckpoint
}

// beginRollback persists the recovery marker: blocks above the common
// checkpoint are scheduled for undo, their old-fork events marked forked.
func beginRollback(ctx context.Context, db *gorm.DB, client *ChainPDPClient, tipNum int64, common *model.PDPChainCheckpoint) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// canonicalize every record between common and the old tip so
		// replay only applies new-chain rows.
		for n := common.BlockNum + 1; n <= tipNum; n++ {
			_, hash, err := client.BlockHeader(ctx, n)
			if err != nil {
				return errors.Wrapf(err, "fetching canonical hash during rollback start at %d", n)
			}
			if err := tx.Exec(
				"UPDATE pdp_event_records SET status = ? WHERE block_num = ? AND block_hash <> ? AND status <> ?",
				model.PDPEventForked, n, hash.Bytes(), model.PDPEventForked,
			).Error; err != nil {
				return errors.Wrap(err, "marking forked events")
			}
		}

		commonNum := common.BlockNum
		undoNext := tipNum
		return tx.Exec(`
			INSERT INTO pdp_sync_state (id, phase, common_num, common_hash, undo_next, reason, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, NOW())
			ON CONFLICT (id) DO UPDATE SET
				phase = EXCLUDED.phase,
				common_num = EXCLUDED.common_num,
				common_hash = EXCLUDED.common_hash,
				undo_next = EXCLUDED.undo_next,
				reason = EXCLUDED.reason,
				updated_at = EXCLUDED.updated_at`,
			syncStateRowID, model.PDPSyncPhaseRollingBack, commonNum, common.BlockHash,
			undoNext, "chain reorg: rolling back to nearest common checkpoint",
		).Error
	})
}

func markUnrecoverable(ctx context.Context, db *gorm.DB, cause error) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Exec(
			`INSERT INTO pdp_sync_state (id, phase, reason, updated_at)
			 VALUES (?, ?, ?, NOW())
			 ON CONFLICT (id) DO UPDATE SET phase = EXCLUDED.phase, reason = EXCLUDED.reason, updated_at = EXCLUDED.updated_at`,
			syncStateRowID, model.PDPSyncPhaseRollingBack, cause.Error(),
		).Error
	})
}

// continueRecovery resumes an in-progress rollback after a restart or a
// failed cycle. Undo needs no RPC and always makes progress first; replay
// proceeds as canonical events become available. The state stays blocked
// until replay reaches the confirmed chain tip.
func continueRecovery(ctx context.Context, db *gorm.DB, client *ChainPDPClient, depth uint64, st *model.PDPSyncState) error {
	if st.CommonNum == nil {
		return ErrNoCommonCheckpoint
	}

	// resume undo (crash-safe: each block is one transaction with the
	// cursor advanced inside it)
	for st.UndoNext != nil && *st.UndoNext > *st.CommonNum {
		b := *st.UndoNext
		if err := undoBlock(ctx, db, b); err != nil {
			return errors.Wrapf(err, "undoing block %d", b)
		}
		next := b - 1
		st.UndoNext = &next
		Logger.Infow("reorg undo progressed", "block", b, "undoNext", next)
	}

	headNum, _, err := client.BlockHeader(ctx, -1)
	if err != nil {
		return errors.Wrap(err, "fetching chain tip during recovery")
	}
	// pull canonical replacement events shovel indexed for the new fork
	if err := ingestInboxEvents(ctx, db, client); err != nil {
		return err
	}
	confirmedNum := confirmedHead(headNum, depth)
	if confirmedNum < *st.CommonNum {
		// chain below the common point cannot normally happen; wait
		return nil
	}

	reached, err := applyForward(ctx, db, client, *st.CommonNum+1, confirmedNum)
	if err != nil {
		return errors.Wrap(err, "replaying canonical chain")
	}
	if reached < confirmedNum {
		// canonical blocks/events not indexed yet; stay blocked and retry
		Logger.Infow("reorg replay waiting for canonical index", "replayedTo", reached, "confirmedTo", confirmedNum)
		return nil
	}

	return finishRecovery(ctx, db)
}

func finishRecovery(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Exec(
			`UPDATE pdp_sync_state
			 SET phase = ?, common_num = NULL, common_hash = NULL, undo_next = NULL, reason = '', updated_at = NOW()
			 WHERE id = ?`,
			model.PDPSyncPhaseSyncing, syncStateRowID,
		).Error
	})
}

// undoBlock restores every before-image captured for block b, removes the
// block's checkpoint and snapshots, and records progress, all atomically.
func undoBlock(ctx context.Context, db *gorm.DB, b int64) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var snaps []model.PDPStateSnapshot
		if err := tx.Where("block_num = ?", b).
			Order("CASE entity WHEN 'deal' THEN 0 WHEN 'proofset' THEN 1 END").
			Find(&snaps).Error; err != nil {
			return errors.Wrap(err, "loading snapshots")
		}
		for i := range snaps {
			if err := restoreSnapshot(tx, &snaps[i]); err != nil {
				return errors.Wrapf(err, "restoring %s %d", snaps[i].Entity, snaps[i].EntityID)
			}
		}
		if err := tx.Where("block_num = ?", b).Delete(&model.PDPStateSnapshot{}).Error; err != nil {
			return err
		}
		if err := tx.Where("block_num = ?", b).Delete(&model.PDPChainCheckpoint{}).Error; err != nil {
			return err
		}
		return tx.Exec(
			"UPDATE pdp_sync_state SET undo_next = ?, updated_at = NOW() WHERE id = ?",
			b-1, syncStateRowID,
		).Error
	})
}

func restoreSnapshot(tx *gorm.DB, snap *model.PDPStateSnapshot) error {
	if !snap.Existed {
		switch snap.Entity {
		case model.PDPSnapshotDeal:
			return tx.Delete(&model.Deal{}, "id = ?", snap.EntityID).Error
		case model.PDPSnapshotProofSet:
			return tx.Delete(&model.PDPProofSet{}, "set_id = ?", snap.EntityID).Error
		}
		return nil
	}

	switch snap.Entity {
	case model.PDPSnapshotDeal:
		var deal model.Deal
		if err := json.Unmarshal(snap.Data, &deal); err != nil {
			return errors.Wrap(err, "decoding deal snapshot")
		}
		return tx.Save(&deal).Error
	case model.PDPSnapshotProofSet:
		var ps model.PDPProofSet
		if err := json.Unmarshal(snap.Data, &ps); err != nil {
			return errors.Wrap(err, "decoding proof set snapshot")
		}
		return tx.Save(&ps).Error
	}
	return nil
}

// ---------------------------------------------------------------------------
// Forward application (shared by normal sync and replay)
// ---------------------------------------------------------------------------

// applyForward applies canonical blocks from start through confirmedNum.
// Blocks are applied one transaction at a time. It returns the highest block
// fully applied; if shovel has not indexed an eventless block yet, it stops
// early (returns reached < confirmedNum) and the caller retries.
func applyForward(ctx context.Context, db *gorm.DB, client *ChainPDPClient, start, confirmedNum int64) (int64, error) {
	reached := start - 1
	for b := start; b <= confirmedNum; b++ {
		_, hash, err := client.BlockHeader(ctx, b)
		if err != nil {
			return reached, errors.Wrapf(err, "fetching canonical block %d", b)
		}

		var pending []model.PDPEventRecord
		if err := db.WithContext(ctx).
			Where("block_num = ? AND status = ?", b, model.PDPEventPending).
			Order("tx_idx, log_idx, id").Find(&pending).Error; err != nil {
			return reached, errors.Wrap(err, "loading pending events")
		}

		var canonical []model.PDPEventRecord
		for _, r := range pending {
			if bytes.Equal(r.BlockHash, hash.Bytes()) {
				canonical = append(canonical, r)
			} else {
				// unconfirmed event from an abandoned fork; never applied
				if err := db.WithContext(ctx).Model(&model.PDPEventRecord{}).
					Where("id = ?", r.ID).Update("status", model.PDPEventForked).Error; err != nil {
					return reached, errors.Wrap(err, "marking unconfirmed forked event")
				}
			}
		}

		if len(canonical) == 0 {
			ready, err := shovelIndexedThrough(ctx, db, b)
			if err != nil {
				return reached, err
			}
			if !ready {
				return reached, nil
			}
			if err := writeCheckpoint(ctx, db, b, hash.Bytes(), 0, 0); err != nil {
				return reached, err
			}
			reached = b
			continue
		}

		if err := applyBlock(ctx, db, client, b, hash.Bytes(), canonical); err != nil {
			return reached, err
		}
		reached = b
	}
	return reached, nil
}

// shovelIndexedThrough reports whether every integration has indexed block b
// (so a missing event at b means "no matching logs" rather than "not caught
// up yet").
func shovelIndexedThrough(ctx context.Context, db *gorm.DB, b int64) (bool, error) {
	var row struct {
		IgCount int
		MinNum  *int64
	}
	err := db.WithContext(ctx).Raw(`
		SELECT COUNT(*) AS ig_count, MIN(max_num) AS min_num FROM (
			SELECT ig_name, MAX(num) AS max_num
			FROM shovel.task_updates
			WHERE src_name = ?
			GROUP BY ig_name
		) s`, srcName).Scan(&row).Error
	if err != nil {
		return false, errors.Wrap(err, "querying shovel index cursor")
	}
	if row.IgCount < len(inboxTables) {
		return false, nil
	}
	return row.MinNum != nil && *row.MinNum >= b, nil
}

// applyBlock snapshots affected rows, applies one block's events in
// dependency order, records new-row markers and the checkpoint — atomically.
func applyBlock(ctx context.Context, db *gorm.DB, client *ChainPDPClient, b int64, hash []byte, records []model.PDPEventRecord) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		setIDs := uniqueSetIDs(records)
		if err := snapshotBefore(tx, b, setIDs); err != nil {
			return err
		}

		now := time.Now()
		var pieceSetIDs map[uint64]struct{}

		for _, r := range records {
			switch r.EventType {
			case model.PDPEventDataSetCreated:
				if err := applyDataSetCreated(ctx, tx, client, r.SetID, b, r.StorageProvider); err != nil {
					return err
				}
			case model.PDPEventSPChanged:
				if err := applySPChanged(ctx, tx, r.SetID, r.NewSP); err != nil {
					return err
				}
			case model.PDPEventPiecesAdded, model.PDPEventPiecesRemoved:
				if pieceSetIDs == nil {
					pieceSetIDs = make(map[uint64]struct{})
				}
				pieceSetIDs[r.SetID] = struct{}{}
			case model.PDPEventNextProvingPeriod:
				epoch := int64(0)
				if r.ChallengeEpoch != nil {
					epoch = *r.ChallengeEpoch
				}
				if err := applyNextProvingPeriod(ctx, tx, r.SetID, epoch); err != nil {
					return err
				}
			case model.PDPEventPossessionProven:
				if err := applyPossessionProven(ctx, tx, r.SetID, now); err != nil {
					return err
				}
			case model.PDPEventDataSetDeleted:
				if err := applyDataSetDeleted(ctx, tx, r.SetID); err != nil {
					return err
				}
			}
		}

		// reconcile pieces once per affected set, after all other block
		// effects for the set are in place
		for setID := range pieceSetIDs {
			if err := reconcileProofSetPieces(ctx, tx, client, setID); err != nil {
				return err
			}
		}

		if err := snapshotAfter(tx, b, setIDs); err != nil {
			return err
		}

		ids := make([]uint64, len(records))
		var lastTx, lastLog int
		for i, r := range records {
			ids[i] = r.ID
			if r.TxIdx > lastTx || (r.TxIdx == lastTx && r.LogIdx > lastLog) {
				lastTx, lastLog = r.TxIdx, r.LogIdx
			}
		}
		if err := tx.Model(&model.PDPEventRecord{}).Where("id IN ?", ids).
			Updates(map[string]any{"status": model.PDPEventApplied, "applied_at": now}).Error; err != nil {
			return err
		}
		return upsertCheckpoint(tx, b, hash, lastTx, lastLog, now)
	})
}

func writeCheckpoint(ctx context.Context, db *gorm.DB, b int64, hash []byte, txIdx, logIdx int) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return upsertCheckpoint(tx, b, hash, txIdx, logIdx, time.Now())
	})
}

func upsertCheckpoint(tx *gorm.DB, b int64, hash []byte, txIdx, logIdx int, at time.Time) error {
	return tx.Exec(`
		INSERT INTO pdp_chain_checkpoints (block_num, block_hash, last_tx_idx, last_log_idx, applied_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (block_num) DO UPDATE
		SET block_hash = EXCLUDED.block_hash,
		    last_tx_idx = EXCLUDED.last_tx_idx,
		    last_log_idx = EXCLUDED.last_log_idx,
		    applied_at = EXCLUDED.applied_at`,
		b, hash, txIdx, logIdx, at).Error
}

func uniqueSetIDs(records []model.PDPEventRecord) []uint64 {
	seen := make(map[uint64]struct{})
	out := make([]uint64, 0, len(records))
	for _, r := range records {
		if _, ok := seen[r.SetID]; ok {
			continue
		}
		seen[r.SetID] = struct{}{}
		out = append(out, r.SetID)
	}
	return out
}

// snapshotBefore captures proof-set rows and their PDP deals before a block
// changes them.
func snapshotBefore(tx *gorm.DB, b int64, setIDs []uint64) error {
	now := time.Now()
	for _, setID := range setIDs {
		var ps model.PDPProofSet
		err := tx.Where("set_id = ?", setID).First(&ps).Error
		switch {
		case err == nil:
			data, err := json.Marshal(&ps)
			if err != nil {
				return errors.Wrap(err, "encoding proof set snapshot")
			}
			if err := createSnapshot(tx, b, model.PDPSnapshotProofSet, setID, true, data, now); err != nil {
				return err
			}
		case errors.Is(err, gorm.ErrRecordNotFound):
			if err := createSnapshot(tx, b, model.PDPSnapshotProofSet, setID, false, nil, now); err != nil {
				return err
			}
		default:
			return err
		}

		var deals []model.Deal
		if err := tx.Where("proof_set_id = ? AND deal_type = ?", setID, model.DealTypePDP).
			Find(&deals).Error; err != nil {
			return err
		}
		for i := range deals {
			data, err := json.Marshal(&deals[i])
			if err != nil {
				return errors.Wrap(err, "encoding deal snapshot")
			}
			if err := createSnapshot(tx, b, model.PDPSnapshotDeal, uint64(deals[i].ID), true, data, now); err != nil {
				return err
			}
		}
	}
	return nil
}

// snapshotAfter records absence markers for rows the block itself created
// (proof sets or deals) so rollback deletes them.
func snapshotAfter(tx *gorm.DB, b int64, setIDs []uint64) error {
	if len(setIDs) == 0 {
		return nil
	}
	now := time.Now()

	var knownSets []struct {
		EntityID uint64
	}
	if err := tx.Model(&model.PDPStateSnapshot{}).
		Select("entity_id").
		Where("block_num = ? AND entity = ?", b, model.PDPSnapshotProofSet).
		Find(&knownSets).Error; err != nil {
		return err
	}
	existingSets := make(map[uint64]struct{}, len(knownSets))
	for _, k := range knownSets {
		existingSets[k.EntityID] = struct{}{}
	}
	for _, setID := range setIDs {
		if _, ok := existingSets[setID]; ok {
			continue
		}
		var count int64
		if err := tx.Model(&model.PDPProofSet{}).Where("set_id = ?", setID).Count(&count).Error; err != nil {
			return err
		}
		if count == 0 {
			if err := createSnapshot(tx, b, model.PDPSnapshotProofSet, setID, false, nil, now); err != nil {
				return err
			}
		}
	}

	var newDeals []model.Deal
	if err := tx.Where("proof_set_id IN ? AND deal_type = ?", setIDs, model.DealTypePDP).
		Find(&newDeals).Error; err != nil {
		return err
	}
	for i := range newDeals {
		var count int64
		if err := tx.Model(&model.PDPStateSnapshot{}).
			Where("block_num = ? AND entity = ? AND entity_id = ?",
				b, model.PDPSnapshotDeal, uint64(newDeals[i].ID)).
			Count(&count).Error; err != nil {
			return err
		}
		if count == 0 {
			if err := createSnapshot(tx, b, model.PDPSnapshotDeal, uint64(newDeals[i].ID), false, nil, now); err != nil {
				return err
			}
		}
	}
	return nil
}

func createSnapshot(tx *gorm.DB, b int64, entity model.PDPSnapshotEntity, id uint64, existed bool, data []byte, at time.Time) error {
	return tx.Exec(`
		INSERT INTO pdp_state_snapshots (block_num, entity, entity_id, existed, data, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (block_num, entity, entity_id) DO NOTHING`,
		b, string(entity), id, existed, data, at).Error
}

// ResetProcessorState clears reorg bookkeeping (used by explicit full
// resync). Derived proof sets and deals are deliberately left untouched.
func ResetProcessorState(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("TRUNCATE TABLE pdp_event_records").Error; err != nil {
			return err
		}
		if err := tx.Exec("TRUNCATE TABLE pdp_chain_checkpoints").Error; err != nil {
			return err
		}
		if err := tx.Exec("TRUNCATE TABLE pdp_state_snapshots").Error; err != nil {
			return err
		}
		return tx.Exec("TRUNCATE TABLE pdp_sync_state").Error
	})
}

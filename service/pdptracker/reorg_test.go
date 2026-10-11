package pdptracker

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/data-preservation-programs/singularity/model"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// mockHeaderChain is a deterministic in-memory chain of block headers.
// reorg() replaces block hashes at/after a height, exactly like a short
// chain reorganization.
type mockHeaderChain struct {
	mu     sync.Mutex
	head   int64
	epoch  int64
	blocks map[int64]*types.Header
}

func newMockHeaderChain(head int64) *mockHeaderChain {
	c := &mockHeaderChain{head: head, epoch: 1, blocks: make(map[int64]*types.Header)}
	c.build(0, head)
	return c
}

func (c *mockHeaderChain) build(from, to int64) {
	for n := from; n <= to; n++ {
		tag := crypto.Keccak256([]byte(fmt.Sprintf("epoch-%d-block-%d", c.epoch, n)))
		c.blocks[n] = &types.Header{
			Number: big.NewInt(n),
			Extra:  tag,
			Time:   uint64(c.epoch*1_000_000 + n),
		}
	}
}

func (c *mockHeaderChain) HeaderByNumber(_ context.Context, number *big.Int) (*types.Header, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.head
	if number != nil {
		n = number.Int64()
	}
	h, ok := c.blocks[n]
	if !ok {
		return nil, fmt.Errorf("block %d not found", n)
	}
	cp := *h
	return &cp, nil
}

func (c *mockHeaderChain) HashAt(n int64) common.Hash {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.blocks[n].Hash()
}

func (c *mockHeaderChain) Head() int64 { return c.head }

// reorg replaces every block at/after forkAt with a new canonical version.
func (c *mockHeaderChain) reorg(forkAt int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.epoch++
	for n := forkAt; n <= c.head; n++ {
		tag := crypto.Keccak256([]byte(fmt.Sprintf("epoch-%d-block-%d", c.epoch, n)))
		c.blocks[n] = &types.Header{
			Number: big.NewInt(n),
			Extra:  tag,
			Time:   uint64(c.epoch*1_000_000 + n),
		}
	}
}

func (c *mockHeaderChain) advance(blocks int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.build(c.head+1, c.head+blocks)
	c.head += blocks
}

type errorHeaderSource struct{}

func (errorHeaderSource) HeaderByNumber(context.Context, *big.Int) (*types.Header, error) {
	return nil, fmt.Errorf("rpc unavailable")
}

type reorgEnv struct {
	pgTestEnv
	chain  *mockHeaderChain
	client *ChainPDPClient
}

func reorgTest(t *testing.T, fn func(t *testing.T, e reorgEnv)) {
	t.Helper()
	pgTest(t, func(t *testing.T, pe pgTestEnv) {
		// minimal shovel cursor table used by the indexer gate
		require.NoError(t, pe.db.Exec(`
			CREATE SCHEMA IF NOT EXISTS shovel;
			CREATE TABLE IF NOT EXISTS shovel.task_updates (
				num numeric, hash bytea, src_name text, ig_name text
			);
		`).Error)

		chain := newMockHeaderChain(410)
		client := &ChainPDPClient{contract: pe.mock, pageSize: 100, headers: chain}
		fn(t, reorgEnv{pgTestEnv: pe, chain: chain, client: client})
	})
}

func (e reorgEnv) run(t *testing.T, depth uint64) error {
	t.Helper()
	return runReorgAwareSync(e.ctx, e.db, e.client, depth)
}

func (e reorgEnv) setShovelCursor(t *testing.T, upTo int64) {
	t.Helper()
	require.NoError(t, e.db.Exec("DELETE FROM shovel.task_updates WHERE src_name = ?", srcName).Error)
	for _, ig := range inboxTables {
		require.NoError(t, e.db.Exec(
			"INSERT INTO shovel.task_updates (num, hash, src_name, ig_name) VALUES (?, ?, ?, ?)",
			upTo, e.chain.HashAt(upTo).Bytes(), srcName, ig.name,
		).Error)
	}
}

func (e reorgEnv) insertDataSetCreated(t *testing.T, block int64, setID uint64, provider common.Address) {
	t.Helper()
	require.NoError(t, e.db.Exec(
		"INSERT INTO pdp_dataset_created (set_id, storage_provider, block_num, block_hash, tx_idx, log_idx) VALUES (?, ?, ?, ?, ?, ?)",
		setID, provider.Bytes(), block, e.chain.HashAt(block).Bytes(), 0, 0,
	).Error)
}

func (e reorgEnv) insertPieces(t *testing.T, table string, block int64, setID uint64) {
	t.Helper()
	require.NoError(t, e.db.Exec(
		"INSERT INTO "+table+" (set_id, block_num, block_hash, tx_idx, log_idx) VALUES (?, ?, ?, ?, ?)",
		setID, block, e.chain.HashAt(block).Bytes(), 0, 0,
	).Error)
}

func (e reorgEnv) insertSPChanged(t *testing.T, block int64, setID uint64, newSP common.Address) {
	t.Helper()
	require.NoError(t, e.db.Exec(
		"INSERT INTO pdp_sp_changed (set_id, old_sp, new_sp, block_num, block_hash, tx_idx, log_idx) VALUES (?, ?, ?, ?, ?, ?, ?)",
		setID, e.providerEth.Bytes(), newSP.Bytes(), block, e.chain.HashAt(block).Bytes(), 0, 0,
	).Error)
}

func (e reorgEnv) insertProvingPeriod(t *testing.T, block int64, setID uint64, epoch int64) {
	t.Helper()
	require.NoError(t, e.db.Exec(
		"INSERT INTO pdp_next_proving_period (set_id, challenge_epoch, leaf_count, block_num, block_hash, tx_idx, log_idx) VALUES (?, ?, ?, ?, ?, ?, ?)",
		setID, epoch, 1, block, e.chain.HashAt(block).Bytes(), 0, 0,
	).Error)
}

func (e reorgEnv) insertPossessionProven(t *testing.T, block int64, setID uint64) {
	t.Helper()
	require.NoError(t, e.db.Exec(
		"INSERT INTO pdp_possession_proven (set_id, block_num, block_hash, tx_idx, log_idx) VALUES (?, ?, ?, ?, ?)",
		setID, block, e.chain.HashAt(block).Bytes(), 0, 0,
	).Error)
}

func (e reorgEnv) insertDataSetDeleted(t *testing.T, block int64, setID uint64) {
	t.Helper()
	require.NoError(t, e.db.Exec(
		"INSERT INTO pdp_dataset_deleted (set_id, deleted_leaf_count, block_num, block_hash, tx_idx, log_idx) VALUES (?, ?, ?, ?, ?, ?)",
		setID, 1, block, e.chain.HashAt(block).Bytes(), 0, 0,
	).Error)
}

func (e reorgEnv) recordCount(t *testing.T, status model.PDPEventStatus, typ model.PDPEventType) int64 {
	t.Helper()
	var n int64
	q := e.db.Model(&model.PDPEventRecord{})
	if status != "" {
		q = q.Where("status = ?", status)
	}
	if typ != "" {
		q = q.Where("event_type = ?", typ)
	}
	require.NoError(t, q.Count(&n).Error)
	return n
}

func (e reorgEnv) latestCheckpoint(t *testing.T) model.PDPChainCheckpoint {
	t.Helper()
	var cp model.PDPChainCheckpoint
	require.NoError(t, e.db.Order("block_num DESC").First(&cp).Error)
	return cp
}

func (e reorgEnv) proofSet(t *testing.T, setID uint64) model.PDPProofSet {
	t.Helper()
	var ps model.PDPProofSet
	require.NoError(t, e.db.Where("set_id = ?", setID).First(&ps).Error)
	return ps
}

func (e reorgEnv) pdpDeal(t *testing.T, setID uint64) model.Deal {
	t.Helper()
	var d model.Deal
	require.NoError(t, e.db.Where("proof_set_id = ? AND deal_type = ?", setID, model.DealTypePDP).First(&d).Error)
	return d
}

// seedOldFork builds the applied state used by rollback tests:
// 100 DataSetCreated, 200 PiecesAdded, 250 SPChanged, 280 proving period,
// 300 PossessionProven, 350 DataSetDeleted — all applied to confirmed depth.
func (e reorgEnv) seedOldFork(t *testing.T) {
	t.Helper()
	require.NoError(t, e.db.Create(&model.Actor{
		ID: "f0100", Address: e.listenerFil.String(),
	}).Error)
	newSP := common.HexToAddress("0x3333333333333333333333333333333333333333")

	e.insertDataSetCreated(t, 100, 1, e.providerEth)
	e.insertPieces(t, "pdp_pieces_added", 200, 1)
	e.insertSPChanged(t, 250, 1, newSP)
	e.insertProvingPeriod(t, 280, 1, 500)
	e.insertPossessionProven(t, 300, 1)
	e.insertDataSetDeleted(t, 350, 1)
	e.setShovelCursor(t, 410)

	require.NoError(t, e.run(t, 10))

	ps := e.proofSet(t, 1)
	require.True(t, ps.Deleted)
	require.True(t, ps.IsLive)
	require.EqualValues(t, 500, *ps.ChallengeEpoch)
	expectedSP, _ := commonToDelegatedAddress(newSP)
	require.Equal(t, expectedSP.String(), ps.Provider)
	require.Equal(t, model.DealExpired, e.pdpDeal(t, 1).State)
	require.Equal(t, int64(400), e.latestCheckpoint(t).BlockNum)
}

func TestReorg_ConfirmationDepthGating(t *testing.T) {
	reorgTest(t, func(t *testing.T, e reorgEnv) {
		require.NoError(t, e.db.Create(&model.Actor{
			ID: "f0100", Address: e.listenerFil.String(),
		}).Error)
		// event trails the tip by only 5 blocks: below the depth of 10
		e.insertDataSetCreated(t, 405, 1, e.providerEth)
		e.setShovelCursor(t, 410)

		// head 410, depth 10 → only blocks <= 400 become visible
		require.NoError(t, e.run(t, 10))
		var count int64
		require.NoError(t, e.db.Model(&model.PDPProofSet{}).Count(&count).Error)
		require.EqualValues(t, 0, count, "unconfirmed event must not be visible")
		require.EqualValues(t, 1, e.recordCount(t, model.PDPEventPending, model.PDPEventDataSetCreated))
		var cpCount int64
		require.NoError(t, e.db.Model(&model.PDPChainCheckpoint{}).Count(&cpCount).Error)
		require.EqualValues(t, 0, cpCount, "no checkpoint may pass the unconfirmed block")

		// advance chain past confirmation depth
		e.chain.advance(10)
		e.setShovelCursor(t, 420)
		require.NoError(t, e.run(t, 10))
		require.NoError(t, e.db.Model(&model.PDPProofSet{}).Count(&count).Error)
		require.EqualValues(t, 1, count)
		require.EqualValues(t, 0, e.recordCount(t, model.PDPEventPending, model.PDPEventDataSetCreated))

		cp := e.latestCheckpoint(t)
		require.Equal(t, int64(410), cp.BlockNum)
		require.Equal(t, e.chain.HashAt(410).Bytes(), cp.BlockHash)
	})
}

func TestReorg_CheckpointsAndIdempotentRedelivery(t *testing.T) {
	reorgTest(t, func(t *testing.T, e reorgEnv) {
		require.NoError(t, e.db.Create(&model.Actor{
			ID: "f0100", Address: e.listenerFil.String(),
		}).Error)
		e.insertDataSetCreated(t, 100, 1, e.providerEth)
		e.insertPieces(t, "pdp_pieces_added", 200, 1)
		e.setShovelCursor(t, 410)
		require.NoError(t, e.run(t, 10))

		var dealCount int64
		require.NoError(t, e.db.Model(&model.Deal{}).Where("deal_type = ?", model.DealTypePDP).Count(&dealCount).Error)
		require.EqualValues(t, 1, dealCount)

		// checkpoint carries height+hash+log position
		cp100, ok := e.checkpoint(t, 100)
		require.True(t, ok)
		require.Equal(t, e.chain.HashAt(100).Bytes(), cp100.BlockHash)
		require.Equal(t, 0, cp100.LastTxIdx)
		require.Equal(t, 0, cp100.LastLogIdx)

		// shovel redelivers the exact same log (same height+hash+position)
		e.insertDataSetCreated(t, 100, 1, e.providerEth)
		e.insertPieces(t, "pdp_pieces_added", 200, 1)
		inboxBefore := e.shovelCount(t, "pdp_dataset_created")
		require.EqualValues(t, 1, inboxBefore)
		require.NoError(t, e.run(t, 10))

		// still exactly one ledger row per log, one proof set, one deal
		require.EqualValues(t, 1, e.recordCount(t, "", model.PDPEventDataSetCreated))
		require.EqualValues(t, 1, e.recordCount(t, "", model.PDPEventPiecesAdded))
		var psCount int64
		require.NoError(t, e.db.Model(&model.PDPProofSet{}).Count(&psCount).Error)
		require.EqualValues(t, 1, psCount)
		require.NoError(t, e.db.Model(&model.Deal{}).Where("deal_type = ?", model.DealTypePDP).Count(&dealCount).Error)
		require.EqualValues(t, 1, dealCount)
		require.EqualValues(t, 0, e.shovelCount(t, "pdp_dataset_created"))
	})
}

func (e reorgEnv) checkpoint(t *testing.T, num int64) (model.PDPChainCheckpoint, bool) {
	t.Helper()
	var cp model.PDPChainCheckpoint
	err := e.db.Where("block_num = ?", num).First(&cp).Error
	if errorsIsNotFound(err) {
		return cp, false
	}
	require.NoError(t, err)
	return cp, true
}

func errorsIsNotFound(err error) bool { return errors.Is(err, gorm.ErrRecordNotFound) }

func TestReorg_RollbackAndReplay_AllEventTypes(t *testing.T) {
	reorgTest(t, func(t *testing.T, e reorgEnv) {
		e.seedOldFork(t)

		newSP := common.HexToAddress("0x3333333333333333333333333333333333333333")
		expectedSP, _ := commonToDelegatedAddress(newSP)
		oldVerified := e.pdpDeal(t, 1).LastVerifiedAt

		// reorg at 300: old PossessionProven (300) and DataSetDeleted (350)
		// are abandoned; canonical chain only carries PossessionProven at 320
		e.chain.reorg(300)
		// shovel has not caught up with the new fork yet: replay must wait
		e.setShovelCursor(t, 299)

		require.NoError(t, e.run(t, 10))

		// visibility is blocked while recovery is incomplete
		status, err := GetSyncStatus(e.ctx, e.db)
		require.NoError(t, err)
		require.True(t, status.Blocked)
		require.Equal(t, model.PDPSyncPhaseRollingBack, status.Phase)
		require.NotNil(t, status.CommonNum)
		require.Equal(t, int64(299), *status.CommonNum, "nearest common checkpoint is the last pre-fork block")

		// derived state undone exactly to the common checkpoint
		ps := e.proofSet(t, 1)
		require.False(t, ps.Deleted, "DataSetDeleted must be undone")
		require.False(t, ps.IsLive, "PossessionProven must be undone")
		require.Equal(t, expectedSP.String(), ps.Provider, "pre-fork SP change survives")
		require.EqualValues(t, 500, *ps.ChallengeEpoch, "pre-fork proving period survives")

		deal := e.pdpDeal(t, 1)
		require.Equal(t, model.DealPublished, deal.State, "deal must return to pre-proof state")

		// old-fork events above the common checkpoint are marked forked
		require.EqualValues(t, 1, e.recordCount(t, model.PDPEventForked, model.PDPEventPossessionProven))
		require.EqualValues(t, 1, e.recordCount(t, model.PDPEventForked, model.PDPEventDataSetDeleted))

		// common checkpoint and its confirmed predecessors are untouched
		_, commonOk := e.checkpoint(t, 299)
		require.True(t, commonOk)
		_, cp280 := e.checkpoint(t, 280)
		require.True(t, cp280, "confirmed event checkpoint before fork survives")
		_, cp300 := e.checkpoint(t, 300)
		require.False(t, cp300, "checkpoint above common point must be removed")

		// snapshots below/at the fork point are retained for future reorgs
		var snapCount int64
		require.NoError(t, e.db.Model(&model.PDPStateSnapshot{}).
			Where("block_num <= ?", 299).Count(&snapCount).Error)
		require.Greater(t, snapCount, int64(0))
		require.NoError(t, e.db.Model(&model.PDPStateSnapshot{}).
			Where("block_num > ?", 299).Count(&snapCount).Error)
		require.EqualValues(t, 0, snapCount)

		// shovel indexes the canonical fork with PossessionProven at 320
		e.insertPossessionProven(t, 320, 1)
		e.setShovelCursor(t, 410)
		require.NoError(t, e.run(t, 10))

		status, err = GetSyncStatus(e.ctx, e.db)
		require.NoError(t, err)
		require.False(t, status.Blocked, "recovery must clear the queryable block")
		require.Equal(t, model.PDPSyncPhaseSyncing, status.Phase)

		// canonical derived state
		ps = e.proofSet(t, 1)
		require.False(t, ps.Deleted)
		require.True(t, ps.IsLive)
		require.Equal(t, expectedSP.String(), ps.Provider)
		require.EqualValues(t, 500, *ps.ChallengeEpoch)

		deal = e.pdpDeal(t, 1)
		require.Equal(t, model.DealActive, deal.State)
		require.NotNil(t, deal.LastVerifiedAt)
		require.True(t, deal.LastVerifiedAt.After(*oldVerified) || !deal.LastVerifiedAt.Equal(*oldVerified),
			"replay must re-stamp verification time")

		// new checkpoints carry canonical hashes; old forked rows never replay
		newCP300, ok := e.checkpoint(t, 300)
		require.True(t, ok)
		require.Equal(t, e.chain.HashAt(300).Bytes(), newCP300.BlockHash)
		require.EqualValues(t, 1, e.recordCount(t, model.PDPEventApplied, model.PDPEventPossessionProven))
		require.EqualValues(t, 1, e.recordCount(t, model.PDPEventForked, model.PDPEventPossessionProven))
	})
}

func TestReorg_ResumeAfterCrashMidUndo(t *testing.T) {
	reorgTest(t, func(t *testing.T, e reorgEnv) {
		e.seedOldFork(t)
		e.chain.reorg(300)

		// find the common checkpoint and begin rollback, then "crash" after
		// undoing only block 400
		cp, err := latestCheckpoint(e.ctx, e.db)
		require.NoError(t, err)
		common, err := findCommonCheckpoint(e.ctx, e.db, e.client, cp.BlockNum)
		require.NoError(t, err)
		require.Equal(t, int64(299), common.BlockNum)
		require.NoError(t, beginRollback(e.ctx, e.db, e.client, cp.BlockNum, common))
		require.NoError(t, undoBlock(e.ctx, e.db, 400))

		var st model.PDPSyncState
		require.NoError(t, e.db.Where("id = ?", syncStateRowID).First(&st).Error)
		require.Equal(t, model.PDPSyncPhaseRollingBack, st.Phase)
		require.NotNil(t, st.UndoNext)
		require.Equal(t, int64(399), *st.UndoNext)
		_, ok := e.checkpoint(t, 400)
		require.False(t, ok)
		_, ok = e.checkpoint(t, 399)
		require.True(t, ok, "undo progress must be granular")

		// restart: recovery resumes from the persisted cursor; shovel still
		// behind, so it completes undo and waits blocked
		e.setShovelCursor(t, 299)
		require.NoError(t, e.run(t, 10))
		status, err := GetSyncStatus(e.ctx, e.db)
		require.NoError(t, err)
		require.True(t, status.Blocked)
		require.Equal(t, int64(299), *status.CommonNum)

		// canonical events arrive, replay finishes on the next cycle
		e.insertPossessionProven(t, 320, 1)
		e.setShovelCursor(t, 410)
		require.NoError(t, e.run(t, 10))
		status, err = GetSyncStatus(e.ctx, e.db)
		require.NoError(t, err)
		require.False(t, status.Blocked)

		ps := e.proofSet(t, 1)
		require.False(t, ps.Deleted)
		require.True(t, ps.IsLive)
		require.Equal(t, model.DealActive, e.pdpDeal(t, 1).State)
	})
}

func TestReorg_RPCUnavailableRetainsInbox(t *testing.T) {
	reorgTest(t, func(t *testing.T, e reorgEnv) {
		e.insertDataSetCreated(t, 100, 1, e.providerEth)

		down := &ChainPDPClient{contract: e.mock, pageSize: 100, headers: errorHeaderSource{}}
		err := runReorgAwareSync(e.ctx, e.db, down, 10)
		require.Error(t, err)

		// nothing ingested, nothing applied, not blocked
		require.EqualValues(t, 0, e.recordCount(t, "", ""))
		require.EqualValues(t, 1, e.shovelCount(t, "pdp_dataset_created"))
		var psCount int64
		require.NoError(t, e.db.Model(&model.PDPProofSet{}).Count(&psCount).Error)
		require.EqualValues(t, 0, psCount)
	})
}

func TestReorg_ApplyFailureRetainsEventsAndCheckpoint(t *testing.T) {
	reorgTest(t, func(t *testing.T, e reorgEnv) {
		// listener lookup fails: block must not advance, rows retained
		e.insertDataSetCreated(t, 100, 1, e.providerEth)
		e.setShovelCursor(t, 410)
		e.mock.listeners = map[uint64]common.Address{}

		require.Error(t, e.run(t, 10))
		require.EqualValues(t, 1, e.recordCount(t, model.PDPEventPending, ""))
		var cpCount int64
		require.NoError(t, e.db.Model(&model.PDPChainCheckpoint{}).Count(&cpCount).Error)
		require.EqualValues(t, 0, cpCount, "no checkpoint may pass a failed block")

		// RPC recovers next cycle: same pending records apply once
		e.mock.listeners = map[uint64]common.Address{1: e.listenerEth}
		require.NoError(t, e.db.Create(&model.Actor{
			ID: "f0100", Address: e.listenerFil.String(),
		}).Error)
		require.NoError(t, e.run(t, 10))
		require.EqualValues(t, 0, e.recordCount(t, model.PDPEventPending, ""))
		require.EqualValues(t, 1, e.recordCount(t, model.PDPEventApplied, ""))
	})
}

func TestReorg_UntrackedClientStillConsumed(t *testing.T) {
	reorgTest(t, func(t *testing.T, e reorgEnv) {
		// proof set belongs to a client we don't track (no Actor row)
		e.insertDataSetCreated(t, 100, 1, e.providerEth)
		e.insertPieces(t, "pdp_pieces_added", 200, 1)
		e.setShovelCursor(t, 410)
		require.NoError(t, e.run(t, 10))

		var dealCount int64
		require.NoError(t, e.db.Model(&model.Deal{}).Where("deal_type = ?", model.DealTypePDP).Count(&dealCount).Error)
		require.EqualValues(t, 0, dealCount)
		require.EqualValues(t, 0, e.recordCount(t, model.PDPEventPending, ""))
		require.EqualValues(t, 2, e.recordCount(t, model.PDPEventApplied, ""))
	})
}

func TestReorg_PiecesOnDeletedProofSetConsumed(t *testing.T) {
	reorgTest(t, func(t *testing.T, e reorgEnv) {
		require.NoError(t, e.db.Create(&model.Actor{
			ID: "f0100", Address: e.listenerFil.String(),
		}).Error)
		e.insertDataSetCreated(t, 100, 1, e.providerEth)
		e.insertDataSetDeleted(t, 200, 1)
		e.insertPieces(t, "pdp_pieces_added", 201, 1)
		e.setShovelCursor(t, 410)
		require.NoError(t, e.run(t, 10))

		require.True(t, e.proofSet(t, 1).Deleted)
		var dealCount int64
		require.NoError(t, e.db.Model(&model.Deal{}).Where("deal_type = ?", model.DealTypePDP).Count(&dealCount).Error)
		require.EqualValues(t, 0, dealCount)
		require.EqualValues(t, 0, e.recordCount(t, model.PDPEventPending, ""))
	})
}

func TestReorg_ResetProcessorState(t *testing.T) {
	reorgTest(t, func(t *testing.T, e reorgEnv) {
		require.NoError(t, e.db.Create(&model.Actor{
			ID: "f0100", Address: e.listenerFil.String(),
		}).Error)
		e.insertDataSetCreated(t, 100, 1, e.providerEth)
		e.setShovelCursor(t, 410)
		require.NoError(t, e.run(t, 10))
		require.EqualValues(t, 1, e.recordCount(t, "", ""))

		// explicit full resync: bookkeeping wiped, derived state preserved
		require.NoError(t, ResetProcessorState(e.ctx, e.db))
		require.EqualValues(t, 0, e.recordCount(t, "", ""))
		var cpCount, snapCount int64
		require.NoError(t, e.db.Model(&model.PDPChainCheckpoint{}).Count(&cpCount).Error)
		require.NoError(t, e.db.Model(&model.PDPStateSnapshot{}).Count(&snapCount).Error)
		require.EqualValues(t, 0, cpCount)
		require.EqualValues(t, 0, snapCount)
		require.Equal(t, e.listenerFil.String(), e.proofSet(t, 1).ClientAddress)
	})
}

func TestReorg_NoCommonCheckpointBlocks(t *testing.T) {
	reorgTest(t, func(t *testing.T, e reorgEnv) {
		require.NoError(t, e.db.Create(&model.Actor{
			ID: "f0100", Address: e.listenerFil.String(),
		}).Error)
		e.insertDataSetCreated(t, 100, 1, e.providerEth)
		e.setShovelCursor(t, 410)
		require.NoError(t, e.run(t, 10))

		// replace the entire chain history, including block 100
		e.chain.epoch = 42
		e.chain.build(1, 410)
		e.setShovelCursor(t, 299)
		err := e.run(t, 10)
		require.ErrorIs(t, err, ErrNoCommonCheckpoint)

		status, err := GetSyncStatus(e.ctx, e.db)
		require.NoError(t, err)
		require.True(t, status.Blocked, "catastrophic reorg must remain queryably blocked")
	})
}

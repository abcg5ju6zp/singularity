package model

import (
	"fmt"
	"time"
)

type DealState string

type ScheduleState string

// DealType represents the type of deal (legacy market vs PDP)
type DealType string

const (
	DealProposed        DealState = "proposed"
	DealPublished       DealState = "published"
	DealActive          DealState = "active"
	DealExpired         DealState = "expired"
	DealProposalExpired DealState = "proposal_expired"
	DealRejected        DealState = "rejected"
	DealSlashed         DealState = "slashed"
	DealErrored         DealState = "error"
)

const (
	// DealTypeMarket represents legacy f05 market actor deals
	DealTypeMarket DealType = "market"
	// DealTypePDP represents f41 PDP (Proof of Data Possession) deals
	DealTypePDP DealType = "pdp"
	// DealTypeDDO represents DDO (Direct Data Onboarding) allocation deals
	DealTypeDDO DealType = "ddo"
)

type DDOAllocationStatus struct {
	Activated    bool
	SectorNumber uint64
}

const (
	// PDPProofSetMaxPieceSize is Singularity's current PDP max piece size policy:
	// 1 GiB minus FR32 expansion overhead.
	PDPProofSetMaxPieceSize int64 = (1 << 30) * 127 / 128 // 1,065,353,216 bytes
)

var DealStateStrings = []string{
	string(DealProposed),
	string(DealPublished),
	string(DealActive),
	string(DealExpired),
	string(DealProposalExpired),
	string(DealRejected),
	string(DealSlashed),
	string(DealErrored),
}

var DealStates = []DealState{
	DealProposed,
	DealPublished,
	DealActive,
	DealExpired,
	DealProposalExpired,
	DealRejected,
	DealSlashed,
	DealErrored,
}

var DealTypeStrings = []string{
	string(DealTypeMarket),
	string(DealTypePDP),
	string(DealTypeDDO),
}

var DealTypes = []DealType{
	DealTypeMarket,
	DealTypePDP,
	DealTypeDDO,
}

const (
	ScheduleActive    ScheduleState = "active"
	SchedulePaused    ScheduleState = "paused"
	ScheduleError     ScheduleState = "error"
	ScheduleCompleted ScheduleState = "completed"
)

var ScheduleStates = []ScheduleState{
	ScheduleActive,
	SchedulePaused,
	ScheduleError,
	ScheduleCompleted,
}

var ScheduleStateStrings = []string{
	string(ScheduleActive),
	string(SchedulePaused),
	string(ScheduleError),
	string(ScheduleCompleted),
}

type DealID uint64

// Deal is the deal model for all deals made by deal pusher or tracked by the tracker.
// index on PieceCID tracks replication of same piece
// index on State and ClientID calculates pending deals
type Deal struct {
	ID               DealID     `gorm:"primaryKey"                      json:"id"                                  table:"verbose"`
	CreatedAt        time.Time  `json:"createdAt"                       table:"verbose;format:2006-01-02 15:04:05"`
	UpdatedAt        time.Time  `json:"updatedAt"                       table:"verbose;format:2006-01-02 15:04:05"`
	LastVerifiedAt   *time.Time `json:"lastVerifiedAt"                  table:"verbose;format:2006-01-02 15:04:05"` // LastVerifiedAt is the last time the deal was verified as active by the tracker
	DealID           *uint64    `gorm:"unique"                          json:"dealId"`
	State            DealState  `gorm:"index:idx_pending"               json:"state"`
	DealType         DealType   `gorm:"index;default:'market'"          json:"dealType"`
	Provider         string     `json:"provider"`
	ProposalID       string     `json:"proposalId"                      table:"verbose"`
	Label            string     `json:"label"                           table:"verbose"`
	PieceCID         CID        `gorm:"column:piece_cid;index;size:255" json:"pieceCid"                            swaggertype:"string"`
	PieceSize        int64      `json:"pieceSize"`
	StartEpoch       int32      `json:"startEpoch"`
	EndEpoch         int32      `json:"endEpoch"                        table:"verbose"`
	SectorStartEpoch int32      `json:"sectorStartEpoch"                table:"verbose"`
	Price            string     `json:"price"`
	Verified         bool       `json:"verified"`
	ErrorMessage     string     `json:"errorMessage"                    table:"verbose"`

	// PDP-specific fields (only populated for DealTypePDP)
	ProofSetID         *uint64 `json:"proofSetId,omitempty"         table:"verbose"` // ProofSetID is the on-chain proof set ID for PDP deals
	ProofSetLive       *bool   `json:"proofSetLive,omitempty"       table:"verbose"` // ProofSetLive indicates if the proof set is live (actively being challenged)
	NextChallengeEpoch *int32  `json:"nextChallengeEpoch,omitempty" table:"verbose"` // NextChallengeEpoch is the next epoch when a challenge proof is due

	// DDO-specific fields (only populated for DealTypeDDO)
	DDOAllocationID *uint64 `json:"ddoAllocationId,omitempty" table:"verbose"`
	DDORailID       *uint64 `json:"ddoRailId,omitempty"       table:"verbose"`
	DDOPaymentToken *string `json:"ddoPaymentToken,omitempty" table:"verbose"`

	// Associations
	ScheduleID *ScheduleID `json:"scheduleId"                                         table:"verbose"`
	Schedule   *Schedule   `gorm:"foreignKey:ScheduleID;constraint:OnDelete:SET NULL" json:"schedule,omitempty" swaggerignore:"true" table:"expand"`
	ClientID   string      `gorm:"index:idx_pending"                                  json:"clientId"`
	WalletID   *uint       `gorm:"index:idx_deal_wallet"                              json:"walletId,omitempty"  table:"verbose"`
	Wallet     *Wallet     `gorm:"foreignKey:WalletID;constraint:OnDelete:SET NULL"   json:"wallet,omitempty"    swaggerignore:"true" table:"expand"`
}

// Key returns a mostly unique key to match deal from locally proposed deals and deals from the chain.
func (d Deal) Key() string {
	return fmt.Sprintf("%s-%s-%s-%d-%d", d.ClientID, d.Provider, d.PieceCID.String(), d.StartEpoch, d.EndEpoch)
}

type ScheduleID uint32

type Schedule struct {
	ID                    ScheduleID    `gorm:"primaryKey"                          json:"id"`
	CreatedAt             time.Time     `json:"createdAt"                           table:"verbose;format:2006-01-02 15:04:05"`
	UpdatedAt             time.Time     `json:"updatedAt"                           table:"verbose;format:2006-01-02 15:04:05"`
	URLTemplate           string        `json:"urlTemplate"                         table:"verbose"`
	HTTPHeaders           ConfigMap     `gorm:"type:JSON"                           json:"httpHeaders"                         table:"verbose"`
	Provider              string        `json:"provider"`
	PricePerGBEpoch       float64       `json:"pricePerGbEpoch"                     table:"verbose"`
	PricePerGB            float64       `json:"pricePerGb"                          table:"verbose"`
	PricePerDeal          float64       `json:"pricePerDeal"                        table:"verbose"`
	TotalDealNumber       int           `json:"totalDealNumber"                     table:"verbose"`
	TotalDealSize         int64         `json:"totalDealSize"`
	Verified              bool          `json:"verified"`
	KeepUnsealed          bool          `json:"keepUnsealed"                        table:"verbose"`
	AnnounceToIPNI        bool          `gorm:"column:announce_to_ipni"             json:"announceToIpni"                      table:"verbose"`
	StartDelay            time.Duration `json:"startDelay"                          swaggertype:"primitive,integer"`
	Duration              time.Duration `json:"duration"                            swaggertype:"primitive,integer"`
	State                 ScheduleState `json:"state"`
	ScheduleCron          string        `json:"scheduleCron"`
	ScheduleCronPerpetual bool          `json:"scheduleCronPerpetual"`
	ScheduleDealNumber    int           `json:"scheduleDealNumber"`
	ScheduleDealSize      int64         `json:"scheduleDealSize"`
	MaxPendingDealNumber  int           `json:"maxPendingDealNumber"`
	MaxPendingDealSize    int64         `json:"maxPendingDealSize"`
	Group                 string        `json:"group"                               table:"verbose"`
	Notes                 string        `json:"notes"`
	ErrorMessage          string        `json:"errorMessage"                        table:"verbose"`
	AllowedPieceCIDs      StringSlice   `gorm:"type:JSON;column:allowed_piece_cids" json:"allowedPieceCids"                    table:"verbose"`
	Force                 bool          `json:"force"`
	DealType              DealType      `gorm:"index;default:'market'"              json:"dealType"`

	// Associations
	PreparationID PreparationID `json:"preparationId"`
	Preparation   *Preparation  `gorm:"foreignKey:PreparationID;constraint:OnDelete:CASCADE" json:"preparation,omitempty" swaggerignore:"true" table:"expand"`
}

// on-chain actor identity tracked by singularity.
// actors we control have a Wallet with ActorID pointing here.
// actors we don't control (counterparties) exist as bare records.
type Actor struct {
	ID      string `gorm:"primaryKey;size:15" json:"id"`      // actor ID (f0...)
	Address string `gorm:"index"              json:"address"` // filecoin address
	// PrivateKey is excluded from GORM entirely so AutoMigrate won't
	// create the column and SELECTs won't reference it. The export-keys
	// handler reads it via raw SQL for legacy databases that still have it.

}

// GORM will rename "wallets" table to "actors" on AutoMigrate
func (Actor) TableName() string {
	return "actors"
}

// PDPEventType identifies a PDP contract event in the processor-owned
// event ledger. Values are stable and must not be renamed.
type PDPEventType string

const (
	PDPEventDataSetCreated    PDPEventType = "dataset_created"
	PDPEventSPChanged         PDPEventType = "sp_changed"
	PDPEventPiecesAdded       PDPEventType = "pieces_added"
	PDPEventPiecesRemoved     PDPEventType = "pieces_removed"
	PDPEventNextProvingPeriod PDPEventType = "next_proving_period"
	PDPEventPossessionProven  PDPEventType = "possession_proven"
	PDPEventDataSetDeleted    PDPEventType = "dataset_deleted"
)

// PDPEventStatus is the processing state of a PDPEventRecord.
type PDPEventStatus string

const (
	// PDPEventPending: ingested from the shovel inbox, not applied yet.
	PDPEventPending PDPEventStatus = "pending"
	// PDPEventApplied: effects are part of a confirmed checkpoint.
	PDPEventApplied PDPEventStatus = "applied"
	// PDPEventForked: block hash is no longer canonical; never applied or
	// eligible for replay.
	PDPEventForked PDPEventStatus = "forked"
)

// PDPEventRecord is the processor-owned durable copy of a shovel inbox row.
// Unlike the shovel fact tables (which shovel truncates when it detects a
// reorg), records here survive reorgs: rows from an abandoned fork are kept
// with status forked and their effects are undone via PDPStateSnapshot,
// while replacement rows on the new canonical chain carry a different
// BlockHash and get replayed.
type PDPEventRecord struct {
	ID       uint64       `gorm:"primaryKey" json:"id"`
	EventType PDPEventType `gorm:"type:text;not null;uniqueIndex:idx_pdp_event_key,priority:1;index:idx_pdp_event_block,priority:1" json:"eventType"`
	BlockNum int64        `gorm:"not null;uniqueIndex:idx_pdp_event_key,priority:2;index:idx_pdp_event_block,priority:2;index:idx_pdp_event_pending,priority:2" json:"blockNum"`
	BlockHash []byte      `gorm:"type:bytea;not null;uniqueIndex:idx_pdp_event_key,priority:3" json:"-"`
	TxIdx    int          `gorm:"uniqueIndex:idx_pdp_event_key,priority:4" json:"txIdx"`
	LogIdx   int          `gorm:"uniqueIndex:idx_pdp_event_key,priority:5" json:"logIdx"`
	SetID    uint64       `gorm:"not null;index:idx_pdp_event_pending,priority:3" json:"setId"`

	// event-specific payloads (only the columns relevant to EventType are set)
	StorageProvider  []byte `gorm:"type:bytea" json:"-"`
	NewSP            []byte `gorm:"type:bytea" json:"-"`
	ChallengeEpoch   *int64 `json:"challengeEpoch,omitempty"`
	LeafCount        *int64 `json:"leafCount,omitempty"`
	DeletedLeafCount *int64 `json:"deletedLeafCount,omitempty"`

	Status    PDPEventStatus `gorm:"type:text;not null;default:pending;index:idx_pdp_event_pending,priority:1" json:"status"`
	AppliedAt *time.Time     `json:"appliedAt,omitempty"`
	CreatedAt time.Time      `json:"createdAt"`
}

func (PDPEventRecord) TableName() string { return "pdp_event_records" }

// PDPChainCheckpoint marks that every event at block <= BlockNum (and, for
// the tip block, every log position up to LastTxIdx/LastLogIdx) has been
// applied to the derived tables from the chain identified by BlockHash.
// Checkpoints are the common-ancestor search path during reorg recovery.
type PDPChainCheckpoint struct {
	BlockNum   int64     `gorm:"primaryKey;autoIncrement:false" json:"blockNum"`
	BlockHash  []byte    `gorm:"type:bytea;not null" json:"-"`
	LastTxIdx  int       `json:"lastTxIdx"`
	LastLogIdx int       `json:"lastLogIdx"`
	AppliedAt  time.Time `json:"appliedAt"`
}

func (PDPChainCheckpoint) TableName() string { return "pdp_chain_checkpoints" }

// PDPSnapshotEntity identifies which derived table a PDPStateSnapshot row
// captures.
type PDPSnapshotEntity string

const (
	PDPSnapshotProofSet PDPSnapshotEntity = "proofset"
	PDPSnapshotDeal     PDPSnapshotEntity = "deal"
)

// PDPStateSnapshot is a before-image of a derived row captured immediately
// before a confirmed block's events are applied. During rollback, snapshots
// are restored in descending block order so derived state returns exactly to
// the nearest common checkpoint. Existed=false markers delete rows created by
// the block (e.g. proof sets or deals).
type PDPStateSnapshot struct {
	ID        uint64             `gorm:"primaryKey" json:"id"`
	BlockNum  int64              `gorm:"not null;uniqueIndex:idx_pdp_snapshot_key,priority:1" json:"blockNum"`
	Entity    PDPSnapshotEntity  `gorm:"type:text;not null;uniqueIndex:idx_pdp_snapshot_key,priority:2" json:"entity"`
	EntityID  uint64             `gorm:"not null;uniqueIndex:idx_pdp_snapshot_key,priority:3" json:"entityId"`
	Existed   bool               `json:"existed"`
	Data      []byte             `gorm:"type:bytea" json:"-"`
	CreatedAt time.Time          `json:"createdAt"`
}

func (PDPStateSnapshot) TableName() string { return "pdp_state_snapshots" }

// PDPSyncPhase is the persisted recovery state machine phase.
type PDPSyncPhase string

const (
	// PDPSyncPhaseSyncing: normal forward event application.
	PDPSyncPhaseSyncing PDPSyncPhase = "syncing"
	// PDPSyncPhaseRollingBack: a reorg was detected; derived visibility is
	// blocked until undo + replay reaches the confirmed chain tip.
	PDPSyncPhaseRollingBack PDPSyncPhase = "rolling_back"
)

// PDPSyncState is the singleton (ID=1) durable recovery marker. A restart
// resumes exactly the undo/replay position recorded here.
type PDPSyncState struct {
	ID         uint64         `gorm:"primaryKey" json:"id"`
	Phase      PDPSyncPhase   `gorm:"type:text;not null;default:syncing" json:"phase"`
	CommonNum  *int64         `json:"commonNum,omitempty"`
	CommonHash []byte         `gorm:"type:bytea" json:"-"`
	// UndoNext is the next block whose snapshots still need restoring.
	UndoNext  *int64  `json:"undoNext,omitempty"`
	Reason    string  `gorm:"type:text" json:"reason,omitempty"`
	UpdatedAt time.Time `json:"updatedAt"`
}

func (PDPSyncState) TableName() string { return "pdp_sync_state" }

type ProofSetHandoffState string

const (
	ProofSetAssembling  ProofSetHandoffState = "assembling"
	ProofSetTransferred ProofSetHandoffState = "transferred"
)

// PDPProofSet tracks on-chain PDP proof set state derived from contract events.
// This is a materialized view built from Shovel-indexed events, replacing
// the per-cycle RPC scans of GetProofSets/GetProofSetsForClient.
type PDPProofSet struct {
	SetID          uint64               `gorm:"primaryKey;autoIncrement:false" json:"setId"`
	ClientAddress  string               `gorm:"not null;index"                 json:"clientAddress"`
	Provider       string               `gorm:"not null"                       json:"provider"`
	IsLive         bool                 `gorm:"default:false"                  json:"isLive"`
	ChallengeEpoch *int64               `                                      json:"challengeEpoch,omitempty"`
	CreatedBlock   int64                `gorm:"not null"                       json:"createdBlock"`
	Deleted        bool                 `gorm:"default:false"                  json:"deleted"`
	HandoffState   ProofSetHandoffState `gorm:"default:'assembling'"           json:"handoffState"`
	PieceCount     int                  `gorm:"default:0"                      json:"pieceCount"`
	// ClientDataSetID is the per-(payer, set) nonce the client signs into
	// CreateDataSet/AddPieces extraData. FWSS rejects reused IDs via its
	// clientNonces[payer][id] check, so we persist before signing to make
	// retries idempotent. Stored decimal because uint256 doesn't fit any
	// native int.
	ClientDataSetID string `                                      json:"clientDataSetId,omitempty"`
	// ServiceURL caches the SP's PDP HTTP endpoint, fetched from
	// ServiceProviderRegistry the first time we push to this provider.
	ServiceURL string `                                      json:"serviceUrl,omitempty"`
}

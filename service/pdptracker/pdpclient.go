package pdptracker

import (
	"context"
	"math/big"

	"github.com/cockroachdb/errors"
	"github.com/data-preservation-programs/go-synapse/contracts"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"
)

const pdpDefaultPageSize uint64 = 100

type activePiecesResult struct {
	Pieces  []contracts.CidsCid
	HasMore bool
}

type pdpContractCaller interface {
	GetDataSetListener(opts *bind.CallOpts, setId *big.Int) (common.Address, error)
	GetActivePieces(opts *bind.CallOpts, setId *big.Int, offset *big.Int, limit *big.Int) (activePiecesResult, error)
}

type pdpVerifierCaller struct {
	contract *contracts.PDPVerifier
}

func (c pdpVerifierCaller) GetDataSetListener(opts *bind.CallOpts, setId *big.Int) (common.Address, error) {
	return c.contract.GetDataSetListener(opts, setId)
}

func (c pdpVerifierCaller) GetActivePieces(opts *bind.CallOpts, setId *big.Int, offset *big.Int, limit *big.Int) (activePiecesResult, error) {
	result, err := c.contract.GetActivePieces(opts, setId, offset, limit)
	if err != nil {
		return activePiecesResult{}, err
	}
	return activePiecesResult{Pieces: result.Pieces, HasMore: result.HasMore}, nil
}

// headerSource fetches block headers by number. ethclient.Client satisfies
// it; tests substitute a mock so reorgs can be driven deterministically.
type headerSource interface {
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
}

type ChainPDPClient struct {
	ethClient *ethclient.Client
	contract  pdpContractCaller
	pageSize  uint64
	headers   headerSource
}

func NewPDPClient(ctx context.Context, rpcURL string, contractAddr common.Address) (*ChainPDPClient, error) {
	if rpcURL == "" {
		return nil, errors.New("rpc URL is required")
	}

	ethClient, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to PDP RPC")
	}

	verifier, err := contracts.NewPDPVerifier(contractAddr, ethClient)
	if err != nil {
		ethClient.Close()
		return nil, errors.Wrap(err, "failed to init PDP verifier contract")
	}

	return &ChainPDPClient{
		ethClient: ethClient,
		contract:  pdpVerifierCaller{contract: verifier},
		pageSize:  pdpDefaultPageSize,
		headers:   ethClient,
	}, nil
}

func (c *ChainPDPClient) Close() error {
	if c.ethClient != nil {
		c.ethClient.Close()
	}
	return nil
}

func (c *ChainPDPClient) GetDataSetListener(ctx context.Context, setID uint64) (common.Address, error) {
	addr, err := c.contract.GetDataSetListener(&bind.CallOpts{Context: ctx}, new(big.Int).SetUint64(setID))
	if err != nil {
		return common.Address{}, errors.Wrap(err, "failed to get dataset listener")
	}
	return addr, nil
}

// paginates internally
func (c *ChainPDPClient) GetActivePieces(ctx context.Context, setID uint64) ([]cid.Cid, error) {
	setIDBig := new(big.Int).SetUint64(setID)
	var (
		offset uint64
		result []cid.Cid
	)

	for {
		pieces, err := c.contract.GetActivePieces(
			&bind.CallOpts{Context: ctx},
			setIDBig,
			new(big.Int).SetUint64(offset),
			new(big.Int).SetUint64(c.pageSize),
		)
		if err != nil {
			return nil, err
		}

		for i, piece := range pieces.Pieces {
			parsed, err := cid.Cast(piece.Data)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid piece CID at index %d", i)
			}
			result = append(result, parsed)
		}

		if !pieces.HasMore || len(pieces.Pieces) == 0 {
			break
		}
		offset += uint64(len(pieces.Pieces))
	}

	return result, nil
}

// BlockHeader returns the number and hash of the block at the given height.
// Pass num < 0 for the chain tip. Used for confirmation-depth gating and
// canonical-chain (reorg) detection.
func (c *ChainPDPClient) BlockHeader(ctx context.Context, num int64) (uint64, common.Hash, error) {
	if c.headers == nil {
		return 0, common.Hash{}, errors.New("block header source not configured")
	}
	var number *big.Int
	if num >= 0 {
		number = big.NewInt(num)
	}
	hdr, err := c.headers.HeaderByNumber(ctx, number)
	if err != nil {
		return 0, common.Hash{}, errors.Wrapf(err, "failed to get block header %d", num)
	}
	return hdr.Number.Uint64(), hdr.Hash(), nil
}

func commonToDelegatedAddress(subaddr common.Address) (address.Address, error) {
	addr, err := address.NewDelegatedAddress(10, subaddr.Bytes())
	if err != nil {
		return address.Undef, errors.Wrap(err, "failed to encode delegated address")
	}
	return addr, nil
}

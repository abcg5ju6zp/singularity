package storagesystem

import (
	"context"
	"io"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/data-preservation-programs/singularity/model"
	"github.com/rclone/rclone/fs"
	"gorm.io/gorm"
)

// FileReaderRouter is implemented by readers that can route a file
// read to the immutable storage generation that file version was
// scanned under. The pack assembler uses it so a storage update between
// scan and pack can never make a pack job fetch different bytes for a
// path it already verified.
type FileReaderRouter interface {
	ReaderForFile(file model.File) (Reader, error)
}

// GenerationFileReader opens rclone handlers lazily per pinned
// generation. Files without a pin (legacy/test rows) use the fallback
// snapshot, which is the job's pinned source generation.
type GenerationFileReader struct {
	ctx      context.Context
	db       *gorm.DB
	fallback model.Storage

	mu       sync.RWMutex
	handlers map[generationReaderKey]Handler
}

type generationReaderKey struct {
	generationID model.StorageGenerationID // 0 for the fallback
	storageID    model.StorageID
}

// NewGenerationFileReader builds a router whose fallback is a fully
// resolved storage snapshot (the job's pinned generation).
func NewGenerationFileReader(ctx context.Context, db *gorm.DB, fallback model.Storage) *GenerationFileReader {
	return &GenerationFileReader{
		ctx:      ctx,
		db:       db,
		fallback: fallback,
		handlers: make(map[generationReaderKey]Handler),
	}
}

func (r *GenerationFileReader) cached(key generationReaderKey, create func() (Handler, error)) (Handler, error) {
	r.mu.RLock()
	handler, ok := r.handlers[key]
	r.mu.RUnlock()
	if ok {
		return handler, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if handler, ok := r.handlers[key]; ok {
		return handler, nil
	}
	handler, err := create()
	if err != nil {
		return nil, err
	}
	r.handlers[key] = handler
	return handler, nil
}

func (r *GenerationFileReader) fallbackHandler() (Handler, error) {
	return r.cached(generationReaderKey{storageID: r.fallback.ID}, func() (Handler, error) {
		return NewRCloneHandler(r.ctx, r.fallback)
	})
}

// Name satisfies Reader; the routed readers carry their own
// generation-specific storage names.
func (r *GenerationFileReader) Name() string {
	return r.fallback.Name
}

// Read satisfies Reader using the fallback snapshot; the pack
// assembler routes through ReaderForFile when it has a file record.
func (r *GenerationFileReader) Read(ctx context.Context, path string, offset, length int64) (io.ReadCloser, fs.Object, error) {
	handler, err := r.fallbackHandler()
	if err != nil {
		return nil, nil, err
	}
	return handler.Read(ctx, path, offset, length)
}

// ReaderForFile returns the backend reader pinned to the generation the
// file version was scanned under, creating and caching its rclone
// handler on first use.
func (r *GenerationFileReader) ReaderForFile(file model.File) (Reader, error) {
	if file.StorageGenerationID == nil {
		return r.fallbackHandler()
	}
	generationID := *file.StorageGenerationID
	return r.cached(generationReaderKey{generationID: generationID}, func() (Handler, error) {
		snapshot, err := GenerationSnapshot(r.ctx, r.db, generationID)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return NewRCloneHandler(r.ctx, snapshot)
	})
}

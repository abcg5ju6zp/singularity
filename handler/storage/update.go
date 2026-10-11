package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/data-preservation-programs/singularity/database"
	"github.com/data-preservation-programs/singularity/handler/handlererror"
	"github.com/data-preservation-programs/singularity/model"
	"github.com/data-preservation-programs/singularity/storagesystem"
	"github.com/rjNemo/underscore"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type UpdateRequest struct {
	Config       map[string]string  `json:"config"`
	ClientConfig model.ClientConfig `json:"clientConfig"`
}

// mergeStorageConfig merges an update request on top of the current storage
// configuration, applying the same provider/default resolution used at create
// time. It is pure so it can be evaluated while holding the storage row lock.
func mergeStorageConfig(storage model.Storage, request UpdateRequest) (model.Storage, error) {
	backend, ok := storagesystem.BackendMap[storage.Type]
	if !ok {
		return model.Storage{}, errors.Newf("storage type %s is not supported", storage.Type)
	}

	provider := storage.Config["provider"]
	if p, ok := request.Config["provider"]; ok {
		provider = p
	}

	rcloneConfig := make(map[string]string)
	providerOptions, err := underscore.Find(backend.ProviderOptions, func(providerOption storagesystem.ProviderOptions) bool {
		return providerOption.Provider == provider
	})
	if err != nil {
		return model.Storage{}, errors.Wrapf(handlererror.ErrInvalidParameter, "provider '%s' is not supported", provider)
	}

	for _, option := range providerOptions.Options {
		if option.Default != nil {
			rcloneConfig[option.Name] = fmt.Sprintf("%v", option.Default)
		}
	}

	for key, value := range storage.Config {
		rcloneConfig[key] = value
	}

	for key, value := range request.Config {
		rcloneConfig[key] = value
	}

	storage.Config = rcloneConfig
	OverrideStorageWithClientConfig(&storage, request.ClientConfig)
	return storage, nil
}

// lockStorageForUpdate loads the storage by ID or name while taking a row
// lock. The lock is the single serialization point for generation activation.
func lockStorageForUpdate(ctx context.Context, tx *gorm.DB, name string) (model.Storage, error) {
	var storage model.Storage
	query := tx.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"})
	if id, parseErr := strconv.ParseUint(name, 10, 32); parseErr == nil && fmt.Sprintf("%d", id) == name {
		err := query.First(&storage, id).Error
		return storage, err
	}
	err := query.Where("name = ?", name).First(&storage).Error
	return storage, err
}

// UpdateStorageHandler updates the configuration of a given storage system.
//
// Updates are implemented as immutable configuration generations:
//   - The storage row is locked first, so concurrent updates take effect in
//     exactly one deterministic activation order, each merging against the
//     freshest committed configuration.
//   - The merged configuration is validated against the real backend before
//     anything is committed. If handler creation or listing fails the whole
//     transaction rolls back: the previous generation stays active and usable.
//   - On success a new generation row is inserted and atomically marked
//     active, the storages row is mirrored to it, and older generations without
//     any remaining attachment/job/file/car reference are cleaned up.
//
// Already claimed jobs, long lived readers and in-flight service requests
// keep using the generation they pinned until they finish; only newly started
// operations see the new configuration.
//
// Parameters:
//   - ctx: A context.Context for request-scoped values, cancellation signals, and deadlines.
//   - db: A *gorm.DB instance for database interactions.
//   - name: The name of the storage system to be updated.
//   - request: The new configuration values to merge and the client config overrides.
//
// Returns:
//   - A pointer to the updated storage model (mirroring the new active generation).
//   - An error if there's an issue during the update process, otherwise nil.
func (DefaultHandler) UpdateStorageHandler(
	ctx context.Context,
	db *gorm.DB,
	name string,
	request UpdateRequest,
) (*model.Storage, error) {
	var storage model.Storage
	err := database.DoRetry(ctx, func() error {
		return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var err error
			storage, err = lockStorageForUpdate(ctx, tx, name)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errors.Wrapf(handlererror.ErrNotFound, "storage %s does not exist", name)
			}
			if err != nil {
				return errors.WithStack(err)
			}

			candidate, err := mergeStorageConfig(storage, request)
			if err != nil {
				return err
			}

			rclone, err := storagesystem.NewRCloneHandler(ctx, candidate)
			if err != nil {
				return errors.Join(handlererror.ErrInvalidParameter, errors.Wrap(err, "creating rclone handler failed"))
			}

			_, err = rclone.List(ctx, "")
			if err != nil {
				return errors.Join(handlererror.ErrInvalidParameter, errors.Wrap(err, "listing the storage failed"))
			}

			var maxVersion uint32
			err = tx.Model(&model.StorageGeneration{}).
				Where("storage_id = ?", storage.ID).
				Select("COALESCE(MAX(version), 0)").Scan(&maxVersion).Error
			if err != nil {
				return errors.WithStack(err)
			}

			generation := model.StorageGeneration{
				StorageID:    storage.ID,
				Version:      maxVersion + 1,
				Name:         storage.Name,
				Type:         candidate.Type,
				Path:         candidate.Path,
				Config:       candidate.Config,
				ClientConfig: candidate.ClientConfig,
			}
			err = tx.Create(&generation).Error
			if err != nil {
				return errors.Wrap(err, "failed to create storage generation")
			}

			// atomic activation: a single pointer flip makes the new
			// generation visible to new operations; nothing mutates old rows
			err = tx.Model(&model.Storage{}).Where("id = ?", storage.ID).Updates(map[string]any{
				"config":               candidate.Config,
				"client_config":        candidate.ClientConfig,
				"active_generation_id": generation.ID,
				"updated_at":           time.Now(),
			}).Error
			if err != nil {
				return errors.WithStack(err)
			}

			if err := storagesystem.DeleteUnreferencedGenerations(ctx, tx, storage.ID); err != nil {
				return errors.Wrap(err, "failed to clean up old storage generations")
			}

			candidate.ActiveGenerationID = &generation.ID
			candidate.GenerationID = &generation.ID
			storage = candidate
			return nil
		}, &sql.TxOptions{})
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &storage, nil
}

func OverrideStorageWithClientConfig(storage *model.Storage, config model.ClientConfig) {
	if config.ConnectTimeout != nil {
		storage.ClientConfig.ConnectTimeout = config.ConnectTimeout
	}
	if config.Timeout != nil {
		storage.ClientConfig.Timeout = config.Timeout
	}
	if config.ExpectContinueTimeout != nil {
		storage.ClientConfig.ExpectContinueTimeout = config.ExpectContinueTimeout
	}
	if config.InsecureSkipVerify != nil {
		storage.ClientConfig.InsecureSkipVerify = config.InsecureSkipVerify
	}
	if config.NoGzip != nil {
		storage.ClientConfig.NoGzip = config.NoGzip
	}
	if config.UserAgent != nil {
		storage.ClientConfig.UserAgent = config.UserAgent
		if *storage.ClientConfig.UserAgent == "" {
			storage.ClientConfig.UserAgent = nil
		}
	}
	if len(config.CaCert) > 0 {
		storage.ClientConfig.CaCert = config.CaCert
		if storage.ClientConfig.CaCert[0] == "" {
			storage.ClientConfig.CaCert = nil
		}
	}
	if config.ClientCert != nil {
		storage.ClientConfig.ClientCert = config.ClientCert
		if *storage.ClientConfig.ClientCert == "" {
			storage.ClientConfig.ClientCert = nil
		}
	}
	if config.ClientKey != nil {
		storage.ClientConfig.ClientKey = config.ClientKey
		if *storage.ClientConfig.ClientKey == "" {
			storage.ClientConfig.ClientKey = nil
		}
	}
	if config.DisableHTTP2 != nil {
		storage.ClientConfig.DisableHTTP2 = config.DisableHTTP2
	}
	if config.DisableHTTPKeepAlives != nil {
		storage.ClientConfig.DisableHTTPKeepAlives = config.DisableHTTPKeepAlives
	}
	if config.Headers != nil {
		for key, value := range config.Headers {
			if key == "" && value == "" {
				storage.ClientConfig.Headers = make(map[string]string)
				break
			}
			if value == "" {
				delete(storage.ClientConfig.Headers, key)
			} else {
				storage.ClientConfig.Headers[key] = value
			}
		}
	}
	if config.RetryMaxCount != nil {
		storage.ClientConfig.RetryMaxCount = config.RetryMaxCount
	}
	if config.RetryDelay != nil {
		storage.ClientConfig.RetryDelay = config.RetryDelay
	}
	if config.RetryBackoff != nil {
		storage.ClientConfig.RetryBackoff = config.RetryBackoff
	}
	if config.RetryBackoffExponential != nil {
		storage.ClientConfig.RetryBackoffExponential = config.RetryBackoffExponential
	}
	if config.SkipInaccessibleFile != nil {
		storage.ClientConfig.SkipInaccessibleFile = config.SkipInaccessibleFile
	}
	if config.UseServerModTime != nil {
		storage.ClientConfig.UseServerModTime = config.UseServerModTime
	}
	if config.LowLevelRetries != nil {
		storage.ClientConfig.LowLevelRetries = config.LowLevelRetries
	}
	if config.ScanConcurrency != nil {
		storage.ClientConfig.ScanConcurrency = config.ScanConcurrency
	}
}

// @ID UpdateStorage
// @Summary Update a storage connection
// @Tags Storage
// @Param name path string true "Storage ID or name"
// @Param config body map[string]string true "Configuration"
// @Accept json
// @Produce json
// @Success 200 {object} model.Storage
// @Failure 400 {object} api.HTTPError
// @Failure 500 {object} api.HTTPError
// @Router /storage/{name} [patch]
func _() {}

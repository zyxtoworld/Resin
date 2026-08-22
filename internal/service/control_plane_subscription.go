package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/topology"
)

// ------------------------------------------------------------------
// Subscription
// ------------------------------------------------------------------

// SubscriptionResponse is the API response for a subscription.
type SubscriptionResponse struct {
	ID                      string `json:"id"`
	Name                    string `json:"name"`
	SourceType              string `json:"source_type"`
	URL                     string `json:"url"`
	Content                 string `json:"content"`
	UpdateInterval          string `json:"update_interval"`
	NodeCount               int    `json:"node_count"`
	HealthyNodeCount        int    `json:"healthy_node_count"`
	Ephemeral               bool   `json:"ephemeral"`
	IncrementalAliveNodes   bool   `json:"incremental_alive_nodes"`
	EphemeralNodeEvictDelay string `json:"ephemeral_node_evict_delay"`
	Enabled                 bool   `json:"enabled"`
	CreatedAt               string `json:"created_at"`
	LastChecked             string `json:"last_checked,omitempty"`
	LastUpdated             string `json:"last_updated,omitempty"`
	LastError               string `json:"last_error,omitempty"`
}

const maxSubscriptionResponseAttempts = 3

var errSubscriptionChangedDuringRead = errors.New("subscription changed while reading")

func (s *ControlPlaneService) subToResponseSnapshot(sub *subscription.Subscription, cfg subscription.ConfigSnapshot) SubscriptionResponse {
	if hook := s.afterSubscriptionNameReadHook; hook != nil {
		hook()
	}
	nodeCount := 0
	healthyNodeCount := 0
	var isHealthyAndEnabled func(*node.NodeEntry) bool
	if cfg.Enabled && s != nil && s.Pool != nil {
		isHealthyAndEnabled = s.Pool.MakeHealthyAndEnabledEvaluator()
	}
	if managed := sub.ManagedNodes(); managed != nil {
		managed.RangeNodes(func(h node.Hash, n subscription.ManagedNode) bool {
			if n.Evicted {
				return true
			}
			nodeCount++
			if isHealthyAndEnabled != nil {
				entry, ok := s.Pool.GetEntry(h)
				if ok && isHealthyAndEnabled(entry) {
					healthyNodeCount++
				}
			}
			return true
		})
	}

	resp := SubscriptionResponse{
		ID:                      sub.ID,
		Name:                    cfg.Name,
		SourceType:              cfg.SourceType,
		URL:                     cfg.URL,
		Content:                 cfg.Content,
		UpdateInterval:          time.Duration(cfg.UpdateIntervalNs).String(),
		NodeCount:               nodeCount,
		HealthyNodeCount:        healthyNodeCount,
		Ephemeral:               cfg.Ephemeral,
		IncrementalAliveNodes:   cfg.IncrementalAliveNodes,
		EphemeralNodeEvictDelay: time.Duration(cfg.EphemeralNodeEvictDelayNs).String(),
		Enabled:                 cfg.Enabled,
		CreatedAt:               time.Unix(0, cfg.CreatedAtNs).UTC().Format(time.RFC3339Nano),
	}
	if lc := cfg.LastCheckedNs; lc > 0 {
		resp.LastChecked = time.Unix(0, lc).UTC().Format(time.RFC3339Nano)
	}
	if lu := cfg.LastUpdatedNs; lu > 0 {
		resp.LastUpdated = time.Unix(0, lu).UTC().Format(time.RFC3339Nano)
	}
	resp.LastError = cfg.LastError
	return resp
}

// ListSubscriptions returns all subscriptions, optionally filtered by enabled.
func (s *ControlPlaneService) ListSubscriptions(enabled *bool) ([]SubscriptionResponse, error) {
	return s.ListSubscriptionsContext(context.Background(), enabled)
}

// ListSubscriptionsContext returns all subscriptions while honoring request
// cancellation before each subscription snapshot and runtime-generation read
// is admitted.
func (s *ControlPlaneService) ListSubscriptionsContext(ctx context.Context, enabled *bool) ([]SubscriptionResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	for attempt := 0; attempt < maxSubscriptionResponseAttempts; attempt++ {
		type candidate struct {
			id      string
			sub     *subscription.Subscription
			cfg     subscription.ConfigSnapshot
			managed *subscription.ManagedNodes
		}
		var candidates []candidate
		var snapshotErr error
		s.SubMgr.Range(func(id string, sub *subscription.Subscription) bool {
			if err := ctx.Err(); err != nil {
				snapshotErr = err
				return false
			}
			cfg, err := sub.SnapshotConfigContext(ctx)
			if err != nil {
				snapshotErr = err
				return false
			}
			if enabled == nil || cfg.Enabled == *enabled {
				candidates = append(candidates, candidate{
					id:      id,
					sub:     sub,
					cfg:     cfg,
					managed: sub.ManagedNodes(),
				})
			}
			return true
		})
		if snapshotErr != nil {
			return nil, snapshotErr
		}

		stable := true
		var result []SubscriptionResponse
		if err := s.withRuntimeReadContext(ctx, func() {
			for _, item := range candidates {
				// A delete/re-register can occur between the config snapshot and
				// the runtime read. Do not combine the old config with a new object.
				if s.SubMgr.Lookup(item.id) != item.sub {
					continue
				}
				result = append(result, s.subToResponseSnapshot(item.sub, item.cfg))
				if item.sub.ManagedNodes() != item.managed {
					stable = false
				}
			}
		}); err != nil {
			return nil, err
		}
		if stable {
			if result == nil {
				result = []SubscriptionResponse{}
			}
			return result, nil
		}
	}
	return nil, internal("list subscriptions", errSubscriptionChangedDuringRead)
}

// GetSubscription returns a single subscription by ID.
func (s *ControlPlaneService) GetSubscription(id string) (*SubscriptionResponse, error) {
	return s.GetSubscriptionContext(context.Background(), id)
}

// GetSubscriptionContext returns one subscription while honoring cancellation
// before its configuration snapshot and runtime-generation read are admitted.
func (s *ControlPlaneService) GetSubscriptionContext(ctx context.Context, id string) (*SubscriptionResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for attempt := 0; attempt < maxSubscriptionResponseAttempts; attempt++ {
		sub := s.SubMgr.Lookup(id)
		if sub == nil {
			return nil, notFound("subscription not found")
		}
		cfg, err := sub.SnapshotConfigContext(ctx)
		if err != nil {
			return nil, err
		}
		managed := sub.ManagedNodes()
		var (
			result  *SubscriptionResponse
			readErr error
		)
		if err := s.withRuntimeReadContext(ctx, func() {
			// Do not return a response for a deleted object or accidentally apply
			// its snapshot to a replacement registered under the same ID.
			if s.SubMgr.Lookup(id) != sub {
				readErr = notFound("subscription not found")
				return
			}
			r := s.subToResponseSnapshot(sub, cfg)
			result = &r
		}); err != nil {
			return nil, err
		}
		if readErr != nil {
			return nil, readErr
		}
		if sub.ManagedNodes() == managed {
			return result, nil
		}
	}
	return nil, internal("get subscription", errSubscriptionChangedDuringRead)
}

// CreateSubscriptionRequest holds create subscription parameters.
type CreateSubscriptionRequest struct {
	Name                    *string `json:"name"`
	SourceType              *string `json:"source_type"`
	URL                     *string `json:"url"`
	Content                 *string `json:"content"`
	UpdateInterval          *string `json:"update_interval"`
	Enabled                 *bool   `json:"enabled"`
	Ephemeral               *bool   `json:"ephemeral"`
	IncrementalAliveNodes   *bool   `json:"incremental_alive_nodes"`
	EphemeralNodeEvictDelay *string `json:"ephemeral_node_evict_delay"`
}

const minSubscriptionUpdateInterval = 30 * time.Second
const defaultSubscriptionEphemeralNodeEvictDelay = 72 * time.Hour

func parseSubscriptionSourceType(raw *string) (string, *ServiceError) {
	if raw == nil {
		return subscription.SourceTypeRemote, nil
	}
	value := strings.ToLower(strings.TrimSpace(*raw))
	switch value {
	case subscription.SourceTypeRemote, subscription.SourceTypeLocal:
		return value, nil
	default:
		return "", invalidArg("source_type: must be remote or local")
	}
}

// CreateSubscription creates a new subscription.
func (s *ControlPlaneService) CreateSubscription(req CreateSubscriptionRequest) (*SubscriptionResponse, error) {
	return s.CreateSubscriptionContext(context.Background(), req)
}

// CreateSubscriptionContext creates a subscription while honoring the
// caller's cancellation before the irreversible persistence transaction.
// After that boundary, the shutdown-owned commit context carries the full
// persistence-to-runtime publication through a client disconnect.
func (s *ControlPlaneService) CreateSubscriptionContext(ctx context.Context, req CreateSubscriptionRequest) (*SubscriptionResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.Name == nil || strings.TrimSpace(*req.Name) == "" {
		return nil, invalidArg("name is required")
	}
	name := strings.TrimSpace(*req.Name)

	sourceType, verr := parseSubscriptionSourceType(req.SourceType)
	if verr != nil {
		return nil, verr
	}

	subURL := ""
	content := ""
	switch sourceType {
	case subscription.SourceTypeRemote:
		if req.URL == nil || strings.TrimSpace(*req.URL) == "" {
			return nil, invalidArg("url is required for remote subscription")
		}
		subURL = strings.TrimSpace(*req.URL)
		if _, verr := parseHTTPAbsoluteURL("url", subURL); verr != nil {
			return nil, verr
		}
		if req.Content != nil && strings.TrimSpace(*req.Content) != "" {
			return nil, invalidArg("content is not allowed for remote subscription")
		}
	case subscription.SourceTypeLocal:
		if req.Content == nil || strings.TrimSpace(*req.Content) == "" {
			return nil, invalidArg("content is required for local subscription")
		}
		content = *req.Content
		if req.URL != nil && strings.TrimSpace(*req.URL) != "" {
			return nil, invalidArg("url is not allowed for local subscription")
		}
	default:
		return nil, invalidArg("source_type: must be remote or local")
	}

	updateInterval := 5 * time.Minute
	if req.UpdateInterval != nil {
		d, err := time.ParseDuration(*req.UpdateInterval)
		if err != nil {
			return nil, invalidArg("update_interval: " + err.Error())
		}
		if d < minSubscriptionUpdateInterval {
			return nil, invalidArg("update_interval: must be >= 30s")
		}
		updateInterval = d
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	ephemeral := false
	if req.Ephemeral != nil {
		ephemeral = *req.Ephemeral
	}
	incrementalAliveNodes := false
	if req.IncrementalAliveNodes != nil {
		incrementalAliveNodes = *req.IncrementalAliveNodes
	}
	ephemeralNodeEvictDelay := defaultSubscriptionEphemeralNodeEvictDelay
	if req.EphemeralNodeEvictDelay != nil {
		d, err := time.ParseDuration(*req.EphemeralNodeEvictDelay)
		if err != nil {
			return nil, invalidArg("ephemeral_node_evict_delay: " + err.Error())
		}
		if d < 0 {
			return nil, invalidArg("ephemeral_node_evict_delay: must be non-negative")
		}
		ephemeralNodeEvictDelay = d
	}

	id := uuid.New().String()
	now := time.Now().UnixNano()

	ms := model.Subscription{
		ID:                        id,
		Name:                      name,
		SourceType:                sourceType,
		URL:                       subURL,
		Content:                   content,
		UpdateIntervalNs:          int64(updateInterval),
		Enabled:                   enabled,
		Ephemeral:                 ephemeral,
		IncrementalAliveNodes:     incrementalAliveNodes,
		EphemeralNodeEvictDelayNs: int64(ephemeralNodeEvictDelay),
		CreatedAtNs:               now,
		UpdatedAtNs:               now,
	}
	if s == nil || s.Engine == nil {
		return nil, internal("subscription persistence unavailable", errors.New("state engine is nil"))
	}
	if s.SubMgr == nil {
		return nil, internal("subscription runtime unavailable", errors.New("subscription manager is nil"))
	}
	var sub *subscription.Subscription
	if err := s.Engine.WithStateWriteAdmissionContextAndCommit(ctx, func(writeCtx, _ context.Context) error {
		if err := s.Engine.UpsertSubscriptionContextAndCommit(writeCtx, ms); err != nil {
			return internal("persist subscription", err)
		}
		if hook := s.afterSubscriptionPersistHook; hook != nil {
			hook()
		}

		sub = subscription.NewSubscription(id, name, subURL, enabled, ephemeral)
		sub.SetFetchConfig(subURL, int64(updateInterval))
		sub.SetSourceType(sourceType)
		sub.SetContent(content)
		sub.SetIncrementalAliveNodes(incrementalAliveNodes)
		sub.SetEphemeralNodeEvictDelayNs(int64(ephemeralNodeEvictDelay))
		sub.CreatedAtNs = now
		sub.UpdatedAtNs = now
		s.SubMgr.Register(sub)
		return nil
	}); err != nil {
		var serviceErr *ServiceError
		if errors.As(err, &serviceErr) {
			return nil, serviceErr
		}
		return nil, internal("create subscription", err)
	}

	cfg := sub.SnapshotConfig()
	var r SubscriptionResponse
	s.withRuntimeRead(func() {
		r = s.subToResponseSnapshot(sub, cfg)
	})
	return &r, nil
}

// UpdateSubscription applies a constrained partial patch to a subscription.
// This is not RFC 7396 JSON Merge Patch: patch must be a non-empty object and
// null values are rejected.
func (s *ControlPlaneService) UpdateSubscription(id string, patchJSON json.RawMessage) (*SubscriptionResponse, error) {
	return s.UpdateSubscriptionContext(context.Background(), id, patchJSON)
}

// UpdateSubscriptionContext applies a subscription patch while honoring the
// caller's cancellation before the irreversible persistence transaction.
// After that boundary, the shutdown-owned commit context carries the full
// persistence-to-runtime publication through a client disconnect.
func (s *ControlPlaneService) UpdateSubscriptionContext(ctx context.Context, id string, patchJSON json.RawMessage) (*SubscriptionResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	patch, verr := parseMergePatch(patchJSON)
	if verr != nil {
		return nil, verr
	}
	if err := patch.validateFields(subscriptionPatchAllowedFields, func(key string) string {
		return fmt.Sprintf("field %q is read-only or unknown", key)
	}); err != nil {
		return nil, err
	}
	if s == nil || s.Engine == nil {
		return nil, internal("subscription persistence unavailable", errors.New("state engine is nil"))
	}
	if s.SubMgr == nil {
		return nil, internal("subscription runtime unavailable", errors.New("subscription manager is nil"))
	}

	sub := s.SubMgr.Lookup(id)
	if sub == nil {
		return nil, notFound("subscription not found")
	}

	// The subscription operation lock owns the complete read -> validate ->
	// persist -> runtime mutation transaction. Scheduler Locked methods below
	// deliberately do not reacquire the same mutex.
	var (
		updateErr      error
		refreshRuntime bool
	)
	s.runSubscriptionMutationHook(subscriptionMutationBeforeLock)
	mutate := func() error {
		if hook := s.beforeSubscriptionOperationLockHook; hook != nil {
			hook()
		}
		var opErr error
		if err := sub.WithOpLockContext(ctx, func() {
			opErr = s.Engine.WithStateWriteAdmissionContextAndCommit(ctx, func(writeCtx, _ context.Context) error {
				// Delete can win after the initial lookup but before this operation gets
				// the lock. Do not resurrect a deleted row or runtime object.
				if s.SubMgr.Lookup(id) != sub {
					updateErr = notFound("subscription not found")
					return updateErr
				}

				nameChanged := false
				enabledChanged := false
				urlChanged := false
				contentChanged := false
				sourceType := sub.SourceType()

				newName := sub.Name()
				if nameStr, ok, err := patch.optionalNonEmptyString("name"); err != nil {
					updateErr = err
					return updateErr
				} else if ok {
					newName = nameStr
					nameChanged = newName != sub.Name()
				}

				newURL := sub.URL()
				if urlStr, ok, err := patch.optionalString("url"); err != nil {
					updateErr = err
					return updateErr
				} else if ok {
					if sourceType != subscription.SourceTypeRemote {
						updateErr = invalidArg("url: field is not allowed for local subscription")
						return updateErr
					}
					if _, verr := parseHTTPAbsoluteURL("url", urlStr); verr != nil {
						updateErr = verr
						return updateErr
					}
					newURL = urlStr
					urlChanged = newURL != sub.URL()
				}

				newContent := sub.Content()
				if contentStr, ok, err := patch.optionalString("content"); err != nil {
					updateErr = err
					return updateErr
				} else if ok {
					if sourceType != subscription.SourceTypeLocal {
						updateErr = invalidArg("content: field is not allowed for remote subscription")
						return updateErr
					}
					if strings.TrimSpace(contentStr) == "" {
						updateErr = invalidArg("content: must be a non-empty string")
						return updateErr
					}
					newContent = contentStr
					contentChanged = newContent != sub.Content()
				}

				newInterval := sub.UpdateIntervalNs()
				if d, ok, err := patch.optionalDurationString("update_interval"); err != nil {
					updateErr = err
					return updateErr
				} else if ok {
					if d < minSubscriptionUpdateInterval {
						updateErr = invalidArg("update_interval: must be >= 30s")
						return updateErr
					}
					newInterval = int64(d)
				}

				newEnabled := sub.Enabled()
				if b, ok, err := patch.optionalBool("enabled"); err != nil {
					updateErr = err
					return updateErr
				} else if ok {
					enabledChanged = b != newEnabled
					newEnabled = b
				}

				newEphemeral := sub.Ephemeral()
				if b, ok, err := patch.optionalBool("ephemeral"); err != nil {
					updateErr = err
					return updateErr
				} else if ok {
					newEphemeral = b
				}

				newIncrementalAliveNodes := sub.IncrementalAliveNodes()
				if b, ok, err := patch.optionalBool("incremental_alive_nodes"); err != nil {
					updateErr = err
					return updateErr
				} else if ok {
					newIncrementalAliveNodes = b
				}

				newEphemeralNodeEvictDelay := sub.EphemeralNodeEvictDelayNs()
				if d, ok, err := patch.optionalDurationString("ephemeral_node_evict_delay"); err != nil {
					updateErr = err
					return updateErr
				} else if ok {
					if d < 0 {
						updateErr = invalidArg("ephemeral_node_evict_delay: must be non-negative")
						return updateErr
					}
					newEphemeralNodeEvictDelay = int64(d)
				}
				s.runSubscriptionMutationHook(subscriptionMutationAfterLoad)

				now := time.Now().UnixNano()
				ms := model.Subscription{
					ID:                        id,
					Name:                      newName,
					SourceType:                sourceType,
					URL:                       newURL,
					Content:                   newContent,
					UpdateIntervalNs:          newInterval,
					Enabled:                   newEnabled,
					Ephemeral:                 newEphemeral,
					IncrementalAliveNodes:     newIncrementalAliveNodes,
					EphemeralNodeEvictDelayNs: newEphemeralNodeEvictDelay,
					CreatedAtNs:               sub.CreatedAtNs,
					UpdatedAtNs:               now,
				}
				if err := s.Engine.UpsertSubscriptionContextAndCommit(writeCtx, ms); err != nil {
					updateErr = internal("persist subscription", err)
					return updateErr
				}
				if hook := s.afterSubscriptionPersistHook; hook != nil {
					hook()
				}

				// Persistence succeeded; now apply every in-memory mutation while the
				// same operation lock still excludes delete/refresh/eviction.
				sub.SetFetchConfig(newURL, newInterval)
				sub.SetContent(newContent)
				sub.SetEphemeral(newEphemeral)
				sub.SetIncrementalAliveNodes(newIncrementalAliveNodes)
				sub.SetEphemeralNodeEvictDelayNs(newEphemeralNodeEvictDelay)
				sub.UpdatedAtNs = now
				if urlChanged || contentChanged {
					// A new refresh input starts a new error generation. Do not
					// expose a failure from the superseded source while its
					// replacement refresh is still pending.
					sub.SetLastError("")
				}

				if nameChanged {
					if s.Scheduler != nil {
						s.Scheduler.RenameSubscriptionLocked(sub, newName)
					} else {
						sub.SetName(newName)
					}
				}
				if enabledChanged {
					if s.Scheduler != nil {
						s.Scheduler.SetSubscriptionEnabledLocked(sub, newEnabled)
					} else {
						sub.SetEnabled(newEnabled)
					}
				}
				if hook := s.afterSubscriptionRuntimeMutationHook; hook != nil {
					hook()
				}
				refreshRuntime = (urlChanged || contentChanged) && s.Scheduler != nil
				if updateErr != nil {
					return updateErr
				}
				return nil
			})
		}); err != nil {
			return err
		}
		return opErr
	}
	var err error
	if s.Scheduler != nil {
		err = s.Scheduler.RunMutation(mutate)
	} else {
		err = mutate()
	}
	if err != nil {
		var serviceErr *ServiceError
		if errors.As(err, &serviceErr) {
			return nil, serviceErr
		}
		return nil, internal("update subscription", err)
	}
	if refreshRuntime {
		s.Scheduler.UpdateSubscriptionAsync(sub)
	}

	cfg := sub.SnapshotConfig()
	var r SubscriptionResponse
	s.withRuntimeRead(func() {
		r = s.subToResponseSnapshot(sub, cfg)
	})
	return &r, nil
}

// DeleteSubscription deletes a subscription and evicts its nodes.
func (s *ControlPlaneService) DeleteSubscription(id string) error {
	return s.DeleteSubscriptionContext(context.Background(), id)
}

// DeleteSubscriptionContext deletes a subscription while honoring the
// caller's cancellation before the irreversible persistence transaction.
// After that boundary, the shutdown-owned commit context carries the full
// persistence-to-runtime publication through a client disconnect.
func (s *ControlPlaneService) DeleteSubscriptionContext(ctx context.Context, id string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.Engine == nil {
		return internal("subscription persistence unavailable", errors.New("state engine is nil"))
	}
	if s.SubMgr == nil {
		return internal("subscription runtime unavailable", errors.New("subscription manager is nil"))
	}
	if s.Pool == nil {
		return internal("subscription runtime unavailable", errors.New("node pool is nil"))
	}

	sub := s.SubMgr.Lookup(id)
	if sub == nil {
		return notFound("subscription not found")
	}

	var (
		managedHashes []node.Hash
		deleteErr     error
	)

	// Keep delete atomic across persistence + in-memory runtime state:
	// if DB delete fails, do not mutate runtime subscription/node state. The
	// state admission also spans the runtime callbacks, so shutdown cannot
	// close dirty admission between the DB delete and node cleanup.
	if hook := s.beforeSubscriptionOperationLockHook; hook != nil {
		hook()
	}
	var opErr error
	if err := sub.WithOpLockContext(ctx, func() {
		opErr = s.Engine.WithStateWriteAdmissionContextAndCommit(ctx, func(writeCtx, _ context.Context) error {
			// Re-check under lock in case another goroutine removed it between
			// the initial Lookup and lock acquisition.
			lockedSub := s.SubMgr.Lookup(id)
			if lockedSub != sub {
				deleteErr = notFound("subscription not found")
				return deleteErr
			}

			lockedSub.ManagedNodes().RangeNodes(func(h node.Hash, _ subscription.ManagedNode) bool {
				managedHashes = append(managedHashes, h)
				return true
			})

			if err := s.Engine.DeleteSubscriptionContextAndCommit(writeCtx, id); err != nil {
				if errors.Is(err, state.ErrNotFound) {
					deleteErr = notFound("subscription not found")
				} else {
					deleteErr = internal("delete subscription", err)
				}
				return deleteErr
			}
			if hook := s.afterSubscriptionPersistHook; hook != nil {
				hook()
			}

			// Persist succeeded; now apply in-memory cleanup as one runtime
			// generation so readers cannot combine a partially removed pool with
			// the still-registered subscription.
			s.Pool.WithRuntimeMutation(func() {
				for _, h := range managedHashes {
					s.Pool.RemoveNodeFromSub(h, id)
				}
				s.SubMgr.UnregisterExact(id, sub)
			})
			return deleteErr
		})
	}); err != nil {
		if errors.Is(err, state.ErrStateWriteAdmissionClosed) {
			return internal("delete subscription", err)
		}
		return err
	}
	return opErr
}

// RefreshSubscription triggers an immediate subscription refresh (blocks).
func (s *ControlPlaneService) RefreshSubscription(id string) error {
	return s.RefreshSubscriptionContext(context.Background(), id)
}

// RefreshSubscriptionContext triggers an immediate subscription refresh while
// honoring the caller's cancellation. Scheduler Stop remains an independent
// lifecycle cancellation source.
func (s *ControlPlaneService) RefreshSubscriptionContext(ctx context.Context, id string) error {
	if s == nil || s.SubMgr == nil {
		return internal("subscription runtime unavailable", errors.New("subscription manager is nil"))
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	sub := s.SubMgr.Lookup(id)
	if sub == nil {
		return notFound("subscription not found")
	}
	if s.Scheduler == nil {
		return internal("subscription runtime unavailable", errors.New("subscription scheduler is nil"))
	}
	admitted, refreshErr := s.Scheduler.UpdateSubscriptionContextResult(ctx, sub)
	if !admitted {
		if err := ctx.Err(); err != nil {
			return err
		}
		return internal("refresh subscription", errors.New("subscription scheduler is stopped"))
	}
	if refreshErr != nil {
		// A scheduler error means the refresh did not commit. Preserve a
		// request cancellation for the API caller, but do not inspect ctx again
		// after a nil result: nil is the scheduler's commit boundary, and the
		// request may be canceled by a late persistence/runtime callback after
		// the new generation is already published.
		if err := ctx.Err(); err != nil {
			return err
		}
		return internal("refresh subscription", refreshErr)
	}
	return nil
}

// CleanupSubscriptionCircuitOpenNodes removes problematic nodes from a subscription.
// It marks nodes as evicted (while keeping managed hashes) for nodes currently
// circuit-open, and nodes with no outbound while carrying a non-empty last error.
func (s *ControlPlaneService) CleanupSubscriptionCircuitOpenNodes(id string) (int, error) {
	return s.CleanupSubscriptionCircuitOpenNodesContext(context.Background(), id)
}

// CleanupSubscriptionCircuitOpenNodesContext removes problematic nodes while
// honoring cancellation while waiting for the subscription operation owner.
func (s *ControlPlaneService) CleanupSubscriptionCircuitOpenNodesContext(ctx context.Context, id string) (int, error) {
	return s.cleanupSubscriptionCircuitOpenNodesContextWithHook(ctx, id, nil)
}

// cleanupSubscriptionCircuitOpenNodesWithHook performs cleanup with an optional
// hook between first scan and second confirmation scan. The hook is only used
// by tests to simulate TOCTOU recovery.
func (s *ControlPlaneService) cleanupSubscriptionCircuitOpenNodesWithHook(
	id string,
	betweenScans func(),
) (int, error) {
	return s.cleanupSubscriptionCircuitOpenNodesContextWithHook(context.Background(), id, betweenScans)
}

func (s *ControlPlaneService) cleanupSubscriptionCircuitOpenNodesContextWithHook(
	ctx context.Context,
	id string,
	betweenScans func(),
) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	sub := s.SubMgr.Lookup(id)
	if sub == nil {
		return 0, notFound("subscription not found")
	}
	if s.beforeSubscriptionCleanupLockHook != nil {
		s.beforeSubscriptionCleanupLockHook(id, sub)
	}

	var (
		cleanedCount int
		evicted      []node.Hash
		cleanupErr   error
	)

	runCleanup := func(admission *state.DirtyWriteAdmission) {
		if err := sub.WithOpLockContext(ctx, func() {
			// Re-check under lock in case another goroutine deleted the subscription
			// between lookup and lock acquisition.
			lockedSub := s.SubMgr.Lookup(id)
			if lockedSub != sub {
				cleanupErr = notFound("subscription not found")
				return
			}

			var onSubNodeChanged func(string, node.Hash, bool)
			if admission != nil {
				onSubNodeChanged = func(subID string, h node.Hash, added bool) {
					if added {
						admission.MarkSubscriptionNode(subID, h.Hex())
						return
					}
					admission.MarkSubscriptionNodeDelete(subID, h.Hex())
				}
			}
			cleanedCount, evicted, cleanupErr = topology.CleanupSubscriptionNodesWithConfirmContextNoLock(
				ctx,
				lockedSub,
				s.Pool,
				shouldCleanupSubscriptionNode,
				betweenScans,
				onSubNodeChanged,
				admission,
			)
			if admission != nil {
				for _, h := range evicted {
					admission.MarkSubscriptionNode(id, h.Hex())
				}
			}
		}); err != nil {
			cleanupErr = err
		}
	}
	if s.Engine != nil {
		if !s.Engine.WithDirtyWriteAdmission(runCleanup) {
			return 0, internal("subscription cleanup", state.ErrDirtyWriteAdmissionClosed)
		}
	} else {
		runCleanup(nil)
	}
	if cleanupErr != nil {
		return 0, cleanupErr
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if s.afterSubscriptionCleanupMutationHook != nil {
		s.afterSubscriptionCleanupMutationHook()
	}

	return cleanedCount, nil
}

func shouldCleanupSubscriptionNode(entry *node.NodeEntry) bool {
	if entry == nil {
		return false
	}
	return entry.IsCircuitOpen() || (!entry.HasOutbound() && entry.GetLastError() != "")
}

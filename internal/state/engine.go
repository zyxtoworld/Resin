package state

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/Resinat/Resin/internal/model"
)

// NodeLatencyDirtyKey is the composite key for the node_latency dirty set.
type NodeLatencyDirtyKey = model.NodeLatencyKey

// LeaseDirtyKey is the composite key for the leases dirty set.
type LeaseDirtyKey = model.LeaseKey

// SubscriptionNodeDirtyKey is the composite key for the subscription_nodes dirty set.
type SubscriptionNodeDirtyKey = model.SubscriptionNodeKey

var ErrDirtyWriteAdmissionClosed = errors.New("dirty write admission closed")

// CacheReaders provides callbacks for reading current in-memory values at flush time.
// If a reader returns nil for a key marked OpUpsert, the key is
// treated as a delete (the object was removed between mark and flush).
type CacheReaders struct {
	// WithNodeSnapshot runs the node and subscription-node readers under one
	// caller-owned generation read boundary. Lease readers have their own
	// routing lifecycle owner and are intentionally outside this callback;
	// database I/O belongs outside it as well. Nil keeps the historical
	// behavior for standalone state-engine callers without a runtime owner.
	WithNodeSnapshot     func(func())
	ReadNodeStatic       func(hash string) *model.NodeStatic
	ReadNodeDynamic      func(hash string) *model.NodeDynamic
	ReadNodeLatency      func(key NodeLatencyDirtyKey) *model.NodeLatency
	ReadLease            func(key LeaseDirtyKey) *model.Lease
	ReadSubscriptionNode func(key SubscriptionNodeDirtyKey) *model.SubscriptionNode
}

// StateEngine is the single write entry point for all persistence operations.
// Strong-persist data (config, platforms, subscriptions, rules) goes through
// transactional writes to state.db. Weak-persist data (nodes, leases) is
// marked dirty and batch-flushed to cache.db.
type StateEngine struct {
	*StateRepo
	*CacheRepo
	// flushMu serializes drain/read/commit as one ordering domain. Without it,
	// an older drained snapshot can commit after a newer flush and roll the
	// cache back even though the dirty mark was already consumed.
	flushMu sync.Mutex

	dirtyNodesStatic       *DirtySet[string]
	dirtyNodesDynamic      *DirtySet[string]
	dirtyNodeLatency       *DirtySet[NodeLatencyDirtyKey]
	dirtyLeases            *DirtySet[LeaseDirtyKey]
	dirtySubscriptionNodes *DirtySet[SubscriptionNodeDirtyKey]

	dirtyWriteMu      sync.Mutex
	dirtyWriteCond    *sync.Cond
	dirtyWritesDone   chan struct{}
	dirtyWritesClosed bool
	activeDirtyWrites int
	// beforeDirtyWriteHook is a package-private deterministic test seam. It is
	// called after admission and before the dirty set mutation.
	beforeDirtyWriteHook func()
}

// DirtyWriteAdmission is a synchronous owner for a compound runtime mutation.
// Its Mark methods are valid only while the owning callback is running.
type DirtyWriteAdmission struct {
	engine *StateEngine
}

// newStateEngine creates a StateEngine with the given repos.
func newStateEngine(stateRepo *StateRepo, cacheRepo *CacheRepo) *StateEngine {
	e := &StateEngine{
		StateRepo:              stateRepo,
		CacheRepo:              cacheRepo,
		dirtyNodesStatic:       NewDirtySet[string](),
		dirtyNodesDynamic:      NewDirtySet[string](),
		dirtyNodeLatency:       NewDirtySet[NodeLatencyDirtyKey](),
		dirtyLeases:            NewDirtySet[LeaseDirtyKey](),
		dirtySubscriptionNodes: NewDirtySet[SubscriptionNodeDirtyKey](),
	}
	e.dirtyWritesDone = make(chan struct{})
	close(e.dirtyWritesDone)
	e.dirtyWriteCond = sync.NewCond(&e.dirtyWriteMu)
	return e
}

// --- Weak-persist methods (dirty-mark only) ---

func (e *StateEngine) withDirtyWrite(fn func()) bool {
	if !e.beginDirtyWrite() {
		return false
	}
	defer e.endDirtyWrite()
	if hook := e.beforeDirtyWriteHook; hook != nil {
		hook()
	}
	fn()
	return true
}

func (e *StateEngine) beginDirtyWrite() bool {
	if e == nil {
		return false
	}
	e.dirtyWriteMu.Lock()
	if e.dirtyWritesClosed {
		e.dirtyWriteMu.Unlock()
		return false
	}
	if e.activeDirtyWrites == 0 {
		e.dirtyWritesDone = make(chan struct{})
	}
	e.activeDirtyWrites++
	e.dirtyWriteMu.Unlock()
	return true
}

func (e *StateEngine) endDirtyWrite() {
	e.dirtyWriteMu.Lock()
	e.activeDirtyWrites--
	if e.activeDirtyWrites == 0 {
		close(e.dirtyWritesDone)
		e.dirtyWriteCond.Broadcast()
	}
	e.dirtyWriteMu.Unlock()
}

// WithDirtyWriteAdmission runs fn while the dirty-write admission remains
// active. It is for a runtime mutation that performs several nested Mark*
// calls: shutdown cannot close the admission halfway through the mutation,
// and a closed admission rejects fn before it changes runtime state.
func (e *StateEngine) WithDirtyWriteAdmission(fn func(*DirtyWriteAdmission)) bool {
	if fn == nil || !e.beginDirtyWrite() {
		return false
	}
	defer e.endDirtyWrite()
	fn(&DirtyWriteAdmission{engine: e})
	return true
}

func (a *DirtyWriteAdmission) mark(fn func()) bool {
	if a == nil || a.engine == nil {
		return false
	}
	if hook := a.engine.beforeDirtyWriteHook; hook != nil {
		hook()
	}
	fn()
	return true
}

// MarkSubscriptionNode records an upsert within this compound mutation.
func (a *DirtyWriteAdmission) MarkSubscriptionNode(subID, nodeHash string) bool {
	return a.mark(func() {
		a.engine.dirtySubscriptionNodes.MarkUpsert(SubscriptionNodeDirtyKey{
			SubscriptionID: subID,
			NodeHash:       nodeHash,
		})
	})
}

// MarkSubscriptionNodeDelete records a delete within this compound mutation.
func (a *DirtyWriteAdmission) MarkSubscriptionNodeDelete(subID, nodeHash string) bool {
	return a.mark(func() {
		a.engine.dirtySubscriptionNodes.MarkDelete(SubscriptionNodeDirtyKey{
			SubscriptionID: subID,
			NodeHash:       nodeHash,
		})
	})
}

// MarkNodeStatic records a static-node upsert within this compound mutation.
func (a *DirtyWriteAdmission) MarkNodeStatic(hash string) bool {
	return a.mark(func() { a.engine.dirtyNodesStatic.MarkUpsert(hash) })
}

// MarkNodeDynamic records a dynamic-node upsert within this compound
// mutation.
func (a *DirtyWriteAdmission) MarkNodeDynamic(hash string) bool {
	return a.mark(func() { a.engine.dirtyNodesDynamic.MarkUpsert(hash) })
}

// MarkNodeLatency records a latency upsert within this compound mutation.
func (a *DirtyWriteAdmission) MarkNodeLatency(nodeHash, domain string) bool {
	return a.mark(func() {
		a.engine.dirtyNodeLatency.MarkUpsert(NodeLatencyDirtyKey{
			NodeHash: nodeHash,
			Domain:   domain,
		})
	})
}

// MarkNodeStaticDelete records a static-node deletion within this compound
// mutation.
func (a *DirtyWriteAdmission) MarkNodeStaticDelete(hash string) bool {
	return a.mark(func() { a.engine.dirtyNodesStatic.MarkDelete(hash) })
}

// MarkNodeDynamicDelete records a dynamic-node deletion within this compound
// mutation.
func (a *DirtyWriteAdmission) MarkNodeDynamicDelete(hash string) bool {
	return a.mark(func() { a.engine.dirtyNodesDynamic.MarkDelete(hash) })
}

// MarkNodeLatencyDelete records a node-latency deletion within this compound
// mutation.
func (a *DirtyWriteAdmission) MarkNodeLatencyDelete(nodeHash, domain string) bool {
	return a.mark(func() {
		a.engine.dirtyNodeLatency.MarkDelete(NodeLatencyDirtyKey{
			NodeHash: nodeHash,
			Domain:   domain,
		})
	})
}

// CloseDirtyWriteAdmission rejects future dirty marks and returns without
// waiting for marks that already passed admission. It is used by shutdown
// barriers that must preserve their caller deadline; the stop owner waits for
// active marks before its final cache flush.
func (e *StateEngine) CloseDirtyWriteAdmission() {
	if e == nil {
		return
	}
	e.dirtyWriteMu.Lock()
	e.dirtyWritesClosed = true
	e.dirtyWriteMu.Unlock()
}

func (e *StateEngine) waitForDirtyWrites() {
	e.dirtyWriteMu.Lock()
	for e.activeDirtyWrites != 0 {
		e.dirtyWriteCond.Wait()
	}
	e.dirtyWriteMu.Unlock()
}

// CloseDirtyWriteAdmissionAndWait rejects future dirty marks and waits for
// marks that already passed admission. The final cache flush must run after
// this method so no late mark can appear behind it.
func (e *StateEngine) CloseDirtyWriteAdmissionAndWait() {
	if e == nil {
		return
	}
	e.CloseDirtyWriteAdmission()
	e.waitForDirtyWrites()
}

// lockDirtyWriteBoundary waits for admitted compound mutations to finish and
// returns with dirtyWriteMu held. New mutations cannot pass admission while
// the caller drains all dirty sets, so one compound mutation cannot be split
// across two flush batches. The lock is released before reader or DB I/O.
func (e *StateEngine) lockDirtyWriteBoundary(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		e.dirtyWriteMu.Lock()
		if err := ctx.Err(); err != nil {
			e.dirtyWriteMu.Unlock()
			return err
		}
		if e.activeDirtyWrites == 0 {
			return nil
		}
		done := e.dirtyWritesDone
		e.dirtyWriteMu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// CloseStateWriteAdmissionAndWait rejects new strong state mutations and
// waits for already-admitted ones until ctx expires. It is used at the HTTP
// shutdown barrier before state.db is closed.
func (e *StateEngine) CloseStateWriteAdmissionAndWait(ctx context.Context) error {
	if e == nil || e.StateRepo == nil {
		return nil
	}
	return e.StateRepo.CloseStateWriteAdmissionAndWait(ctx)
}

// WithStateWriteAdmission keeps strong-write admission active for the entire
// compound mutation. The callback may perform the normal StateEngine CRUD
// operation and then update runtime state; shutdown cannot close dirty-write
// admission between those two halves.
func (e *StateEngine) WithStateWriteAdmission(fn func() error) error {
	if e == nil || e.StateRepo == nil {
		return ErrStateWriteAdmissionClosed
	}
	return e.StateRepo.withWrite(func(context.Context) error {
		if fn == nil {
			return nil
		}
		return fn()
	})
}

// WithStateWriteAdmissionContext keeps a strong-write mutation admitted while
// combining the caller context with the shutdown admission context.
func (e *StateEngine) WithStateWriteAdmissionContext(ctx context.Context, fn func(context.Context) error) error {
	if e == nil || e.StateRepo == nil {
		return ErrStateWriteAdmissionClosed
	}
	return e.StateRepo.withWriteContext(ctx, func(writeCtx context.Context) error {
		if fn == nil {
			return nil
		}
		return fn(writeCtx)
	})
}

// waitForStateWrites waits for the already-admitted strong mutation owner.
// Shutdown closes state admission before calling this method so no new
// mutation can pass the empty-set observation.
func (e *StateEngine) waitForStateWrites() {
	if e == nil || e.StateRepo == nil {
		return
	}
	e.StateRepo.waitForWrites()
}

// WaitForStateWrites waits for every strong mutation admitted before the
// state-write admission was closed. Shutdown continuations use this barrier
// before stopping resources whose callbacks can be reached by those
// mutations.
func (e *StateEngine) WaitForStateWrites() {
	e.waitForStateWrites()
}

func (e *StateEngine) MarkNodeStatic(hash string) bool {
	return e.withDirtyWrite(func() { e.dirtyNodesStatic.MarkUpsert(hash) })
}
func (e *StateEngine) MarkNodeStaticDelete(hash string) bool {
	return e.withDirtyWrite(func() { e.dirtyNodesStatic.MarkDelete(hash) })
}
func (e *StateEngine) MarkNodeDynamic(hash string) bool {
	return e.withDirtyWrite(func() { e.dirtyNodesDynamic.MarkUpsert(hash) })
}
func (e *StateEngine) MarkNodeDynamicDelete(hash string) bool {
	return e.withDirtyWrite(func() { e.dirtyNodesDynamic.MarkDelete(hash) })
}

func (e *StateEngine) MarkNodeLatency(nodeHash, domain string) bool {
	return e.withDirtyWrite(func() {
		e.dirtyNodeLatency.MarkUpsert(NodeLatencyDirtyKey{NodeHash: nodeHash, Domain: domain})
	})
}
func (e *StateEngine) MarkNodeLatencyDelete(nodeHash, domain string) bool {
	return e.withDirtyWrite(func() {
		e.dirtyNodeLatency.MarkDelete(NodeLatencyDirtyKey{NodeHash: nodeHash, Domain: domain})
	})
}

func (e *StateEngine) MarkLease(platformID, account string) bool {
	return e.withDirtyWrite(func() {
		e.dirtyLeases.MarkUpsert(LeaseDirtyKey{PlatformID: platformID, Account: account})
	})
}
func (e *StateEngine) MarkLeaseDelete(platformID, account string) bool {
	return e.withDirtyWrite(func() {
		e.dirtyLeases.MarkDelete(LeaseDirtyKey{PlatformID: platformID, Account: account})
	})
}

func (e *StateEngine) MarkSubscriptionNode(subID, nodeHash string) bool {
	return e.withDirtyWrite(func() {
		e.dirtySubscriptionNodes.MarkUpsert(SubscriptionNodeDirtyKey{SubscriptionID: subID, NodeHash: nodeHash})
	})
}
func (e *StateEngine) MarkSubscriptionNodeDelete(subID, nodeHash string) bool {
	return e.withDirtyWrite(func() {
		e.dirtySubscriptionNodes.MarkDelete(SubscriptionNodeDirtyKey{SubscriptionID: subID, NodeHash: nodeHash})
	})
}

// DirtyCount returns the total number of dirty entries across all sets.
func (e *StateEngine) DirtyCount() int {
	return e.dirtyNodesStatic.Len() +
		e.dirtyNodesDynamic.Len() +
		e.dirtyNodeLatency.Len() +
		e.dirtyLeases.Len() +
		e.dirtySubscriptionNodes.Len()
}

// classifyDirtySet splits a drained dirty-set snapshot into upsert values and
// delete keys. For OpUpsert entries, the reader is called to fetch the current
// in-memory value; a nil return is treated as a delete.
func classifyDirtySet[K comparable, V any](
	drained map[K]DirtyOp,
	reader func(K) *V,
) (upserts []V, deletes []K) {
	for key, op := range drained {
		if op == OpDelete {
			deletes = append(deletes, key)
			continue
		}
		v := reader(key)
		if v == nil {
			deletes = append(deletes, key)
		} else {
			upserts = append(upserts, *v)
		}
	}
	return
}

// FlushDirtySets drains all dirty sets, reads current values via readers,
// and batch-writes to cache.db in a single transaction.
// On failure, undrained entries are merged back.
func (e *StateEngine) FlushDirtySets(readers CacheReaders) error {
	return e.FlushDirtySetsContext(context.Background(), readers)
}

// FlushDirtySetsContext flushes all currently dirty cache state using ctx for
// the database transaction. A canceled flush re-merges the drained batch.
func (e *StateEngine) FlushDirtySetsContext(ctx context.Context, readers CacheReaders) error {
	if ctx == nil {
		ctx = context.Background()
	}
	e.flushMu.Lock()
	defer e.flushMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	// Cross-set draining is the cache linearization boundary for compound
	// runtime mutations. Hold only the short dirty-set admission lock here;
	// readers and the SQLite transaction run after it is released.
	if err := e.lockDirtyWriteBoundary(ctx); err != nil {
		return err
	}
	drainedStatic := e.dirtyNodesStatic.Drain()
	drainedSubNodes := e.dirtySubscriptionNodes.Drain()
	drainedDynamic := e.dirtyNodesDynamic.Drain()
	drainedLatency := e.dirtyNodeLatency.Drain()
	drainedLeases := e.dirtyLeases.Drain()
	e.dirtyWriteMu.Unlock()

	// Re-merge helper on failure.
	remerge := func() {
		e.dirtyNodesStatic.Merge(drainedStatic)
		e.dirtySubscriptionNodes.Merge(drainedSubNodes)
		e.dirtyNodesDynamic.Merge(drainedDynamic)
		e.dirtyNodeLatency.Merge(drainedLatency)
		e.dirtyLeases.Merge(drainedLeases)
	}

	// Classify node and subscription dirty sets inside one caller-owned runtime
	// generation read boundary. The callback deliberately ends before SQLite
	// I/O, so a slow or locked cache database cannot block runtime mutation
	// preparation. Lease state is protected by Router's separate lifecycle
	// owner and is classified outside this pool-owned boundary.
	var (
		upsertStatic   []model.NodeStatic
		deleteStatic   []string
		upsertSubNodes []model.SubscriptionNode
		deleteSubNodes []model.SubscriptionNodeKey
		upsertDynamic  []model.NodeDynamic
		deleteDynamic  []string
		upsertLatency  []model.NodeLatency
		deleteLatency  []model.NodeLatencyKey
	)
	classifyNodes := func() {
		upsertStatic, deleteStatic = classifyDirtySet(drainedStatic, readers.ReadNodeStatic)
		upsertSubNodes, deleteSubNodes = classifyDirtySet(drainedSubNodes, readers.ReadSubscriptionNode)
		upsertDynamic, deleteDynamic = classifyDirtySet(drainedDynamic, readers.ReadNodeDynamic)
		upsertLatency, deleteLatency = classifyDirtySet(drainedLatency, readers.ReadNodeLatency)
	}
	if readers.WithNodeSnapshot != nil {
		readers.WithNodeSnapshot(classifyNodes)
	} else {
		classifyNodes()
	}
	upsertLeases, deleteLeases := classifyDirtySet(drainedLeases, readers.ReadLease)

	flushOps := FlushOps{
		UpsertNodesStatic:       upsertStatic,
		DeleteNodesStatic:       deleteStatic,
		UpsertSubscriptionNodes: upsertSubNodes,
		DeleteSubscriptionNodes: deleteSubNodes,
		UpsertNodesDynamic:      upsertDynamic,
		DeleteNodesDynamic:      deleteDynamic,
		UpsertNodeLatency:       upsertLatency,
		DeleteNodeLatency:       deleteLatency,
		UpsertLeases:            upsertLeases,
		DeleteLeases:            deleteLeases,
	}
	var flushErr error
	if ctx.Done() == nil {
		flushErr = e.CacheRepo.FlushTx(flushOps)
	} else {
		flushErr = e.CacheRepo.FlushTxContext(ctx, flushOps)
	}
	if flushErr != nil {
		remerge()
		return fmt.Errorf("flush: %w", flushErr)
	}

	log.Printf("[state] flushed dirty sets: static=%d, sub_nodes=%d, dynamic=%d, latency=%d, leases=%d",
		len(drainedStatic), len(drainedSubNodes), len(drainedDynamic), len(drainedLatency), len(drainedLeases))
	return nil
}

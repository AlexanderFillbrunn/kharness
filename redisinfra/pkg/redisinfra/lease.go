// Package redisinfra holds the distributed halves the agent framework left
// local: the leases that keep two runs off one arc and two compactions off
// one session when they are in two processes, the cancellation notice that
// reaches a run already under way, the queue that delivers the runs a write
// owes an arc, and the event log those processes share.
//
// Everything here talks to Redis by address from configuration. Nothing
// listens, nothing is reachable from outside, and nothing knows about an
// agent beyond the interfaces in pkg/agent it implements.
//
// The three pieces share one stance on failure: the log is the source of
// truth, and every Redis structure here is a hint about it. A lost lease, a
// missed notification or a redelivered item costs a replay or a run that does
// nothing, never a wrong write.
package redisinfra

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"fillbrunn.ai/kube-agent/agent/pkg/agent"
)

// leaseKey names the key one arc's lease lives under. The session and the arc
// ID are both in the key, because nothing that locks by arc can be
// partitioned by session without being told the session as well.
func leaseKey(ref common.ArcRef) string {
	return "kube-agent:lease:" + ref.SessionID + ":" + ref.ArcID
}

// sessionLeaseKey names the key one session's compaction lease lives under.
// The lease is scoped to the session rather than to an arc or an item,
// because a compaction is a fact about the session's log, not about anything
// a work item names.
func sessionLeaseKey(sessionID string) string {
	return "kube-agent:session-lease:" + sessionID
}

// releaseScript deletes the lease only while it still names the holder asking
// for the deletion. A holder whose TTL has expired finds the key naming
// somebody else and deletes nothing, which is what keeps a late release from
// taking the next holder's lease away.
var releaseScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
end
return 0
`)

// renewScript extends the TTL only while the key still names the holder
// asking for the extension. A renewal that cannot prove it still holds the
// lease does nothing, so a heartbeat can never resurrect a lease that
// expired and was taken by somebody else.
var renewScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("pexpire", KEYS[1], ARGV[2])
end
return 0
`)

// keyLease is the machinery both leases share: a holder token under one key
// with a TTL, heartbeated while the holder lives and released only by its
// own token. The TTL is the backstop for a holder that dies without
// releasing, and it is also the honest limit of what the lease promises: the
// lease contracts in pkg/agent have no fencing, so a holder whose TTL
// expired can still be writing, and the persistor cannot detect it. A
// generous TTL makes that a run finishing late rather than a run overlapping
// its successor, which is why the default is minutes and not seconds.
type keyLease struct {
	client *redis.Client
	ttl    time.Duration
	renew  time.Duration

	// logger receives the lease's records: acquisitions and releases at
	// Debug, a lost heartbeat at Warn, unreachable Redis at Error. Set with
	// WithLeaseLogger; nil becomes slog.Default() at build.
	logger *slog.Logger
}

// LeaseOption configures a lease.
type LeaseOption func(*keyLease)

// WithLeaseLogger gives a lease the logger its acquisitions report to. Nil
// means slog.Default().
func WithLeaseLogger(logger *slog.Logger) LeaseOption {
	return func(l *keyLease) { l.logger = logger }
}

// WithLeaseTTL sets how long a lease survives a holder that stops
// heartbeating it. It should stay generous: shrinking it trades a rare
// overlapping run for a common stranded arc.
func WithLeaseTTL(ttl time.Duration) LeaseOption {
	return func(l *keyLease) { l.ttl = ttl }
}

// WithRenewalInterval sets how often a holder extends its lease, or 0 to
// hold for the TTL alone.
func WithRenewalInterval(interval time.Duration) LeaseOption {
	return func(l *keyLease) { l.renew = interval }
}

// newKeyLease builds the shared part of a lease over a Redis client. The
// client is used by every acquisition, so it must not be closed while a
// lease may be taken.
func newKeyLease(client *redis.Client, options ...LeaseOption) keyLease {
	l := keyLease{client: client, ttl: time.Minute, renew: 20 * time.Second}
	for _, option := range options {
		option(&l)
	}
	l.logger = resolveLogger(l.logger).With("component", "lease")
	return l
}

// Acquire takes the lease by writing a holder token under the key, which
// fails without error when somebody else's token is already there.
//
// The three outcomes are the contracts', and they are kept apart exactly:
// ok false means the lease is held and is not a failure, and an error means
// Redis could not be reached and the caller learns nothing about who holds
// it. A caller that read an unreachable lock service as "nobody holds it"
// would run twice, so nothing here turns a reachability failure into a
// refusal.
//
// The release func returned on success stops the heartbeat and then deletes
// the key only if it still names this holder. It runs on its own context, so
// a run cancelled through its own context still lets go of the lease.
func (l *keyLease) Acquire(ctx context.Context, key string) (func(), bool, error) {
	token, err := newLeaseToken()
	if err != nil {
		return nil, false, fmt.Errorf("could not mint a holder token for %s: %w", key, err)
	}
	ok, err := l.client.SetNX(ctx, key, token, l.ttl).Result()
	if err != nil {
		l.logger.ErrorContext(ctx, "could not reach the lease", "key", key, "error", err)
		return nil, false, fmt.Errorf("could not reach the lease on %s: %w", key, err)
	}
	if !ok {
		l.logger.DebugContext(ctx, "lease held by another holder", "key", key)
		return nil, false, nil
	}
	l.logger.DebugContext(ctx, "lease acquired", "key", key, "ttl", l.ttl)
	stopHeartbeat := l.startHeartbeat(ctx, key, token)
	return func() {
		stopHeartbeat()
		// The release runs even when the run's context is done, since a
		// lease held past its holder is held against everybody else for a
		// TTL.
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := releaseScript.Run(releaseCtx, l.client, []string{key}, token).Result(); err != nil {
			// A release that cannot reach Redis leaves the key to expire
			// on its TTL. That is the backstop the TTL exists for, and a
			// caller has nothing left to do about it.
			l.logger.Warn("could not release the lease; the key expires on its TTL", "key", key, "error", err)
		} else {
			l.logger.Debug("lease released", "key", key)
		}
	}, true, nil
}

// startHeartbeat extends the lease on a ticker until it is stopped, and
// returns the stop. The heartbeat holds nothing the run needs: it is a
// goroutine that ends when the lease is released, lost, or its parent
// context cancelled, whichever comes first.
//
// A renewal that cannot reach Redis keeps trying, because a blip in the
// connection is not evidence the lease is gone. A renewal that reaches Redis
// and finds another holder's token is evidence the lease is gone, and the
// heartbeat stops: the run keeps writing, which is the unfenced-writer hole
// the lease contracts already own.
func (l *keyLease) startHeartbeat(ctx context.Context, key, token string) (stop func()) {
	if l.renew <= 0 {
		return func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(l.renew)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				held, err := renewScript.Run(ctx, l.client, []string{key}, token, l.ttl.Milliseconds()).Int()
				if err != nil {
					continue
				}
				if held == 0 {
					l.logger.Warn("lease lost: another holder took the key after this holder's TTL expired; writes are unfenced", "key", key)
					return
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// RedisLease is an ArcLease for many processes, on the same rule as the
// local one: one holder per arc, and an arc left to whoever holds it.
type RedisLease struct {
	keyLease
}

// NewRedisLease builds an arc lease over a Redis client.
func NewRedisLease(client *redis.Client, options ...LeaseOption) *RedisLease {
	return &RedisLease{newKeyLease(client, options...)}
}

var _ agent.ArcLease = (*RedisLease)(nil)

// Acquire takes the arc's lease. See keyLease.Acquire for the outcomes.
func (l *RedisLease) Acquire(ctx context.Context, ref common.ArcRef) (func(), bool, error) {
	return l.keyLease.Acquire(ctx, leaseKey(ref))
}

// RedisSessionLease is a SessionLease for many processes: one holder per
// session's compaction, whatever compaction item they hold, so parallel
// computation of one session's compactions is excluded process-wide. The
// lease's expiry bounds a worker that died computing, and the queue's
// autoclaim re-offers its item, so the lease needs no other reaping.
type RedisSessionLease struct {
	keyLease
}

// NewRedisSessionLease builds a session lease over a Redis client.
func NewRedisSessionLease(client *redis.Client, options ...LeaseOption) *RedisSessionLease {
	return &RedisSessionLease{newKeyLease(client, options...)}
}

var _ agent.SessionLease = (*RedisSessionLease)(nil)

// Acquire takes the session's lease. See keyLease.Acquire for the outcomes.
func (l *RedisSessionLease) Acquire(ctx context.Context, sessionID string) (func(), bool, error) {
	return l.keyLease.Acquire(ctx, sessionLeaseKey(sessionID))
}

// newLeaseToken mints the value a holder writes under the lease key. It is
// random per acquisition, so a release or a renewal can tell its own lease
// from one taken over after its TTL expired.
func newLeaseToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

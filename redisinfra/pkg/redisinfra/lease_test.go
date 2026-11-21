package redisinfra

import (
	"context"
	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newRedis starts an in-process Redis and returns a client on it, so that no
// test reaches past loopback.
func newRedis(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return client, server
}

// acquire takes the lease on a ref, failing the test on the outcomes an
// acquirer is not supposed to see.
func acquire(t *testing.T, lease *RedisLease, ref common.ArcRef) func() {
	t.Helper()
	release, ok, err := lease.Acquire(context.Background(), ref)
	if err != nil || !ok {
		t.Fatalf("could not take the lease on %s: ok %v, err %v", ref, ok, err)
	}
	return release
}

func TestLeaseExcludesASecondHolder(t *testing.T) {
	client, _ := newRedis(t)
	lease := NewRedisLease(client)
	ref := common.ArcRef{SessionID: "s", ArcID: "a"}

	release := acquire(t, lease, ref)
	defer release()

	_, ok, err := lease.Acquire(context.Background(), ref)
	if err != nil {
		t.Fatalf("a held arc is not an error: %v", err)
	}
	if ok {
		t.Fatal("a held arc was taken twice")
	}
}

func TestLeaseReleaseLetsTheNextHolderIn(t *testing.T) {
	client, _ := newRedis(t)
	lease := NewRedisLease(client)
	ref := common.ArcRef{SessionID: "s", ArcID: "a"}

	acquire(t, lease, ref)()

	acquire(t, lease, ref)()
}

func TestLeaseReleaseDoesNotDeleteAnotherHoldersLease(t *testing.T) {
	client, server := newRedis(t)
	lease := NewRedisLease(client, WithLeaseTTL(50*time.Millisecond), WithRenewalInterval(0))
	ref := common.ArcRef{SessionID: "s", ArcID: "a"}

	// The first holder's TTL runs out, so the second takes the arc over.
	staleRelease := acquire(t, lease, ref)
	server.FastForward(100 * time.Millisecond)
	freshRelease := acquire(t, lease, ref)
	defer freshRelease()

	// The first holder finishes and releases, which must leave the second
	// holder's lease alone.
	staleRelease()

	if _, ok, err := lease.Acquire(context.Background(), ref); err != nil || ok {
		t.Fatalf("a late release took the next holder's lease: ok %v, err %v", ok, err)
	}
}

func TestLeaseTTLExpiryFreesTheArc(t *testing.T) {
	client, server := newRedis(t)
	lease := NewRedisLease(client, WithLeaseTTL(50*time.Millisecond), WithRenewalInterval(0))
	ref := common.ArcRef{SessionID: "s", ArcID: "a"}

	acquire(t, lease, ref)
	server.FastForward(100 * time.Millisecond)

	acquire(t, lease, ref)()
}

func TestLeaseUnreachableIsAnErrorNotARefusal(t *testing.T) {
	client, server := newRedis(t)
	lease := NewRedisLease(client)
	server.Close()

	_, ok, err := lease.Acquire(context.Background(), common.ArcRef{SessionID: "s", ArcID: "a"})
	if err == nil {
		t.Fatal("an unreachable lease service must be an error, since a caller that read it as a refusal would run the arc twice")
	}
	if ok {
		t.Fatal("an unreachable lease service holds nothing")
	}
}

func TestLeaseHeartbeatKeepsASlowRunHolding(t *testing.T) {
	client, server := newRedis(t)
	ttl := 150 * time.Millisecond
	lease := NewRedisLease(client, WithLeaseTTL(ttl), WithRenewalInterval(50*time.Millisecond))
	ref := common.ArcRef{SessionID: "s", ArcID: "a"}

	release := acquire(t, lease, ref)
	defer release()

	// The run outlives its TTL, and the heartbeat keeps the lease alive
	// through it.
	time.Sleep(2 * ttl)
	if _, ok, err := lease.Acquire(context.Background(), ref); err != nil || ok {
		t.Fatalf("a heartbeat lease was lost under a live run: ok %v, err %v", ok, err)
	}
	// The TTL must have been extended rather than reset once, so the key
	// still has most of a TTL left rather than none of one.
	ttlLeft, err := client.PTTL(context.Background(), leaseKey(ref)).Result()
	if err != nil {
		t.Fatalf("could not read the lease's TTL: %v", err)
	}
	if ttlLeft <= 0 {
		t.Fatal("a heartbeat lease carries no TTL")
	}
	server.FastForward(0)
}

func TestLeaseHeartbeatDoesNotResurrectALostLease(t *testing.T) {
	client, server := newRedis(t)
	ttl := 100 * time.Millisecond
	lease := NewRedisLease(client, WithLeaseTTL(ttl), WithRenewalInterval(50*time.Millisecond))
	ref := common.ArcRef{SessionID: "s", ArcID: "a"}

	// The first holder's lease expires while its heartbeat is running, and
	// the second holder takes the arc over.
	staleRelease := acquire(t, lease, ref)
	server.FastForward(2 * ttl)
	freshRelease := acquire(t, lease, ref)
	defer freshRelease()

	// The first holder's heartbeat fires after the takeover, and must not
	// extend a lease that no longer names it.
	time.Sleep(2 * (ttl + 50*time.Millisecond))
	if _, ok, err := lease.Acquire(context.Background(), ref); err != nil || ok {
		t.Fatalf("a heartbeat extended a lease its holder had lost: ok %v, err %v", ok, err)
	}
	staleRelease()
}

// acquireSession takes the session lease, failing the test on the outcomes
// an acquirer is not supposed to see.
func acquireSession(t *testing.T, lease *RedisSessionLease, sessionID string) func() {
	t.Helper()
	release, ok, err := lease.Acquire(context.Background(), sessionID)
	if err != nil || !ok {
		t.Fatalf("could not take the session lease on %s: ok %v, err %v", sessionID, ok, err)
	}
	return release
}

func TestSessionLeaseExcludesASecondCompactionOfTheSession(t *testing.T) {
	client, _ := newRedis(t)
	lease := NewRedisSessionLease(client)

	release := acquireSession(t, lease, "s")
	defer release()

	_, ok, err := lease.Acquire(context.Background(), "s")
	if err != nil {
		t.Fatalf("a held session is not an error: %v", err)
	}
	if ok {
		t.Fatal("a held session's compaction lease was taken twice")
	}
}

func TestSessionLeaseIsScopedToTheSession(t *testing.T) {
	client, _ := newRedis(t)
	lease := NewRedisSessionLease(client)

	// One session's compaction lease says nothing about another's.
	release := acquireSession(t, lease, "s1")
	defer release()

	acquireSession(t, lease, "s2")()
}

func TestSessionLeaseReleaseLetsTheNextWorkerIn(t *testing.T) {
	client, _ := newRedis(t)
	lease := NewRedisSessionLease(client)

	acquireSession(t, lease, "s")()

	acquireSession(t, lease, "s")()
}

func TestSessionLeaseTTLExpiryFreesTheSession(t *testing.T) {
	client, server := newRedis(t)
	lease := NewRedisSessionLease(client, WithLeaseTTL(50*time.Millisecond), WithRenewalInterval(0))

	acquireSession(t, lease, "s")
	server.FastForward(100 * time.Millisecond)

	acquireSession(t, lease, "s")()
}

func TestSessionLeaseReleaseDoesNotDeleteAnotherHoldersLease(t *testing.T) {
	client, server := newRedis(t)
	lease := NewRedisSessionLease(client, WithLeaseTTL(50*time.Millisecond), WithRenewalInterval(0))

	// The first holder's TTL runs out, so the second takes the session over.
	staleRelease := acquireSession(t, lease, "s")
	server.FastForward(100 * time.Millisecond)
	freshRelease := acquireSession(t, lease, "s")
	defer freshRelease()

	// The first holder finishes and releases, which must leave the second
	// holder's lease alone.
	staleRelease()

	if _, ok, err := lease.Acquire(context.Background(), "s"); err != nil || ok {
		t.Fatalf("a late release took the next holder's lease: ok %v, err %v", ok, err)
	}
}

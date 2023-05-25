package rimer

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"
)

var (
	defaultPrefix = "timers"
)

// Client is a client for managing timers. It uses several Redis data structures
// and stores them using the following key-naming scheme.
//
// timers:<namespace>:timer:<key>
//
//	The expiring timer itself, you can specify any key and value
//
// timers:<namespace>:registered
//
//	A set of all registered timer keys
//
// timers:<namespace>:_registered_<random number>
//
//	Used temporarily during polling to determine which timers need to be fired
//
// timers:<namespace>:queue
//
//	A list of all timers that need to be fired
type Client struct {
	r      *redis.Client
	Prefix string
}

// New creates a new rimer client that uses the given redis client.
func New(client *redis.Client) *Client {
	return &Client{
		r:      client,
		Prefix: defaultPrefix,
	}
}

// Namespace allows callers to scope timers to a particular namespace. This means
// that timers in this namespace will have the namespace's prefix in Redis, they'll
// also be independent of timers in other namespaces. Polling timers in one namespace
// will not affect timers in another namespace.
func (c *Client) Namespace(ns string) *Namespace {
	return &Namespace{
		name:   ns,
		client: c,
	}
}

type Namespace struct {
	name   string
	client *Client
}

// Poll iterates over all available timers and executes them if they are ready.
func (n *Namespace) Poll(ctx context.Context) error {
	_, err := n.client.r.Pipelined(ctx, func(p redis.Pipeliner) error {
		s1 := n.registeredKey()
		s2, err := n.getRegisteredTempSet(ctx, p)
		if err != nil {
			return err
		}

		// Figure out which timers need to be fired. We do this by finding
		// all the unexpired keys, then adding them to a temporary set, and
		// performing a set difference between the temporary set and the set
		// of registered timers. The result is the set of timers that need
		// to be fired.
		keys, err := n.client.r.Keys(ctx, n.timerKey("*")).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			err = n.client.r.SAdd(ctx, s2, toAny(keys)...).Err()
			if err != nil {
				return err
			}
			keys, err = n.client.r.SDiff(ctx, s1, s2).Result()
			if err != nil {
				return err
			}
			err = n.client.r.Del(ctx, s2).Err()
			if err != nil {
				return err
			}
		} else {
			// If all the timers are expired, then the diff is going to
			// just be any keys in s1.
			keys, err = n.client.r.SMembers(ctx, s1).Result()
			if err != nil {
				return err
			}
		}

		for _, k := range keys {
			err = n.client.r.LPush(ctx, n.queueKey(), k).Err()
			if err != nil {
				return err
			}
			err = n.client.r.SRem(ctx, s1, k).Err()
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// Next returns the next timer that needs to be fired. If there are no timers
// available, this will block until one is available.
func (n *Namespace) Next(ctx context.Context) (key string, err error) {
	_, err = n.client.r.Pipelined(ctx, func(p redis.Pipeliner) error {
		var keys []string
		keys, err = n.client.r.BRPop(ctx, 0, n.queueKey()).Result()
		if err != nil {
			return err
		}
		if len(keys) != 2 {
			return fmt.Errorf("expected 2 keys, got %d", len(keys))
		}
		key = keys[1]
		return err
	})
	return
}

// Create creates a new timer with the given key and duration. The key can be
// any string, and the duration is the amount of time before the timer expires.
// Once the duration has passed, the timer will be returned by Next(...) assuming
// that someone Polls.
func (n *Namespace) Create(ctx context.Context, key string, duration time.Duration) error {
	_, err := n.client.r.Pipelined(ctx, func(p redis.Pipeliner) error {
		err := p.Set(ctx, n.timerKey(key), []byte{}, duration).Err()
		if err != nil {
			return err
		}
		err = p.SAdd(ctx, n.registeredKey(), key).Err()
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

// timerKey returns the redis key for a specific timer.
func (n *Namespace) timerKey(id string) string {
	return n.client.Prefix + ":" + n.name + ":timer:" + id
}

// queueKey returns the redis key for the queue of timers in this namespace.
func (n *Namespace) queueKey() string {
	return n.client.Prefix + ":" + n.name + ":queue"
}

// registeredKey returns the redis key for the set of registered timers in this namespace.
func (n *Namespace) registeredKey() string {
	return n.client.Prefix + ":" + n.name + ":registered"
}

func (n *Namespace) registeredTempKey() string {
	return n.client.Prefix + ":" + n.name + ":_registered_" + strconv.Itoa(int(time.Now().UnixNano()))
}

func (n *Namespace) registeredTempPrefix() string {
	return n.client.Prefix + ":" + n.name + ":_registered_*"
}

func (n *Namespace) getRegisteredTempSet(ctx context.Context, p redis.Pipeliner) (string, error) {
	s2 := n.registeredTempKey()
	exists, err := p.Exists(ctx, s2).Result()
	if err != nil {
		return "", err
	}
	if exists == 1 {
		return "", fmt.Errorf("temporary registered key already exists, try again later")
	}
	return s2, nil
}

// toAny converts a slice of T into a slice of any. SAdd accepts a slice of interface{},
// but passing .SAdd(..., strings...) doesn't work with the type system, so we
// need to convert it to a slice of interface{} first.
func toAny[T any](in []T) []any {
	out := make([]any, len(in))
	for i, v := range in {
		out[i] = v
	}
	return out
}

package rimer

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"strings"
	"testing"
	"time"
)

var ctx = context.Background()

func TestClient(t *testing.T) {
	c, stop := client(t)
	defer stop()

	ns := c.Namespace("foo")

	// Create a timer, assert that the timer has not been fired yet (not in the queue)
	assert.NoError(t, ns.Create(ctx, "foo", time.Second))
	ns.assertKeysLen(t, 1)
	ns.assertRegisteredLen(t, 1)
	ns.assertQueueLen(t, 0)

	// Wait
	time.Sleep(2 * time.Second)
	assert.NoError(t, ns.Poll(ctx))

	// Assert that the timer is in the queue
	ns.assertQueueLen(t, 1)
	ns.assertKeysLen(t, 0)
	ns.assertRegisteredLen(t, 0)
	ns.assertRegisteredTempLen(t, 0)

	// Read the event
	key, err := ns.Next(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "foo", key)

	// Everything should be cleaned up
	ns.assertQueueLen(t, 0)
	ns.assertKeysLen(t, 0)
	ns.assertRegisteredLen(t, 0)
}

func ExampleClient() {
	c := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	timers := New(c)

	ns := timers.Namespace("foo")

	// Start a go-routine to poll for timers
	go func() {
		for {
			err := ns.Poll(context.Background())
			if err != nil {
				panic(err)
			}
			time.Sleep(time.Second)
		}
	}()

	// Start a go-routine to handle any timers that are fired
	go func() {
		for {
			key, err := ns.Next(context.Background())
			if err != nil {
				panic(err)
			}
			fmt.Printf("Timer fired: %s\n", key)
		}
	}()

	// Create a timer
	err := ns.Create(context.Background(), "foo", time.Second)
	if err != nil {
		panic(err)
	}

	// Wait for a second to see the timer fire
	time.Sleep(2 * time.Second)
}

// client returns a new rimer client for testing and a function to stop the
// redis container once we're done.
func client(t *testing.T) (*Client, func()) {
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:latest",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp"),
		},
		Started: true,
	})
	require.NoError(t, err)
	stop := func() {
		require.NoError(t, container.Terminate(ctx))
	}
	ep, err := container.PortEndpoint(ctx, "6379/tcp", "tcp")
	require.NoError(t, err)
	return New(redis.NewClient(&redis.Options{
		Network: "tcp",
		Addr:    strings.Replace(ep, "tcp://", "", 1),
	})), stop
}

func (n *Namespace) assertKeysLen(t *testing.T, len int) {
	keys, err := n.client.r.Keys(ctx, n.timerKey("*")).Result()
	require.NoError(t, err)
	assert.Len(t, keys, len, "unexpected number of keys")
}

func (n *Namespace) assertQueueLen(t *testing.T, len int) {
	count, err := n.client.r.LLen(ctx, n.queueKey()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(len), count, "unexpected queue length")
}

func (n *Namespace) assertRegisteredLen(t *testing.T, len int) {
	count, err := n.client.r.SCard(ctx, n.registeredKey()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(len), count, "unexpected registered length")
}

func (n *Namespace) assertRegisteredTempLen(t *testing.T, len int) {
	keys, err := n.client.r.Keys(ctx, n.registeredTempPrefix()).Result()
	require.NoError(t, err)
	assert.Len(t, keys, len, "unexpected number of registered temp keys")
}

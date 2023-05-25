# rimer
Redis-backed event timers. Redis does not provide a native way of scheduling events to be fired at some time in the future, and manually building something using the data structures that Redis does provide is tedious. This library aims to provide a simple polling-based distributed solution to this problem.

## Features
* Extremely simple API
* Polling can be done from multiple clients at any frequency that you'd like. Applications that require infrequent updates can poll much less frequently.
* Once timers fire, they can be handled by any number of distributed consumers at any time. Queue up hundreds of workers to wait for timers to expire and they'll share the load.

## Example
In the following example, we'll walk through how to use the library in your project. 

```go
// Create a new client
c := rimer.New(client)

// Setup a namespace for our timers
ns := c.Namespace("my-timers")

// Wait for the next timer to come around
key, err := ns.Next(ctx)
if err != nil {
    return err
}
```

In another go-routine, or in another application entirely, make sure to periodically poll the namespace for timers that are ready to fire.
```go
for {
    err := ns.Poll(ctx)
    if err != nil {
        return err
    }
    time.Sleep(time.Minute)
}
```

Once there's something polling in the background, we can start adding timers, and any callers waiting for the next timer will be notified once the timer expires.
```go
err := ns.Create(ctx, "timer-1", time.Hour)
if err != nil {
    return err
}
```

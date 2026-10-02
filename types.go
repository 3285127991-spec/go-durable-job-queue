package durablequeue

import (
	"errors"
	"time"
)

// Status is the lifecycle state of a task.
type Status string

const (
	StatusPending Status = "pending" // waiting to be claimed
	StatusClaimed Status = "claimed" // leased to a worker
	StatusDone    Status = "done"    // acknowledged, finished
	StatusDead    Status = "dead"    // exhausted max attempts, in the dead letter queue
)

// Task is a unit of work. Payload is opaque to the queue.
type Task struct {
	ID          string    `json:"id"`
	Payload     []byte    `json:"payload"`
	Status      Status    `json:"status"`
	Attempts    int       `json:"attempts"`
	MaxAttempts int       `json:"max_attempts"`
	Token       string    `json:"token,omitempty"`
	LeaseExpiry time.Time `json:"lease_expiry,omitempty"`
	NotBefore   time.Time `json:"not_before"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Available reports whether the task can be claimed at time now.
func (t *Task) Available(now time.Time) bool {
	switch t.Status {
	case StatusPending:
		return !t.NotBefore.After(now)
	case StatusClaimed:
		return !t.LeaseExpiry.After(now) // lease expired: reclaimable
	default:
		return false
	}
}

var (
	ErrTaskNotFound  = errors.New("queue: task not found")
	ErrTokenMismatch = errors.New("queue: lease token mismatch")
	ErrLeaseExpired  = errors.New("queue: lease expired")
	ErrNotClaimed    = errors.New("queue: task is not claimed")
	ErrClosed        = errors.New("queue: closed")
)

// Options configures a Queue. Zero values pick sensible defaults.
type Options struct {
	// Dir is the data directory holding the WAL and snapshot files.
	Dir string
	// LeaseDuration is how long a claim holds a task. Default 30s.
	LeaseDuration time.Duration
	// BaseBackoff is the retry delay after the first failed attempt.
	// It doubles per attempt, capped at MaxBackoff. Default 1s.
	BaseBackoff time.Duration
	// MaxBackoff caps the retry delay. Default 30s.
	MaxBackoff time.Duration
	// NoSync disables fsync on every committed record. Default false,
	// meaning every commit is durable. Set only for benchmarks; a crash
	// may lose recent writes.
	NoSync bool
}

// DefaultMaxAttempts is used when Enqueue gets maxAttempts <= 0.
const DefaultMaxAttempts = 3

func (o *Options) withDefaults() {
	if o.LeaseDuration <= 0 {
		o.LeaseDuration = 30 * time.Second
	}
	if o.BaseBackoff <= 0 {
		o.BaseBackoff = time.Second
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = 30 * time.Second
	}
	if o.MaxBackoff < o.BaseBackoff {
		o.MaxBackoff = o.BaseBackoff
	}
}

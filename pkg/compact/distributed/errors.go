// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"

	"github.com/thanos-io/thanos/pkg/compact"
)

// ClassifyError maps an error raised while executing a task onto the outcome
// reported to the manager. The classes mirror the compact package's own error
// taxonomy so the manager can rebuild the error on the other side.
func ClassifyError(err error) (Outcome, string) {
	if err == nil {
		return OutcomeCompleted, ""
	}

	switch {
	case compact.IsHaltError(err):
		return OutcomeFailedHalt, ""
	case compact.IsIssue347Error(err):
		return OutcomeFailedIssue347, issue347Block(err)
	case compact.IsOutOfOrderChunkError(err):
		return OutcomeFailedOOOChunks, outOfOrderBlock(err)
	default:
		// Everything else is worth another attempt. The compactor treats an
		// unclassified error as retriable too.
		return OutcomeFailedRetryable, ""
	}
}

func issue347Block(err error) string {
	var target compact.Issue347Error
	if errors.As(err, &target) {
		return target.Block().String()
	}
	return ""
}

func outOfOrderBlock(err error) string {
	var target compact.OutOfOrderChunksError
	if errors.As(err, &target) {
		return target.Block().String()
	}
	return ""
}

// ReconstructError turns a reported outcome back into an error of the class the
// worker hit, so that BucketCompactor's control loop reacts to a remote failure
// exactly as it would to a local one.
//
// The aborted outcomes deliberately produce no error: the worker threw its work
// away without a data failure, so there is nothing for the compactor to
// react to beyond running the task again.
func ReconstructError(res Result) error {
	msg := res.ErrorMessage
	if msg == "" {
		msg = string(res.Outcome)
	}
	err := errors.New(msg)

	switch res.Outcome {
	case OutcomeCompleted:
		return nil
	case OutcomeFailedHalt:
		return compact.NewHaltError(err)
	case OutcomeFailedIssue347:
		id, parseErr := ulid.Parse(res.OffendingBlock)
		if parseErr != nil {
			// Without a block to repair the issue347 path cannot do anything
			// useful, so fall back to a plain retry.
			return compact.NewRetryError(err)
		}
		return compact.NewIssue347Error(err, id)
	case OutcomeFailedOOOChunks:
		id, parseErr := ulid.Parse(res.OffendingBlock)
		if parseErr != nil {
			return compact.NewRetryError(err)
		}
		return compact.NewOutOfOrderChunksError(err, id)
	case OutcomeAbortedOwnershipLost, OutcomeAbortedStoreUnreachable, OutcomeAbortedWorkerShutdown:
		return nil
	case OutcomeAbandoned:
		// The task is parked; retrying delivers the message once, and the next
		// plan over these sources is deferred rather than re-dispatched.
		return compact.NewRetryError(err)
	default:
		return compact.NewRetryError(err)
	}
}

// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/compact"
)

func TestWorkerExecutionErrorDistinguishesCancellation(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		canceled, acknowledged bool
		aborted                Outcome
		want                   Outcome
	}{
		{"data error", false, true, "", OutcomeFailedHalt},
		{"shutdown", true, true, "", OutcomeAbortedWorkerShutdown},
		{"revoked lease", true, false, "", OutcomeAbortedOwnershipLost},
		{"ownership read canceled on shutdown", true, true, OutcomeAbortedStoreUnreachable, OutcomeAbortedWorkerShutdown},
		{"unreachable journal", false, true, OutcomeAbortedStoreUnreachable, OutcomeAbortedStoreUnreachable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.canceled {
				cancel()
			}
			w := &Worker{}
			result := w.triageExecutionError(ctx, Result{TaskID: "t"}, compact.NewHaltError(context.Canceled), tc.aborted, newAtomicBool(tc.acknowledged))
			testutil.Equals(t, tc.want, result.Outcome)
			testutil.Equals(t, "t", result.TaskID)
		})
	}
}

// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compacttest

import "testing"

// TestStandaloneScenarios runs the generic fault scenarios against the
// standalone compactor: the oracle judging itself under faults. Slow and off
// by default; see SkipUnlessScenarios.
func TestStandaloneScenarios(t *testing.T) {
	RunSuite(t, Suite{})
}

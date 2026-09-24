// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package downsample_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/compacttest"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

// stuckScenarios cover what --downsampling.enable-stuck-blocks adds: blocks
// the compactor can never grow are downsampled although they are below the
// downsampling span, and nothing else is.
func stuckScenarios() []compacttest.Scenario {
	cases := make([]compacttest.Scenario, 0, 2)
	for _, enabled := range []bool{false, true} {
		cases = append(cases, compacttest.Scenario{
			Name: fmt.Sprintf("stuck_blocks_enabled_%t", enabled),
			// Windows 1 and 3 are marked no-compact for index size, so they
			// are final and window 2 is fenced in between them; window 0 sits
			// at the edge of the group and windows 4 and 5 are unfenced, so
			// those stay raw.
			Tenants: []compacttest.TenantSpec{{
				Name: "stuck", Series: 2, Windows: 6, Samples: 20,
				NoCompact: map[int]metadata.NoCompactReason{1: metadata.IndexSizeExceedingNoCompactReason, 3: metadata.IndexSizeExceedingNoCompactReason},
			}},
			Conf: func(c compacttest.NodeConfig) compacttest.NodeConfig {
				c.DedupReplicaLabels = nil
				c.DedupFunc = ""
				c.EnableStuckBlockDownsampling = enabled
				return c
			},
			Run: func(t *testing.T, r *compacttest.Run, want, _ *compacttest.BucketDump) {
				got := compacttest.Converged(t, r, want)

				var spans [][2]int64
				for _, b := range got.Blocks {
					if b.Res == downsample.ResLevel1 {
						spans = append(spans, [2]int64{b.MinT, b.MaxT})
					}
				}
				slices.SortFunc(spans, func(a, b [2]int64) int { return int(a[0] - b[0]) })
				downsampled := make([]string, 0, len(spans))
				for _, s := range spans {
					downsampled = append(downsampled, fmt.Sprintf("[%d,%d)", s[0], s[1]))
				}
				window := compacttest.Window.Milliseconds()
				expected := []string{}
				if enabled {
					expected = []string{
						fmt.Sprintf("[%d,%d)", 1*window, 2*window),
						fmt.Sprintf("[%d,%d)", 2*window, 3*window),
						fmt.Sprintf("[%d,%d)", 3*window, 4*window),
					}
				}
				testutil.Equals(t, expected, downsampled, "only opting in may downsample the marked and fenced windows")
			},
		})
	}
	return cases
}

// TestStuckBlockScenarios runs the stuck-block scenarios against the
// standalone compactor. Slow and off by default; see
// compacttest.SkipUnlessScenarios.
func TestStuckBlockScenarios(t *testing.T) {
	compacttest.RunSuite(t, compacttest.Suite{Extra: true, Scenarios: stuckScenarios()})
}

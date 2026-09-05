// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/oklog/ulid/v2"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

// downsample is the binary's runDownsampling: in manager mode the candidates
// go to workers, otherwise they are downsampled here. The marks go along, so
// the plan can waive the span rule for blocks the compactor is done with.
func (n *node) downsample(ctx context.Context, metas map[ulid.ULID]*metadata.Meta, noCompact map[ulid.ULID]*metadata.NoCompactMark, noDownsample map[ulid.ULID]*metadata.NoDownsampleMark) error {
	if n.sched != nil {
		return DispatchDownsampling(ctx, n.logger, n.bkt, n.sched, metas, noCompact, noDownsample, n.conf.enableStuckBlockDownsampling, 2, metadata.NoneFunc, 1, false)
	}
	candidates, err := downsample.Plan(metas, noCompact, noDownsample, n.conf.enableStuckBlockDownsampling)
	if err != nil {
		return err
	}
	for _, c := range candidates {
		if err := n.downsampleLocally(ctx, c.Meta, c.TargetResolution); err != nil {
			return err
		}
	}
	return nil
}

// stuckScenarios cover what this branch adds: blocks the compactor can never
// grow are downsampled although they are below the downsampling span.
func stuckScenarios() []scenario {
	var cases []scenario
	for _, enabled := range []bool{false, true} {
		cases = append(cases, scenario{
			name: fmt.Sprintf("stuck_blocks_enabled_%t", enabled),
			// Windows 1 and 3 are marked no-compact for index size, so they
			// are final and window 2 is fenced in between them; window 0 sits
			// at the edge of the group and windows 4 and 5 are unfenced, so
			// those stay raw.
			tenants: []tenantSpec{{
				name: "stuck", series: 2, windows: 6, samples: 20,
				noCompact: map[int]metadata.NoCompactReason{1: metadata.IndexSizeExceedingNoCompactReason, 3: metadata.IndexSizeExceedingNoCompactReason},
			}},
			conf: func(c nodeConfig) nodeConfig {
				c.dedupReplicaLabels = nil
				c.dedupFunc = ""
				c.enableStuckBlockDownsampling = enabled
				return c
			},
			run: func(t *testing.T, s *scenarioRun, want, _ *bucketDump) {
				s.startWorker("w1")
				s.startWorker("w2")
				got := converged(t, s, want)

				var spans [][2]int64
				for _, b := range got.blocks {
					if b.res == downsample.ResLevel1 {
						spans = append(spans, [2]int64{b.mint, b.maxt})
					}
				}
				// The dump orders blocks by ULID, which two racing workers mint
				// in either order; the claim is about windows, not about who won.
				slices.SortFunc(spans, func(a, b [2]int64) int { return int(a[0] - b[0]) })
				var downsampled []string
				for _, s := range spans {
					downsampled = append(downsampled, fmt.Sprintf("[%d,%d)", s[0], s[1]))
				}
				window := scnWindow.Milliseconds()
				var expected []string
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

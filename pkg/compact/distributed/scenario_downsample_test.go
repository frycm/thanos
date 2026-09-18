// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"

	"github.com/oklog/ulid/v2"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/compacttest"
)

// downsample is the manager's half of the binary's runDownsampling: the
// candidates go to workers. The marks go along, so the plan can waive the span
// rule for blocks the compactor is done with.
func (n *node) downsample(ctx context.Context, cn *compacttest.Node, metas map[ulid.ULID]*metadata.Meta, noCompact map[ulid.ULID]*metadata.NoCompactMark, noDownsample map[ulid.ULID]*metadata.NoDownsampleMark) error {
	return DispatchDownsampling(ctx, cn.Logger, cn.Bkt, n.sched, metas, noCompact, noDownsample, n.conf.EnableStuckBlockDownsampling, 2, metadata.NoneFunc, 1, false, n.downsamples, n.downsampleFailures)
}

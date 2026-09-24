// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"

	"github.com/oklog/ulid/v2"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/compacttest"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

// downsample is the manager's half of the binary's downsampling pass: the
// candidates go to workers, planned as the binary plans them.
func (n *node) downsample(ctx context.Context, cn *compacttest.Node, metas map[ulid.ULID]*metadata.Meta, opts downsample.PlanOptions) error {
	return DispatchDownsampling(ctx, cn.Logger, cn.Bkt, n.sched, metas, opts, 2, metadata.NoneFunc, 1, false, n.downsamples, n.downsampleFailures)
}

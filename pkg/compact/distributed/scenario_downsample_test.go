// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"maps"

	"github.com/oklog/ulid/v2"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/compacttest"
)

// downsample is the manager's half of the binary's runDownsampling: the
// candidates go to workers. Blocks marked no-downsample are taken out of the
// view first, as the binary does.
func (n *node) downsample(ctx context.Context, cn *compacttest.Node, metas map[ulid.ULID]*metadata.Meta, _ map[ulid.ULID]*metadata.NoCompactMark, noDownsample map[ulid.ULID]*metadata.NoDownsampleMark) error {
	metas = maps.Clone(metas)
	for id := range noDownsample {
		delete(metas, id)
	}
	return DispatchDownsampling(ctx, cn.Logger, cn.Bkt, n.sched, metas, 2, metadata.NoneFunc, 1, false, n.downsamples, n.downsampleFailures)
}

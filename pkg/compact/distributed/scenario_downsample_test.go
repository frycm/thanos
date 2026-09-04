// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"maps"

	"github.com/oklog/ulid/v2"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

// downsample is the binary's runDownsampling: in manager mode the candidates
// go to workers, otherwise they are downsampled here. Blocks marked
// no-downsample are taken out of the view first, as the binary does.
func (n *node) downsample(ctx context.Context, metas map[ulid.ULID]*metadata.Meta, _ map[ulid.ULID]*metadata.NoCompactMark, noDownsample map[ulid.ULID]*metadata.NoDownsampleMark) error {
	metas = maps.Clone(metas)
	for id := range noDownsample {
		delete(metas, id)
	}
	if n.sched != nil {
		return DispatchDownsampling(ctx, n.logger, n.bkt, n.sched, metas, 2, metadata.NoneFunc, 1, false)
	}
	candidates, err := downsample.Plan(metas)
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

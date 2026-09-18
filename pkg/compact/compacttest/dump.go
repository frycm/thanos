// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compacttest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/errors"
	"github.com/thanos-io/thanos/pkg/logutil"
)

// Sample is one raw or aggregated sample; histograms are compared by their
// printed form.
type Sample struct {
	T int64
	V float64
	H string
}

// AggrRaw keys the samples of raw blocks; downsampled blocks are keyed by
// aggregate type.
const AggrRaw = downsample.AggrType(math.MaxUint8)

// AggrTypes are the aggregates read from downsampled blocks.
var AggrTypes = []downsample.AggrType{downsample.AggrCount, downsample.AggrSum, downsample.AggrMin, downsample.AggrMax, downsample.AggrCounter}

// ServedBlock is one block a store gateway would load from the bucket. Ext
// keeps the compactor's shard label, so overlaps are judged per shard.
type ServedBlock struct {
	ID      ulid.ULID
	Ext     string
	Res     int64
	MinT    int64
	MaxT    int64
	Sources []string
	// Meta is the block's metadata as fetched, for checks on fields the dump
	// does not interpret, such as extensions.
	Meta *metadata.Meta
}

// BucketDump is what a store gateway would serve from a bucket: the blocks
// that carry no deletion mark, deduplicated the way the store's fetcher does,
// and every series and sample in them keyed by resolution, external labels
// and series labels.
type BucketDump struct {
	Series map[string]map[downsample.AggrType][]Sample
	Blocks []ServedBlock
}

// NewBucketDump returns an empty dump to be filled with ReadBlock.
func NewBucketDump() *BucketDump {
	return &BucketDump{Series: map[string]map[downsample.AggrType][]Sample{}}
}

// DumpBucket reads every served block of the bucket.
func DumpBucket(t *testing.T, bkt objstore.Bucket) *BucketDump {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	logger := log.NewNopLogger()
	insBkt := objstore.WithNoopInstr(bkt)

	base, err := block.NewBaseFetcher(logger, 4, insBkt, block.NewConcurrentLister(logger, insBkt), "", prometheus.NewRegistry())
	testutil.Ok(t, err)
	fetcher := base.NewMetaFetcher(prometheus.NewRegistry(), []block.MetadataFilter{
		block.NewIgnoreDeletionMarkFilter(logger, insBkt, 0, 4),
		block.NewDeduplicateFilter(4),
	})
	metas, _, err := fetcher.Fetch(ctx)
	testutil.Ok(t, err)

	d := NewBucketDump()
	dir := t.TempDir()
	for id, m := range metas {
		sb := ServedBlock{ID: id, Ext: labels.FromMap(m.Thanos.Labels).String(), Res: m.Thanos.Downsample.Resolution, MinT: m.MinTime, MaxT: m.MaxTime, Meta: m}
		for _, s := range m.Compaction.Sources {
			sb.Sources = append(sb.Sources, s.String())
		}
		slices.Sort(sb.Sources)
		d.Blocks = append(d.Blocks, sb)
		d.ReadBlock(t, ctx, bkt, dir, m)
	}
	slices.SortFunc(d.Blocks, func(a, b ServedBlock) int { return a.ID.Compare(b.ID) })
	d.sort()
	return d
}

func (d *BucketDump) sort() {
	for _, byAggr := range d.Series {
		for _, samples := range byAggr {
			slices.SortFunc(samples, func(a, b Sample) int {
				if a.T != b.T {
					return int(a.T - b.T)
				}
				switch {
				case a.V < b.V:
					return -1
				case a.V > b.V:
					return 1
				}
				return strings.Compare(a.H, b.H)
			})
		}
	}
}

// ReadBlock adds every series and sample of the block to the dump. Callers
// building a dump by hand must call Sort once they are done.
func (d *BucketDump) ReadBlock(t *testing.T, ctx context.Context, bkt objstore.Bucket, dir string, m *metadata.Meta) {
	t.Helper()
	logger := log.NewNopLogger()
	bdir := filepath.Join(dir, m.ULID.String())
	if err := block.Download(ctx, logger, bkt, m.ULID, bdir); err != nil {
		if bkt.IsObjNotFoundErr(errors.Cause(err)) || errors.Is(err, os.ErrNotExist) || strings.Contains(err.Error(), "not found") {
			// The block went away between listing and reading: something is
			// deleting it, as garbage collection or a manager's maintenance
			// does, and a store gateway would stop serving it a moment
			// later. What it held is judged by what remains.
			t.Logf("block %s disappeared while being read; skipping it", m.ULID)
			return
		}
		testutil.Ok(t, err)
	}
	b, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(logger), bdir, downsample.NewPool(), nil)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, b.Close()) }()

	indexr, err := b.Index()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, indexr.Close()) }()
	chunkr, err := b.Chunks()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, chunkr.Close()) }()

	k, v := index.AllPostingsKey()
	all, err := indexr.Postings(ctx, k, v)
	testutil.Ok(t, err)
	// Keyed as a store gateway serves it: without the compactor's shard label,
	// so that a split range and an unsplit one compare equal.
	ext := labels.NewBuilder(labels.FromMap(m.Thanos.Labels)).Del(metadata.CompactorShardLabel).Labels().String()
	for all.Next() {
		var builder labels.ScratchBuilder
		var chks []chunks.Meta
		testutil.Ok(t, indexr.Series(all.At(), &builder, &chks))
		key := fmt.Sprintf("res=%d ext=%s series=%s", m.Thanos.Downsample.Resolution, ext, builder.Labels().String())
		byAggr := d.Series[key]
		if byAggr == nil {
			byAggr = map[downsample.AggrType][]Sample{}
			d.Series[key] = byAggr
		}
		for _, c := range chks {
			chk, _, err := chunkr.ChunkOrIterable(c)
			testutil.Ok(t, err)
			if m.Thanos.Downsample.Resolution == 0 {
				byAggr[AggrRaw] = append(byAggr[AggrRaw], chunkSamples(t, chk)...)
				continue
			}
			ac, ok := chk.(*downsample.AggrChunk)
			testutil.Assert(t, ok, "block %s at resolution %d holds a %T, not an aggregate chunk", m.ULID, m.Thanos.Downsample.Resolution, chk)
			for _, at := range AggrTypes {
				sub, err := ac.Get(at)
				if err != nil {
					continue // Not every aggregate is present for every series.
				}
				byAggr[at] = append(byAggr[at], chunkSamples(t, sub)...)
			}
		}
	}
	testutil.Ok(t, all.Err())
}

// Sort orders the samples of a dump built with ReadBlock.
func (d *BucketDump) Sort() { d.sort() }

func chunkSamples(t *testing.T, chk chunkenc.Chunk) []Sample {
	t.Helper()
	var out []Sample
	it := chk.Iterator(nil)
	for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
		switch vt {
		case chunkenc.ValFloat:
			ts, v := it.At()
			out = append(out, Sample{T: ts, V: v})
		case chunkenc.ValHistogram:
			ts, h := it.AtHistogram(nil)
			out = append(out, Sample{T: ts, H: fmt.Sprintf("%#v", h)})
		case chunkenc.ValFloatHistogram:
			ts, h := it.AtFloatHistogram(nil)
			out = append(out, Sample{T: ts, H: fmt.Sprintf("%#v", h)})
		default:
			t.Fatalf("unexpected sample type %v", vt)
		}
	}
	testutil.Ok(t, it.Err())
	return out
}

// AssertSameContent fails with the first differences between two dumps.
func AssertSameContent(t *testing.T, want, got *BucketDump, what string) {
	t.Helper()
	if !SameContent(t, want, got, what) {
		t.FailNow()
	}
}

// SameContent reports the first differences between two dumps and whether
// there were any.
func SameContent(t *testing.T, want, got *BucketDump, what string) bool {
	t.Helper()
	keys := map[string]struct{}{}
	for k := range want.Series {
		keys[k] = struct{}{}
	}
	for k := range got.Series {
		keys[k] = struct{}{}
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	var diffs []string
	for _, k := range sorted {
		w, g := want.Series[k], got.Series[k]
		switch {
		case w == nil:
			diffs = append(diffs, fmt.Sprintf("unexpected %s", k))
			continue
		case g == nil:
			diffs = append(diffs, fmt.Sprintf("missing %s", k))
			continue
		}
		for _, at := range append([]downsample.AggrType{AggrRaw}, AggrTypes...) {
			ws, gs := w[at], g[at]
			if len(ws) != len(gs) {
				diffs = append(diffs, fmt.Sprintf("%s aggr=%d: want %d samples, got %d", k, at, len(ws), len(gs)))
				continue
			}
			for i := range ws {
				if ws[i].T != gs[i].T || math.Float64bits(ws[i].V) != math.Float64bits(gs[i].V) || ws[i].H != gs[i].H {
					diffs = append(diffs, fmt.Sprintf("%s aggr=%d: sample %d want %+v, got %+v", k, at, i, ws[i], gs[i]))
					break
				}
			}
		}
	}
	if len(diffs) > 0 {
		if len(diffs) > 12 {
			diffs = append(diffs[:12], fmt.Sprintf("... %d more", len(diffs)-12))
		}
		t.Logf("expected layout:\n  %s\ngot layout:\n  %s", strings.Join(want.Layout(), "\n  "), strings.Join(got.Layout(), "\n  "))
		t.Errorf("%s: bucket content differs:\n  %s", what, strings.Join(diffs, "\n  "))
		return false
	}
	if len(got.Series) == 0 {
		t.Errorf("%s: the dump is empty, the oracle proves nothing", what)
		return false
	}
	return true
}

// Layout describes the served blocks by what they are made of, not by ID.
func (d *BucketDump) Layout() []string {
	var out []string
	for _, b := range d.Blocks {
		out = append(out, fmt.Sprintf("res=%d ext=%s [%d,%d) sources=%v", b.Res, b.Ext, b.MinT, b.MaxT, b.Sources))
	}
	slices.Sort(out)
	return out
}

// ServedIDs returns the IDs of the served blocks, sorted.
func (d *BucketDump) ServedIDs() []ulid.ULID {
	ids := make([]ulid.ULID, 0, len(d.Blocks))
	for _, b := range d.Blocks {
		ids = append(ids, b.ID)
	}
	return ids
}

// Without returns the dump restricted to blocks whose external labels do not
// match the predicate, for judging the untouched part of a bucket after a
// scenario damaged one group.
func (d *BucketDump) Without(ext func(string) bool) *BucketDump {
	out := NewBucketDump()
	for _, b := range d.Blocks {
		if !ext(b.Ext) {
			out.Blocks = append(out.Blocks, b)
		}
	}
	for k, v := range d.Series {
		e := strings.SplitN(strings.TrimPrefix(k[strings.Index(k, " ext="):], " ext="), " series=", 2)[0]
		if !ext(e) {
			out.Series[k] = v
		}
	}
	return out
}

// AssertNoOverlaps checks that within one external label set and resolution
// the served blocks do not overlap in time, which is what a double execution
// or a bracketing plan would produce.
func (d *BucketDump) AssertNoOverlaps(t *testing.T) {
	t.Helper()
	groups := map[string][]ServedBlock{}
	for _, b := range d.Blocks {
		k := fmt.Sprintf("%s/%d", b.Ext, b.Res)
		groups[k] = append(groups[k], b)
	}
	for k, bs := range groups {
		slices.SortFunc(bs, func(a, b ServedBlock) int { return int(a.MinT - b.MinT) })
		for i := 1; i < len(bs); i++ {
			if bs[i].MinT < bs[i-1].MaxT {
				t.Errorf("group %s: served blocks %s [%d,%d) and %s [%d,%d) overlap", k,
					bs[i-1].ID, bs[i-1].MinT, bs[i-1].MaxT, bs[i].ID, bs[i].MinT, bs[i].MaxT)
			}
		}
	}
}

// AssertNoDeletionMarks fails if any block in the bucket is marked for deletion.
func AssertNoDeletionMarks(t *testing.T, bkt objstore.Bucket) {
	t.Helper()
	testutil.Ok(t, bkt.Iter(context.Background(), "", func(name string) error {
		if strings.HasSuffix(name, metadata.DeletionMarkFilename) {
			t.Errorf("unexpected deletion mark %s", name)
		}
		return nil
	}, objstore.WithRecursiveIter()))
}

// BlockIDs returns the sorted IDs of every block with a meta.json in the bucket.
func BlockIDs(t *testing.T, bkt objstore.Bucket) []ulid.ULID {
	t.Helper()
	var ids []ulid.ULID
	testutil.Ok(t, bkt.Iter(context.Background(), "", func(name string) error {
		if !strings.HasSuffix(name, "/"+block.MetaFilename) {
			return nil
		}
		id, err := ulid.Parse(strings.TrimSuffix(name, "/"+block.MetaFilename))
		if err == nil {
			ids = append(ids, id)
		}
		return nil
	}, objstore.WithRecursiveIter()))
	slices.SortFunc(ids, func(a, b ulid.ULID) int { return a.Compare(b) })
	return ids
}

// Exists reports whether the object exists.
func Exists(t *testing.T, bkt objstore.Bucket, name string) bool {
	t.Helper()
	ok, err := bkt.Exists(context.Background(), name)
	testutil.Ok(t, err)
	return ok
}

// SourceObjects snapshots every object under the given block directories,
// except deletion marks, which a compactor legitimately adds to a source.
func SourceObjects(t *testing.T, bkt objstore.Bucket, ids []ulid.ULID) map[string][]byte {
	t.Helper()
	objects := map[string][]byte{}
	for _, id := range ids {
		testutil.Ok(t, bkt.Iter(context.Background(), id.String()+"/", func(name string) error {
			if strings.HasSuffix(name, metadata.DeletionMarkFilename) {
				return nil
			}
			r, err := bkt.Get(context.Background(), name)
			testutil.Ok(t, err)
			defer r.Close()
			objects[name], err = io.ReadAll(r)
			return err
		}, objstore.WithRecursiveIter()))
	}
	return objects
}

// Fingerprint summarizes the bucket: every object name, and the content of
// every metadata and marker file. Two identical fingerprints around a pass
// mean the pass changed nothing.
func Fingerprint(bkt objstore.Bucket) string {
	h := sha256.New()
	ctx := context.Background()
	_ = bkt.Iter(ctx, "", func(name string) error {
		_, _ = h.Write([]byte(name))
		if strings.HasSuffix(name, ".json") {
			rc, err := bkt.Get(ctx, name)
			if err != nil {
				return nil
			}
			defer rc.Close()
			buf := make([]byte, 64<<10)
			for {
				k, err := rc.Read(buf)
				_, _ = h.Write(buf[:k])
				if err != nil {
					break
				}
			}
		}
		return nil
	}, objstore.WithRecursiveIter())
	return hex.EncodeToString(h.Sum(nil))
}

// CounterTotal sums a counter over all its label values.
func CounterTotal(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gather()
	testutil.Ok(t, err)
	var total float64
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			total += m.GetCounter().GetValue()
		}
	}
	return total
}

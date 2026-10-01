// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package dedup

import (
	"bytes"
	"cmp"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

func TestDedupChunkSeriesMerger(t *testing.T) {
	m := NewChunkSeriesMerger()

	for _, tc := range []struct {
		name     string
		input    []storage.ChunkSeries
		expected storage.ChunkSeries
	}{
		{
			name: "single empty series",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), nil),
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), nil),
		},
		{
			name: "single series",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{1, 1}, sample{2, 2}}, []chunks.Sample{sample{3, 3}}),
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{1, 1}, sample{2, 2}}, []chunks.Sample{sample{3, 3}}),
		},
		{
			name: "two empty series",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), nil),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), nil),
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), nil),
		},
		{
			name: "two non overlapping",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{1, 1}, sample{2, 2}}, []chunks.Sample{sample{3, 3}, sample{5, 5}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{7, 7}, sample{9, 9}}, []chunks.Sample{sample{10, 10}}),
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{1, 1}, sample{2, 2}}, []chunks.Sample{sample{3, 3}, sample{5, 5}}, []chunks.Sample{sample{7, 7}, sample{9, 9}}, []chunks.Sample{sample{10, 10}}),
		},
		{
			name: "two overlapping",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{1, 1}, sample{2, 2}}, []chunks.Sample{sample{3, 3}, sample{8, 8}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{7, 7}, sample{9, 9}}, []chunks.Sample{sample{10, 10}}),
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{1, 1}, sample{2, 2}}, []chunks.Sample{sample{3, 3}, sample{8, 8}}, []chunks.Sample{sample{10, 10}}),
		},
		{
			name: "two overlapping with large time diff",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{1, 1}, sample{2, 2}}, []chunks.Sample{sample{2, 2}, sample{5008, 5008}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{7, 7}, sample{9, 9}}, []chunks.Sample{sample{10, 10}}),
			},
			// sample{5008, 5008} is added to the result due to its large timestamp.
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{1, 1}, sample{2, 2}, sample{5008, 5008}}),
		},
		{
			name: "two duplicated",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{1, 1}, sample{2, 2}, sample{3, 3}, sample{5, 5}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{2, 2}, sample{3, 3}, sample{5, 5}}),
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{1, 1}, sample{2, 2}, sample{3, 3}, sample{5, 5}}),
		},
		{
			name: "three overlapping",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{1, 1}, sample{2, 2}, sample{3, 3}, sample{5, 5}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{2, 2}, sample{3, 3}, sample{6, 6}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{0, 0}, sample{4, 4}}),
			},
			// only samples from the last series are retained due to high penalty.
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{0, 0}, sample{4, 4}}),
		},
		{
			name: "three in chained overlap",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{1, 1}, sample{2, 2}, sample{3, 3}, sample{5, 5}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{4, 4}, sample{6, 66}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{6, 6}, sample{10, 10}}),
			},
			// only samples from the last series are retained due to high penalty.
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{1, 1}, sample{2, 2}, sample{3, 3}, sample{5, 5}}),
		},
		{
			name: "three in chained overlap complex",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{0, 0}, sample{5, 5}}, []chunks.Sample{sample{10, 10}, sample{15, 15}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{2, 2}, sample{20, 20}}, []chunks.Sample{sample{25, 25}, sample{30, 30}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{18, 18}, sample{26, 26}}, []chunks.Sample{sample{31, 31}, sample{35, 35}}),
			},
			// The second chunk of the first series continues the first one rather than being
			// another replica, so it is kept. The last chunk overlaps nothing and is kept as is.
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"),
				[]chunks.Sample{sample{0, 0}, sample{5, 5}, sample{10, 10}, sample{15, 15}},
				[]chunks.Sample{sample{31, 31}, sample{35, 35}},
			),
		},
		{
			name: "110 overlapping samples",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), chunks.GenerateSamples(0, 110)), // [0 - 110)
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), chunks.GenerateSamples(60, 50)), // [60 - 110)
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"),
				chunks.GenerateSamples(0, 110),
			),
		},
		{
			name: "150 overlapping samples, no chunk splitting due to penalty deduplication",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), chunks.GenerateSamples(0, 90)),  // [0 - 90)
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), chunks.GenerateSamples(60, 90)), // [90 - 150)
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"),
				chunks.GenerateSamples(0, 90),
			),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			merged := m(tc.input...)
			testutil.Equals(t, tc.expected.Labels(), merged.Labels())
			actChks, actErr := storage.ExpandChunks(merged.Iterator(nil))
			expChks, expErr := storage.ExpandChunks(tc.expected.Iterator(nil))

			testutil.Equals(t, expErr, actErr)
			testutil.Equals(t, expChks, actChks)
		})
	}
}

func TestDedupChunkSeriesMergerDownsampledChunks(t *testing.T) {
	m := NewChunkSeriesMerger()

	defaultLabels := labels.FromStrings("bar", "baz")
	emptySamples := downsample.SamplesFromTSDBSamples([]chunks.Sample{})
	// Samples are created with step 1m. So the 5m downsampled chunk has 2 samples.
	samples1 := downsample.SamplesFromTSDBSamples(createSamplesWithStep(0, 10, 60*1000))
	// Non overlapping samples with samples1. 5m downsampled chunk has 2 samples.
	samples2 := downsample.SamplesFromTSDBSamples(createSamplesWithStep(600000, 10, 60*1000))
	// Overlapped with samples1.
	samples3 := downsample.SamplesFromTSDBSamples(createSamplesWithStep(120000, 10, 60*1000))

	for _, tc := range []struct {
		name     string
		input    []storage.ChunkSeries
		expected storage.ChunkSeries
	}{
		{
			name: "single empty series",
			input: []storage.ChunkSeries{
				&storage.ChunkSeriesEntry{
					Lset: defaultLabels,
					ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
						return storage.NewListChunkSeriesIterator(downsample.DownsampleRaw(emptySamples, downsample.ResLevel1)...)
					},
				},
			},
			expected: &storage.ChunkSeriesEntry{
				Lset: defaultLabels,
				ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
					return storage.NewListChunkSeriesIterator()
				},
			},
		},
		{
			name: "single series",
			input: []storage.ChunkSeries{
				&storage.ChunkSeriesEntry{
					Lset: defaultLabels,
					ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
						return storage.NewListChunkSeriesIterator(downsample.DownsampleRaw(samples1, downsample.ResLevel1)...)
					},
				},
			},
			expected: &storage.ChunkSeriesEntry{
				Lset: defaultLabels,
				ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
					return storage.NewListChunkSeriesIterator(downsample.DownsampleRaw(samples1, downsample.ResLevel1)...)
				},
			},
		},
		{
			name: "two empty series",
			input: []storage.ChunkSeries{
				&storage.ChunkSeriesEntry{
					Lset: defaultLabels,
					ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
						return storage.NewListChunkSeriesIterator(downsample.DownsampleRaw(emptySamples, downsample.ResLevel1)...)
					},
				},
				&storage.ChunkSeriesEntry{
					Lset: defaultLabels,
					ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
						return storage.NewListChunkSeriesIterator(downsample.DownsampleRaw(emptySamples, downsample.ResLevel1)...)
					},
				},
			},
			expected: &storage.ChunkSeriesEntry{
				Lset: defaultLabels,
				ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
					return storage.NewListChunkSeriesIterator()
				},
			},
		},
		{
			name: "two non overlapping series",
			input: []storage.ChunkSeries{
				&storage.ChunkSeriesEntry{
					Lset: defaultLabels,
					ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
						return storage.NewListChunkSeriesIterator(downsample.DownsampleRaw(samples1, downsample.ResLevel1)...)
					},
				},
				&storage.ChunkSeriesEntry{
					Lset: defaultLabels,
					ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
						return storage.NewListChunkSeriesIterator(downsample.DownsampleRaw(samples2, downsample.ResLevel1)...)
					},
				},
			},
			expected: &storage.ChunkSeriesEntry{
				Lset: defaultLabels,
				ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
					return storage.NewListChunkSeriesIterator(
						append(downsample.DownsampleRaw(samples1, downsample.ResLevel1),
							downsample.DownsampleRaw(samples2, downsample.ResLevel1)...)...)
				},
			},
		},
		{
			// 1:1 duplicated chunks are deduplicated.
			name: "two same series",
			input: []storage.ChunkSeries{
				&storage.ChunkSeriesEntry{
					Lset: defaultLabels,
					ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
						return storage.NewListChunkSeriesIterator(downsample.DownsampleRaw(samples1, downsample.ResLevel1)...)
					},
				},
				&storage.ChunkSeriesEntry{
					Lset: defaultLabels,
					ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
						return storage.NewListChunkSeriesIterator(downsample.DownsampleRaw(samples1, downsample.ResLevel1)...)
					},
				},
			},
			expected: &storage.ChunkSeriesEntry{
				Lset: defaultLabels,
				ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
					return storage.NewListChunkSeriesIterator(
						downsample.DownsampleRaw(samples1, downsample.ResLevel1)...)
				},
			},
		},
		{
			name: "two overlapping series",
			input: []storage.ChunkSeries{
				&storage.ChunkSeriesEntry{
					Lset: defaultLabels,
					ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
						return storage.NewListChunkSeriesIterator(downsample.DownsampleRaw(samples1, downsample.ResLevel1)...)
					},
				},
				&storage.ChunkSeriesEntry{
					Lset: defaultLabels,
					ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
						return storage.NewListChunkSeriesIterator(downsample.DownsampleRaw(samples3, downsample.ResLevel1)...)
					},
				},
			},
			expected: &storage.ChunkSeriesEntry{
				Lset: defaultLabels,
				ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
					// Where both series have a sample at the same time, the first one is preferred.
					samples := [][]chunks.Sample{
						{sample{299999, 5}, sample{540000, 5}},
						{sample{299999, 600000}, sample{540000, 2100000}},
						{sample{299999, 0}, sample{540000, 300000}},
						{sample{299999, 240000}, sample{540000, 540000}},
						{sample{299999, 240000}, sample{299999, 240000}},
					}
					var chks [5]chunkenc.Chunk
					for i, s := range samples {
						chk, err := chunks.ChunkFromSamples(s)
						testutil.Ok(t, err)
						chks[i] = chk.Chunk
					}
					return storage.NewListChunkSeriesIterator(chunks.Meta{
						MinTime: 299999,
						MaxTime: 540000,
						Chunk:   downsample.EncodeAggrChunk(chks),
					})
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			merged := m(tc.input...)
			testutil.Equals(t, tc.expected.Labels(), merged.Labels())
			actChks, actErr := storage.ExpandChunks(merged.Iterator(nil))
			expChks, expErr := storage.ExpandChunks(tc.expected.Iterator(nil))

			testutil.Equals(t, expErr, actErr)
			testutil.Equals(t, expChks, actChks)
		})
	}
}

type histoSample struct {
	t  int64
	f  float64
	h  *histogram.Histogram
	fh *histogram.FloatHistogram
}

func (h histoSample) T() int64 {
	return h.t
}

func (h histoSample) F() float64 {
	return h.f
}

func (h histoSample) H() *histogram.Histogram {
	return h.h
}

func (h histoSample) FH() *histogram.FloatHistogram {
	return h.fh
}

func (h histoSample) Type() chunkenc.ValueType {
	if h.fh != nil {
		return chunkenc.ValFloatHistogram
	}
	if h.h != nil {
		return chunkenc.ValHistogram
	}
	return chunkenc.ValFloat
}

func (h histoSample) Copy() chunks.Sample {
	c := histoSample{}
	if h.h != nil {
		c.h = h.h.Copy()
	}
	if h.fh != nil {
		c.fh = h.fh.Copy()
	}
	return c
}

var histogramSample = &histogram.Histogram{
	Schema:        0,
	Count:         20,
	Sum:           -3.1415,
	ZeroCount:     12,
	ZeroThreshold: 0.001,
	NegativeSpans: []histogram.Span{
		{Offset: 0, Length: 4},
		{Offset: 1, Length: 1},
	},
	NegativeBuckets:  []int64{1, 2, -2, 1, -1},
	CounterResetHint: histogram.UnknownCounterReset,
}

func TestDedupChunkSeriesMerger_Histogram(t *testing.T) {
	scrapeIntervalMilli := int64(30_000)

	testCases := []struct {
		name     string
		input    []storage.ChunkSeries
		expected storage.ChunkSeries
	}{
		{
			name: "two overlapping - Histogram and Histogram",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{
					histoSample{t: 0 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 2 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 3 * scrapeIntervalMilli, h: histogramSample},
				}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{
					histoSample{t: 1 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 2 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 3 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 4 * scrapeIntervalMilli, h: histogramSample},
				}),
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{
				histoSample{t: 0 * scrapeIntervalMilli, h: histogramSample},
				histoSample{t: 1 * scrapeIntervalMilli, h: histogramSample},
				histoSample{t: 2 * scrapeIntervalMilli, h: histogramSample},
				histoSample{t: 3 * scrapeIntervalMilli, h: histogramSample},
				histoSample{t: 4 * scrapeIntervalMilli, h: histogramSample},
			}),
		},
		{
			name: "overlapping mixed - XOR then Histogram - panic repro case",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{
					histoSample{t: 1 * scrapeIntervalMilli, f: 1},
					histoSample{t: 2 * scrapeIntervalMilli, f: 1},
				}, []chunks.Sample{
					histoSample{t: 5 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 6 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 7 * scrapeIntervalMilli, h: histogramSample},
				}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{
					histoSample{t: 1 * scrapeIntervalMilli, f: 1},
					histoSample{t: 2 * scrapeIntervalMilli, f: 1},
				}, []chunks.Sample{
					histoSample{t: 5 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 6 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 7 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 8 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 9 * scrapeIntervalMilli, h: histogramSample},
				},
				),
			},
			expected: storage.NewListChunkSeriesFromSamples(
				labels.FromStrings("bar", "baz"),
				[]chunks.Sample{
					histoSample{t: 1 * scrapeIntervalMilli, f: 1},
					histoSample{t: 2 * scrapeIntervalMilli, f: 1},
				}, []chunks.Sample{
					histoSample{t: 5 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 6 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 7 * scrapeIntervalMilli, h: histogramSample},
				},
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := NewChunkSeriesMerger()
			merged := m(tc.input...)
			testutil.Equals(t, labels.FromStrings("bar", "baz"), merged.Labels())
			actChks, actErr := storage.ExpandChunks(merged.Iterator(nil))
			testutil.Ok(t, actErr)

			expChks, expErr := storage.ExpandChunks(tc.expected.Iterator(nil))
			testutil.Ok(t, expErr)
			testutil.Equals(t, expChks, actChks)
		})
	}
}

func createSamplesWithStep(start, numOfSamples, step int) []chunks.Sample {
	res := make([]chunks.Sample, numOfSamples)
	cur := start
	for i := range numOfSamples {
		res[i] = sample{t: int64(cur), f: float64(cur)}
		cur += step
	}

	return res
}

// TestDedupChunkSeriesMerger_MatchesQuerier checks the merger against the
// querier's penalty deduplication of the same replicas. The merger
// deduplicates each group of overlapping chunks on its own, so for every
// group it must return exactly what the querier returns for that group's
// time range.
func TestDedupChunkSeriesMerger_MatchesQuerier(t *testing.T) {
	const (
		sec        = int64(1000)
		interval   = 15 * sec
		blockRange = 7200 * sec
	)
	// scrape returns a sample every interval at phase+k*interval within [from, to).
	scrape := func(from, to, phase int64, value func(k int64) chunks.Sample) []chunks.Sample {
		var out []chunks.Sample
		for ts := phase; ts < to; ts += interval {
			if ts >= from {
				s := value(ts / interval)
				out = append(out, withT(s, ts))
			}
		}
		return out
	}
	float := func(v float64) func(int64) chunks.Sample {
		return func(int64) chunks.Sample { return histoSample{f: v} }
	}
	hist := func(offset int64) func(int64) chunks.Sample {
		return func(k int64) chunks.Sample { return histoSample{h: tsdbutil.GenerateTestHistogram(k + offset)} }
	}
	floatHist := func(offset int64) func(int64) chunks.Sample {
		return func(k int64) chunks.Sample {
			return histoSample{fh: tsdbutil.GenerateTestFloatHistogram(k + offset)}
		}
	}
	without := func(samples []chunks.Sample, from, to int64) []chunks.Sample {
		return slices.DeleteFunc(slices.Clone(samples), func(s chunks.Sample) bool { return s.T() >= from && s.T() < to })
	}

	complete := scrape(0, blockRange, 0, float(1))
	for _, tc := range []struct {
		name     string
		replicas [][]chunks.Sample
		// If set, the merger must return exactly the first replica.
		keepsFirst bool
	}{
		{
			name:       "lockstep, aligned chunks",
			replicas:   [][]chunks.Sample{complete, scrape(0, blockRange, 0, float(2))},
			keepsFirst: true,
		},
		{
			name:       "lockstep, second replica starts 600s later",
			replicas:   [][]chunks.Sample{complete, scrape(600*sec, blockRange, 0, float(2))},
			keepsFirst: true,
		},
		{
			name:       "lockstep, second replica starts 1234s later",
			replicas:   [][]chunks.Sample{complete, scrape(1234*sec, blockRange, 0, float(2))},
			keepsFirst: true,
		},
		{
			name:       "lockstep, first replica starts 600s later",
			replicas:   [][]chunks.Sample{scrape(600*sec, blockRange, 0, float(1)), complete},
			keepsFirst: false,
		},
		{
			name:     "lockstep, gap in the first replica",
			replicas: [][]chunks.Sample{without(complete, 3000*sec, 3600*sec), scrape(600*sec, blockRange, 0, float(2))},
		},
		{
			name:     "lockstep, gap in the second replica",
			replicas: [][]chunks.Sample{scrape(600*sec, blockRange, 0, float(1)), without(scrape(0, blockRange, 0, float(2)), 3000*sec, 3600*sec)},
		},
		{
			name:     "scraped 6s apart",
			replicas: [][]chunks.Sample{complete, scrape(0, blockRange, 6*sec, float(2))},
		},
		{
			name:     "scraped 6s apart, second replica starts 600s later",
			replicas: [][]chunks.Sample{complete, scrape(600*sec, blockRange, 6*sec, float(2))},
		},
		{
			name:     "scraped 9s apart, first replica starts 600s later",
			replicas: [][]chunks.Sample{scrape(600*sec, blockRange, 9*sec, float(1)), complete},
		},
		{
			name: "three replicas",
			replicas: [][]chunks.Sample{
				without(complete, 3000*sec, 3600*sec),
				scrape(600*sec, blockRange, 5*sec, float(2)),
				scrape(1234*sec, blockRange, 11*sec, float(3)),
			},
		},
		{
			name: "floats and native histograms",
			replicas: [][]chunks.Sample{
				scrape(0, 600*sec, 0, float(1)),
				scrape(300*sec, 1200*sec, 1*sec, hist(0)),
			},
		},
		{
			// Switching to the second replica looks like a counter reset.
			name: "native histograms",
			replicas: [][]chunks.Sample{
				without(scrape(0, blockRange, 0, hist(1)), 3000*sec, 3600*sec),
				scrape(600*sec, blockRange, 0, hist(0)),
			},
		},
		{
			name: "native histograms scraped 6s apart",
			replicas: [][]chunks.Sample{
				scrape(0, blockRange, 0, hist(0)),
				scrape(600*sec, blockRange, 6*sec, hist(1)),
			},
		},
		{
			name: "float native histograms",
			replicas: [][]chunks.Sample{
				without(scrape(0, blockRange, 0, floatHist(1)), 3000*sec, 3600*sec),
				scrape(600*sec, blockRange, 0, floatHist(0)),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			lset := labels.FromStrings("__name__", "up")
			var (
				input    []storage.ChunkSeries
				inChunks []chunks.Meta
			)
			for _, r := range tc.replicas {
				s := storage.NewListChunkSeriesFromSamples(lset, headChunks(r, blockRange)...)
				chks, err := storage.ExpandChunks(s.Iterator(nil))
				testutil.Ok(t, err)
				inChunks = append(inChunks, chks...)
				input = append(input, s)
			}

			merged := NewChunkSeriesMerger()(input...)
			testutil.Equals(t, lset, merged.Labels())
			out, err := storage.ExpandChunks(merged.Iterator(nil))
			testutil.Ok(t, err)
			got := expandCheckedChunks(t, out, inChunks)

			var exp []string
			for _, g := range overlapGroups(inChunks) {
				exp = append(exp, querierPenaltyDedup(t, lset, g[0], g[1], tc.replicas)...)
			}
			testutil.Equals(t, len(exp), len(got), "number of samples")
			testutil.Equals(t, exp, got)

			if tc.keepsFirst {
				testutil.Equals(t, formatSamples(t, storage.NewListSeries(lset, tc.replicas[0]).Iterator(nil)), got)
			}
		})
	}
}

// TestDedupChunkSeriesMerger_OverlappingChunksOfOneSeries checks that the
// chunks an input series has that overlap each other are merged without loss,
// rather than deduplicated as if they were replicas.
func TestDedupChunkSeriesMerger_OverlappingChunksOfOneSeries(t *testing.T) {
	lset := labels.FromStrings("bar", "baz")
	merged := NewChunkSeriesMerger()(
		storage.NewListChunkSeriesFromSamples(lset,
			[]chunks.Sample{sample{0, 0}, sample{10000, 10}, sample{20000, 20}, sample{30000, 30}},
			[]chunks.Sample{sample{5000, 5}, sample{15000, 15}, sample{25000, 25}},
		),
		storage.NewListChunkSeriesFromSamples(lset,
			[]chunks.Sample{sample{100000, 100}, sample{110000, 110}},
		),
	)
	act, err := storage.ExpandChunks(merged.Iterator(nil))
	testutil.Ok(t, err)
	exp, err := storage.ExpandChunks(storage.NewListChunkSeriesFromSamples(lset,
		[]chunks.Sample{sample{0, 0}, sample{5000, 5}, sample{10000, 10}, sample{15000, 15}, sample{20000, 20}, sample{25000, 25}, sample{30000, 30}},
		[]chunks.Sample{sample{100000, 100}, sample{110000, 110}},
	).Iterator(nil))
	testutil.Ok(t, err)
	testutil.Equals(t, exp, act)
}

func TestDedupChunkSeriesMerger_InputError(t *testing.T) {
	lset := labels.FromStrings("bar", "baz")
	merged := NewChunkSeriesMerger()(
		storage.NewListChunkSeriesFromSamples(lset, []chunks.Sample{sample{0, 0}, sample{10000, 10}}),
		&storage.ChunkSeriesEntry{
			Lset: lset,
			ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
				return errChunksIterator{err: errors.New("chunk read failed")}
			},
		},
	)
	_, err := storage.ExpandChunks(merged.Iterator(nil))
	testutil.NotOk(t, err)
}

// withT returns the sample at the given time.
func withT(s chunks.Sample, t int64) chunks.Sample {
	hs := s.(histoSample)
	hs.t = t
	return hs
}

// headChunks cuts samples into chunks the way the TSDB head does: aiming at
// 120 samples per chunk and spreading the samples evenly over the chunks of
// the chunk range, so the chunks of replicas that started at different times
// do not line up.
func headChunks(samples []chunks.Sample, chunkRange int64) [][]chunks.Sample {
	const samplesPerChunk = 120
	var (
		chks   [][]chunks.Sample
		curr   []chunks.Sample
		nextAt int64
	)
	for _, s := range samples {
		if len(curr) > 0 {
			if len(curr) == samplesPerChunk/4 {
				nextAt = computeChunkEndTime(curr[0].T(), curr[len(curr)-1].T(), nextAt)
			}
			if s.T() >= nextAt || len(curr) >= 2*samplesPerChunk || s.Type() != curr[0].Type() {
				chks = append(chks, curr)
				curr = nil
			}
		}
		if len(curr) == 0 {
			nextAt = (s.T()/chunkRange + 1) * chunkRange
		}
		curr = append(curr, s)
	}
	if len(curr) > 0 {
		chks = append(chks, curr)
	}
	return chks
}

// computeChunkEndTime is the TSDB head's estimate of when to cut a chunk
// that is a quarter full.
func computeChunkEndTime(start, cur, maxT int64) int64 {
	n := float64(maxT-start) / (float64(cur-start+1) * 4)
	if n <= 1 {
		return maxT
	}
	return int64(float64(start) + float64(maxT-start)/math.Floor(n))
}

// overlapGroups returns the time ranges of the groups of chunks that overlap,
// directly or through a chain of overlaps.
func overlapGroups(chks []chunks.Meta) [][2]int64 {
	chks = slices.Clone(chks)
	slices.SortFunc(chks, func(a, b chunks.Meta) int { return cmp.Compare(a.MinTime, b.MinTime) })
	var groups [][2]int64
	for _, c := range chks {
		if n := len(groups); n > 0 && c.MinTime <= groups[n-1][1] {
			groups[n-1][1] = max(groups[n-1][1], c.MaxTime)
			continue
		}
		groups = append(groups, [2]int64{c.MinTime, c.MaxTime})
	}
	return groups
}

// querierPenaltyDedup returns what the querier's penalty deduplication returns
// for the replicas' samples within [mint, maxt], offered in the given order.
func querierPenaltyDedup(t *testing.T, lset labels.Labels, mint, maxt int64, replicas [][]chunks.Sample) []string {
	t.Helper()
	var set testSeriesSet
	for _, r := range replicas {
		var in []chunks.Sample
		for _, s := range r {
			if s.T() >= mint && s.T() <= maxt {
				in = append(in, s)
			}
		}
		if len(in) > 0 {
			set.series = append(set.series, storage.NewListSeries(lset, in))
		}
	}
	dedupSet := NewSeriesSet(&set, "", AlgorithmPenalty)
	var out []string
	for dedupSet.Next() {
		out = append(out, formatSamples(t, dedupSet.At().Iterator(nil))...)
	}
	testutil.Ok(t, dedupSet.Err())
	return out
}

// expandCheckedChunks checks that the chunks are well-formed and sorted, and
// returns their samples. Chunks that are not input chunks passed through must
// not hold more samples than chunks are encoded with.
func expandCheckedChunks(t *testing.T, chks, inChunks []chunks.Meta) []string {
	t.Helper()
	var (
		out   []string
		lastT = int64(math.MinInt64)
	)
	for i, c := range chks {
		testutil.Assert(t, c.MinTime <= c.MaxTime, "chunk %d: min time %d after max time %d", i, c.MinTime, c.MaxTime)
		testutil.Assert(t, c.MinTime > lastT, "chunk %d: starts at %d, before the previous chunk ends at %d", i, c.MinTime, lastT)

		passedThrough := slices.ContainsFunc(inChunks, func(in chunks.Meta) bool {
			return in.MinTime == c.MinTime && in.MaxTime == c.MaxTime && bytes.Equal(in.Chunk.Bytes(), c.Chunk.Bytes())
		})
		n := c.Chunk.NumSamples()
		testutil.Assert(t, n > 0, "chunk %d: no samples", i)
		testutil.Assert(t, passedThrough || n <= 120, "chunk %d: %d samples", i, n)

		it := c.Chunk.Iterator(nil)
		samples := formatSamples(t, it)
		testutil.Equals(t, n, len(samples), "chunk %d: number of samples", i)
		it = c.Chunk.Iterator(it)
		for it.Next() != chunkenc.ValNone {
			ts := it.AtT()
			testutil.Assert(t, ts > lastT, "chunk %d: sample at %d after one at %d", i, ts, lastT)
			testutil.Assert(t, ts >= c.MinTime && ts <= c.MaxTime, "chunk %d: sample at %d outside [%d, %d]", i, ts, c.MinTime, c.MaxTime)
			if lastT < c.MinTime {
				testutil.Equals(t, c.MinTime, ts, "chunk %d: first sample", i)
			}
			lastT = ts
		}
		testutil.Ok(t, it.Err())
		testutil.Equals(t, c.MaxTime, lastT, "chunk %d: last sample", i)
		out = append(out, samples...)
	}
	return out
}

// formatSamples returns the samples as strings, ignoring counter reset hints
// and the layout of histogram buckets.
func formatSamples(t *testing.T, it chunkenc.Iterator) []string {
	t.Helper()
	var out []string
	for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
		switch vt {
		case chunkenc.ValFloat:
			ts, v := it.At()
			out = append(out, fmt.Sprintf("%d float %v", ts, v))
		case chunkenc.ValHistogram:
			ts, h := it.AtHistogram(nil)
			out = append(out, fmt.Sprintf("%d histogram %s", ts, h.String()))
		case chunkenc.ValFloatHistogram:
			ts, fh := it.AtFloatHistogram(nil)
			out = append(out, fmt.Sprintf("%d float histogram %s", ts, fh.String()))
		}
	}
	testutil.Ok(t, it.Err())
	return out
}

type testSeriesSet struct {
	series []storage.Series
	i      int
}

func (s *testSeriesSet) Next() bool {
	s.i++
	return s.i <= len(s.series)
}

func (s *testSeriesSet) At() storage.Series                { return s.series[s.i-1] }
func (s *testSeriesSet) Err() error                        { return nil }
func (s *testSeriesSet) Warnings() annotations.Annotations { return nil }

// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// ProvenanceExtension is the key under which blocks produced by a worker record
// where they came from, in the block's meta.json extensions.
//
// Extensions are the right place for this rather than Thanos.Source: the
// consistency delay filter exempts compactor-sourced blocks from the delay,
// and a new source value would hide worker output from the manager's next sync
// while the sources are already marked for deletion.
const ProvenanceExtension = "thanos_compact_distributed"

// Provenance records which task produced a block. It is what lets an operator
// tell blocks produced by the distributed compactor apart from every other
// block in the bucket - when debugging, and when rolling a trial back.
//
// It names the block it was written for and the blocks the task consumed.
// Both are there because extensions are inherited: the standalone compactor
// keeps the extensions its sources agree on, and downsampling copies them
// whole, so a block compacted or downsampled from a worker's block would carry
// the worker's stamp verbatim. The block ID tells an inherited stamp from the
// real one; the sources let a rollback check that what the block replaced can
// still be brought back before it deletes the block.
type Provenance struct {
	TaskID     string   `json:"task_id"`
	TaskType   TaskType `json:"task_type"`
	WorkerID   string   `json:"worker_id"`
	JournalID  string   `json:"journal_id"`
	Generation uint64   `json:"generation"`

	// BlockID is the ULID of the block the stamp was written for.
	BlockID string `json:"block_id"`
	// Sources are the blocks the task consumed to produce the block: the
	// plan's blocks for a compaction, the block downsampled for a downsample.
	Sources []string `json:"sources"`
}

// For returns a copy of the provenance completed for one output block.
func (p Provenance) For(id ulid.ULID, sources []string) Provenance {
	p.BlockID = id.String()
	p.Sources = slices.Clone(sources)
	return p
}

// Stamp adds the provenance to a block's extensions and returns the result.
// Extensions are an open map; anything already there is kept.
func (p Provenance) Stamp(extensions any) (any, error) {
	ext := map[string]any{}
	if extensions != nil {
		existing, ok := extensions.(map[string]any)
		if !ok {
			// Extensions of another shape would be lost by merging, and silently
			// dropping the provenance would undermine what it exists for.
			return nil, errors.Errorf("cannot stamp provenance onto extensions of type %T", extensions)
		}
		for k, v := range existing {
			ext[k] = v
		}
	}
	ext[ProvenanceExtension] = p
	return ext, nil
}

// ProvenanceOf returns the provenance recorded on a block, if any. A stamp
// written for a different block - inherited through compaction or
// downsampling by something other than a worker - does not count: the block
// carrying it was not produced by the task named, and treating it as if it
// were would make a rollback delete it.
func ProvenanceOf(m *metadata.Meta) (Provenance, bool) {
	ext, ok := m.Thanos.Extensions.(map[string]any)
	if !ok {
		return Provenance{}, false
	}
	raw, ok := ext[ProvenanceExtension]
	if !ok {
		return Provenance{}, false
	}

	// Whether the value is a Provenance stamped in process or a map read back
	// from JSON, a round trip through JSON normalises it.
	encoded, err := json.Marshal(raw)
	if err != nil {
		return Provenance{}, false
	}
	var p Provenance
	if err := json.Unmarshal(encoded, &p); err != nil || p.TaskID == "" || p.BlockID != m.ULID.String() {
		return Provenance{}, false
	}
	return p, true
}

// deletionDetailsPrefix opens the details of every deletion mark the
// distributed manager writes, so they can be told apart from marks written by
// anything else - the standalone compactor, retention, or an operator.
const deletionDetailsPrefix = "source of block compacted by distributed compactor"

// DeletionDetails returns the deletion mark details the manager writes when it
// marks the sources of a finished task.
func DeletionDetails(journalID, taskID string) string {
	return fmt.Sprintf("%s; journal %s; task %s", deletionDetailsPrefix, journalID, taskID)
}

// ParseDeletionDetails recognises deletion mark details written by the
// distributed manager and returns the journal and task they name.
func ParseDeletionDetails(details string) (journalID, taskID string, ok bool) {
	if !strings.HasPrefix(details, deletionDetailsPrefix+"; ") {
		return "", "", false
	}
	parts := strings.Split(strings.TrimPrefix(details, deletionDetailsPrefix+"; "), "; ")
	if len(parts) != 2 {
		return "", "", false
	}
	journalID = strings.TrimPrefix(parts[0], "journal ")
	taskID = strings.TrimPrefix(parts[1], "task ")
	if journalID == "" || taskID == "" || journalID == parts[0] || taskID == parts[1] {
		return "", "", false
	}
	return journalID, taskID, true
}

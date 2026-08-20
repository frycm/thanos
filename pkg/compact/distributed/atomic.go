// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import "sync"

// atomicBool is a small mutex guarded bool, used to tell the goroutine executing
// a task that its lease is no longer acknowledged.
type atomicBool struct {
	mtx sync.Mutex
	v   bool
}

func newAtomicBool(v bool) *atomicBool {
	return &atomicBool{v: v}
}

func (b *atomicBool) get() bool {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	return b.v
}

func (b *atomicBool) set(v bool) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	b.v = v
}

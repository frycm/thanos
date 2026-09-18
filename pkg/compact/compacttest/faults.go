// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compacttest

import (
	"context"
	"io"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
)

// HookBucket is one participant's view of a shared bucket. Faults injected
// here affect only this participant.
type HookBucket struct {
	objstore.Bucket
	mtx         sync.Mutex
	onGet       func(ctx context.Context, name string) error
	onUpload    func(ctx context.Context, name string) error
	afterUpload func(ctx context.Context, name string) error
}

// NewHookBucket wraps a bucket in a fault-injectable view.
func NewHookBucket(shared objstore.Bucket) *HookBucket {
	return &HookBucket{Bucket: shared}
}

func (b *HookBucket) hooks() (onGet, onUpload, afterUpload func(context.Context, string) error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	return b.onGet, b.onUpload, b.afterUpload
}

// SetOnGet installs a hook consulted before every read; a non-nil error fails
// the read. nil removes the hook.
func (b *HookBucket) SetOnGet(f func(ctx context.Context, name string) error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	b.onGet = f
}

// SetOnUpload installs a hook consulted before every write; a non-nil error
// rejects the write before it reaches the bucket. nil removes the hook.
func (b *HookBucket) SetOnUpload(f func(ctx context.Context, name string) error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	b.onUpload = f
}

// SetAfterUpload installs a hook consulted after a successful write; a non-nil
// error is returned to the writer although the object is in the bucket, which
// is what a lost acknowledgement looks like. nil removes the hook.
func (b *HookBucket) SetAfterUpload(f func(ctx context.Context, name string) error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	b.afterUpload = f
}

// Get implements objstore.Bucket.
func (b *HookBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if onGet, _, _ := b.hooks(); onGet != nil {
		if err := onGet(ctx, name); err != nil {
			return nil, err
		}
	}
	return b.Bucket.Get(ctx, name)
}

// Upload implements objstore.Bucket.
func (b *HookBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	_, onUpload, afterUpload := b.hooks()
	if onUpload != nil {
		if err := onUpload(ctx, name); err != nil {
			return err
		}
	}
	if err := b.Bucket.Upload(ctx, name, r, opts...); err != nil {
		return err
	}
	if afterUpload != nil {
		return afterUpload(ctx, name)
	}
	return nil
}

// ErrInjected is the error every injected fault returns.
var ErrInjected = errors.New("injected fault")

// Outage makes every read and write of the view fail until the returned
// function is called.
func (b *HookBucket) Outage() (lift func()) {
	fail := func(context.Context, string) error { return errors.Wrap(ErrInjected, "object store down") }
	b.SetOnGet(fail)
	b.SetOnUpload(fail)
	var once sync.Once
	return func() {
		once.Do(func() {
			b.SetOnGet(nil)
			b.SetOnUpload(nil)
		})
	}
}

// PublicationFault fails the first write of a block file of the given kind -
// "chunks", "index" or "meta.json" - once. Before is a rejection before the
// object reaches the bucket; otherwise the object is written and the
// acknowledgement is lost.
type PublicationFault struct {
	Stage  string
	Before bool
	// Skip exempts object names from the fault, for writes that are not block
	// publication, such as a journal.
	Skip func(name string) bool

	mtx  sync.Mutex
	hits int
}

// Hits reports how often the fault fired.
func (f *PublicationFault) Hits() int {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	return f.hits
}

func (f *PublicationFault) matches(name string) bool {
	if f.Skip != nil && f.Skip(name) {
		return false
	}
	return strings.Contains(name, "/"+f.Stage+"/") || strings.HasSuffix(name, "/"+f.Stage)
}

func (f *PublicationFault) fire(name string) error {
	if !f.matches(name) {
		return nil
	}
	f.mtx.Lock()
	defer f.mtx.Unlock()
	if f.hits > 0 {
		return nil
	}
	f.hits++
	return errors.Wrap(ErrInjected, "publication failure")
}

// Install arms the fault on the view.
func (f *PublicationFault) Install(b *HookBucket) {
	if f.Before {
		b.SetOnUpload(func(_ context.Context, name string) error { return f.fire(name) })
		return
	}
	b.SetAfterUpload(func(_ context.Context, name string) error { return f.fire(name) })
}

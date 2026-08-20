// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/pkg/errors"

	"github.com/thanos-io/thanos/pkg/compact"
)

// TestSchedulerMaintenanceErrorPropagatesHalts asserts the manager's
// maintenance actor ends the manager on a halt and only logs anything else.
// A manager that swallowed the halt would stay alive while its journal aged
// past the point where a rollback takes it for stopped, and could then wake
// up and race that rollback.
func TestSchedulerMaintenanceErrorPropagatesHalts(t *testing.T) {
	logger := log.NewNopLogger()

	testutil.Ok(t, schedulerMaintenanceError(logger, nil))
	testutil.Ok(t, schedulerMaintenanceError(logger, errors.New("journal write failed, transient")))

	halt := compact.NewHaltError(errors.New("journal taken over by another manager"))
	err := schedulerMaintenanceError(logger, halt)
	testutil.NotOk(t, err)
	testutil.Equals(t, true, compact.IsHaltError(err))
}

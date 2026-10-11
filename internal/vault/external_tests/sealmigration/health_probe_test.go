// Copyright (c) 2026 OpenBao contributors
// SPDX-License-Identifier: MPL-2.0

package sealmigration

import (
	"testing"
	"time"

	"github.com/go-test/deep"

	"github.com/hashicorp/go-hclog"
	"github.com/openbao/openbao/sdk/v2/helper/logging"
	"github.com/openbao/openbao/v2/internal/helper/testhelpers"
	sealhelper "github.com/openbao/openbao/v2/internal/helper/testhelpers/seal"
	"github.com/openbao/openbao/v2/internal/helper/testhelpers/teststorage"
	"github.com/openbao/openbao/v2/internal/vault"
)

// TestSealMigration_HealthDuringPendingMigration reproduces issue #4182:
// after a standby is unsealed with -migrate but before the migration
// completes (i.e., before this node becomes active), /sys/health and
// /sys/seal-status should still report the node's actual state rather
// than returning an error.
func TestSealMigration_HealthDuringPendingMigration(t *testing.T) {
	t.Parallel()

	// Raft: read-enabled standby -> postUnseal runs on standby, matching
	// the reporter's integrated-storage HA deployment (issue #4182).
	logger := logging.NewVaultLogger(hclog.Trace).Named(t.Name())
	// Use a port range outside the other tests' allocations (see
	// BasePort_* constants) so parallel subtests don't collide.
	raftBasePort := 25100
	vault.TestingUpdateClusterAddr.Store(true)
	addressProvider := testhelpers.NewHardcodedServerAddressProvider(numTestCores, raftBasePort+10)
	storage := teststorage.MakeReusableRaftStorage(
		t, logger, numTestCores, addressProvider,
	)

	// Initialize the backend using shamir
	cluster, opts := initializeShamir(t, logger, storage, raftBasePort)
	defer storage.Cleanup(t, cluster)
	_ = cluster.RootToken

	// Create the transit server.
	tss := sealhelper.NewTransitSealServer(t, 0)
	defer tss.Cleanup()
	sealKeyName := "transit-seal-key-1"
	tss.MakeKey(t, sealKeyName)

	// Configure migration to transit.
	opts.SealFunc = func() vault.Seal {
		seal, err := tss.MakeSeal(t, sealKeyName)
		if err != nil {
			t.Fatal(err)
		}
		return seal
	}

	// Restart one follower with the new config and unseal it with -migrate.
	// This leaves the node in the pending-migration standby state.
	cluster.StopCore(t, 1)
	if storage.IsRaft {
		teststorage.CloseRaftStorage(t, cluster, 1)
	}
	cluster.StartCore(t, 1, opts)

	unsealMigrate(t, cluster.Cores[1].Client, cluster.BarrierKeys, true)

	// PROBE: the node is now unsealed, in standby, pending migration.
	// Issue #4182 says health/seal-status error out in this window.
	t.Run("seal-status", func(t *testing.T) {
		resp, err := cluster.Cores[1].Client.Sys().SealStatus()
		if err != nil {
			t.Fatalf("seal-status during pending migration returned error: %v", err)
		}
		if resp.Sealed {
			t.Fatalf("expected unsealed node, got %#v", resp)
		}
	})

	t.Run("health", func(t *testing.T) {
		req := cluster.Cores[1].Client.NewRequest("GET", "/v1/sys/health")
		resp, err := cluster.Cores[1].Client.RawRequest(req)
		if err != nil {
			t.Fatalf("health during pending migration returned error: %v", err)
		}
		defer resp.Body.Close()
	})

	// Complete the migration the way an operator would: migrate the
	// remaining followers, step down the active node, wait for the
	// migration to finish on the new active node, and verify the
	// cluster still serves data and every core reports the new seal.
	for i := 2; i < len(cluster.Cores); i++ {
		cluster.StopCore(t, i)
		if storage.IsRaft {
			teststorage.CloseRaftStorage(t, cluster, i)
		}
		cluster.StartCore(t, i, opts)

		unsealMigrate(t, cluster.Cores[i].Client, cluster.BarrierKeys, true)
	}

	if err := cluster.Cores[0].Client.Sys().StepDown(); err != nil {
		t.Fatal(err)
	}

	var leaderIdx int
	for range 30 {
		var err error
		leaderIdx, err = testhelpers.AwaitLeader(t, cluster)
		if err != nil {
			t.Fatal(err)
		}
		if leaderIdx != 0 {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if leaderIdx == 0 {
		t.Fatal("Core 0 cannot be the leader right now")
	}
	leader := cluster.Cores[leaderIdx]

	// Wait for the migration to complete on the new active node.
	awaitMigration(t, leader.Client)

	// The cluster must still serve previously-written data.
	secret, err := leader.Client.Logical().Read("kv-wrapped/foo")
	if err != nil {
		t.Fatal(err)
	}
	if diff := deep.Equal(secret.Data, map[string]any{"zork": "quux"}); len(diff) > 0 {
		t.Fatal(diff)
	}

	// Every core must now report the transit seal in its stored config.
	var appliedIndex uint64
	if storage.IsRaft {
		appliedIndex = testhelpers.RaftAppliedIndex(leader)
	}
	for _, core := range cluster.Cores {
		if storage.IsRaft {
			testhelpers.WaitForRaftApply(t, core, appliedIndex)
		}
		verifySealConfigTransit(t, core)
	}
}

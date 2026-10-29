package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	grpClerks map[tester.Tgid]*shardgrp.Clerk // one clerk per group, so each remembers its leader

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt, grpClerks: make(map[tester.Tgid]*shardgrp.Clerk)}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

func (sck *ShardCtrler) grpClerk(gid tester.Tgid, servers []string) *shardgrp.Clerk {
	ck, ok := sck.grpClerks[gid]
	if !ok {
		ck = shardgrp.MakeClerk(sck.clnt, servers)
		sck.grpClerks[gid] = ck
	}
	return ck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// put in kvsrv at version 0
	err := sck.IKVClerk.Put("config", cfg.String(), 0)
	if err == rpc.ErrMaybe {
		s, _, gerr := sck.IKVClerk.Get("config")
		if gerr == rpc.OK && shardcfg.FromString(s).Num >= cfg.Num {
			err = rpc.OK
		}
	}
	if err != rpc.OK {
		panic("InitConfig: failed to put new config in kvsrv")
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.

	// get current config from kvsrv
	s, v, err := sck.IKVClerk.Get("config")
	if err != rpc.OK {
		panic("ChangeConfigTo: failed to get current config from kvsrv")
	}
	// unmarshal the string into a ShardConfig
	cur := shardcfg.FromString(s)

	// check that new.Num is cur.Num + 1
	if new.Num != cur.Num+1 {
		panic("ChangeConfigTo: new config number is not cur.Num + 1")
	}

	// Diff the current and new configurations to determine which shards have changed ownership
	for shard := range shardcfg.Tshid(shardcfg.NShards) {
		curGid := cur.Shards[shard]
		newGid := new.Shards[shard]
		if curGid != newGid {
			// Shard ownership has changed, so we need to update the kvsrv for the new owner

			curOwner := sck.grpClerk(curGid, cur.Groups[curGid])
			newOwner := sck.grpClerk(newGid, new.Groups[newGid])

			// Freeze shard on current owner
			state, err := curOwner.FreezeShard(shard, new.Num)
			if err == rpc.ErrOldConfig {
				println("ChangeConfigTo: freeze shard", shard, "on gid", curGid, "OldConfig")
				return
			} else if err != rpc.OK {
				panic("ChangeConfigTo: failed to freeze shard on current owner: " + string(err))
			}

			// Install shard on new owner
			err = newOwner.InstallShard(shard, state, new.Num)
			if err == rpc.ErrOldConfig {
				println("ChangeConfigTo: install shard", shard, "on gid", newGid, "OldConfig")
				return
			} else if err != rpc.OK {
				panic("ChangeConfigTo: failed to install shard on new owner: " + string(err))
			}

			// Delete shard from current owner
			err = curOwner.DeleteShard(shard, new.Num)
			if err == rpc.ErrOldConfig {
				println("ChangeConfigTo: delete shard", shard, "on gid", curGid, "OldConfig")
				return
			} else if err != rpc.OK {
				panic("ChangeConfigTo: failed to delete shard from current owner: " + string(err))
			}
		}
	}

	// Put the new configuration in the kvsrv at version new.Num
	err = sck.IKVClerk.Put("config", new.String(), v)
	if err == rpc.ErrMaybe {
		s, _, gerr := sck.IKVClerk.Get("config")
		if gerr == rpc.OK && shardcfg.FromString(s).Num >= new.Num {
			err = rpc.OK
		}
	}
	if err != rpc.OK {
		panic("ChangeConfigTo: failed to put new config in kvsrv")
	}
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	s, _, err := sck.IKVClerk.Get("config")
	if err != rpc.OK {
		panic("Query: failed to get current config from kvsrv")
	}
	// unmarshal the string into a ShardConfig
	return shardcfg.FromString(s)
}

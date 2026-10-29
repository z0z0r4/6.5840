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

func (sck *ShardCtrler) applyConfig(cur *shardcfg.ShardConfig, new *shardcfg.ShardConfig) bool {
	// Diff the current and new configurations to determine which shards have changed ownership
	for shard := range shardcfg.Tshid(shardcfg.NShards) {
		curGid := cur.Shards[shard]
		newGid := new.Shards[shard]

		if curGid != newGid {
			// Shard ownership has changed, so we need to update the kvsrv for the new owner
			curOwner := sck.grpClerk(curGid, cur.Groups[curGid])
			newOwner := sck.grpClerk(newGid, new.Groups[newGid])

			// Freeze shard on current owner
			err := curOwner.FreezeShard(shard, new.Num)
			switch err {
			case rpc.ErrOldConfig, rpc.ErrWrongGroup:
				return false
			case rpc.ErrDuplicateOp:
				// continue
			case rpc.OK:
				// continue
				println("ChangeConfigTo: froze shard", shard, "on current owner", curGid)
			default:
				panic("ChangeConfigTo: failed to freeze shard on current owner: " + string(err))
			}

			// Get the state of the shard from the current owner
			need_install := true
			state, owned, _, err := curOwner.GetShard(shard)
			switch err {
			case rpc.ErrWrongGroup:
				// check newOwner
				_, new_owned, _, new_err := newOwner.GetShard(shard)
				if new_err == rpc.OK && new_owned {
					need_install = false
				}
			case rpc.OK:
				println("ChangeConfigTo: got shard", shard, "state from current owner", curGid)
			default:
				panic("ChangeConfigTo: failed to get shard state from current owner: " + string(err))
			}

			if need_install {
				// Install shard on new owner
				err = newOwner.InstallShard(shard, state, new.Num)
				switch err {
				case rpc.ErrOldConfig, rpc.ErrWrongGroup:
					return false
				case rpc.ErrDuplicateOp:
					// continue
				case rpc.OK:
					// continue
					println("ChangeConfigTo: installed shard", shard, "on new owner", newGid)
				default:
					panic("ChangeConfigTo: failed to install shard on new owner: " + string(err))
				}
			}

			// Delete shard from current owner
			if owned {
				err = curOwner.DeleteShard(shard, new.Num)
				switch err {
				case rpc.ErrOldConfig, rpc.ErrWrongGroup:
					return false
				case rpc.ErrDuplicateOp:
					// continue
				case rpc.OK:
					// continue
					println("ChangeConfigTo: deleted shard", shard, "from current owner", curGid)
				default:
					panic("ChangeConfigTo: failed to delete shard from current owner: " + string(err))
				}
			}
		}
	}
	return true
}

func (sck *ShardCtrler) get_config(key string) (*shardcfg.ShardConfig, rpc.Tversion, rpc.Err) {
	// Get the current configuration from the kvsrv
	config_, v, err := sck.Get(key)
	if err != rpc.OK {
		return nil, 0, err
	}
	config := shardcfg.FromString(config_)
	return config, v, rpc.OK
}

func (sck *ShardCtrler) put_config(key string, cfg *shardcfg.ShardConfig, version rpc.Tversion) bool {
	// Update the kvsrv for the new configuration
	err := sck.Put(key, cfg.String(), version)
	switch err {
	case rpc.ErrVersion:
		return false
	case rpc.ErrMaybe:
		if current, _, _ := sck.get_config(key); current != nil && current.String() == cfg.String() {
			return true
		}
	case rpc.OK:
		return true
	}
	return false
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	next_config, _, nerr := sck.get_config("next_config")
	if nerr == rpc.ErrNoKey {
		return
	} else if nerr != rpc.OK {
		panic("InitController: failed to get next_config from kvsrv")
	}

	cur, v, err := sck.get_config("config")
	if err == rpc.OK {
		// continue apply it
		if next_config.Num > cur.Num {
			sck.applyConfig(cur, next_config)
			put_success := sck.put_config("config", next_config, v)
			if put_success {
				println("InitController: applied next_config", next_config.Num, "to config")
			} else {
				println("InitController: failed to put config from", cur.Num, "to", next_config.Num)
			}
		}
	} else {
		panic("InitController: failed to get config from kvsrv")
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	sck.Put("config", cfg.String(), 0)
	// sck.Put("next_config", cfg.String(), 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// get current config from kvsrv
	cur, v, err := sck.get_config("config")
	if err != rpc.OK {
		panic("ChangeConfigTo: failed to get current config from kvsrv")
	}

	if cur.Num+1 != new.Num {
		return
	}

	next_config, nv, nerr := sck.get_config("next_config")

	if nerr == rpc.ErrNoKey || nerr == rpc.OK {
		put_success := sck.put_config("next_config", new, nv)
		if !put_success {
			return
		} else {
			next_config = new
		}
	} else if nerr != rpc.OK {
		panic("ChangeConfigTo: failed to get next_config from kvsrv")
	}

	if !sck.applyConfig(cur, next_config) {
		return
	}

	put_success := sck.put_config("config", next_config, v)
	if put_success {
		println("ChangeConfigTo: Success from", cur.Num, "to", new.Num)
	} else {
		println("ChangeConfigTo: Failed to put config from", cur.Num, "to", new.Num)
	}
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	s, _, err := sck.get_config("config")
	if err != rpc.OK {
		panic("Query: failed to get current config from kvsrv")
	}
	// unmarshal the string into a ShardConfig
	return s
}

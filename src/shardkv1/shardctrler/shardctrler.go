package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	kvraft "6.5840/kvraft1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	grpClerks map[tester.Tgid]*shardgrp.Clerk // one clerk per group, so each remembers its leader

	// Set once another controller has changed the configuration under
	// this one. A deposed controller has lost leadership for good and
	// makes no further configuration changes.
	deposed bool

	// Your data here.
}

// Make a ShardCltler, which stores its state in the kvraft group
// formed by servers.
func MakeShardCtrler(clnt *tester.Clnt, servers []string) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt, grpClerks: make(map[tester.Tgid]*shardgrp.Clerk)}
	sck.IKVClerk = kvraft.MakeClerk(clnt, servers)
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

// superseded reports whether the stored configuration has already
// reached num, i.e. another controller finished this step.
func (sck *ShardCtrler) superseded(num shardcfg.Tnum) bool {
	cfg, _, err := sck.get_config("config")
	return err == rpc.OK && cfg.Num >= num
}

// untilAnswered runs a shard RPC until some server of the group answers,
// and returns its error. If no server answers, the group may have left
// and been shut down, which only happens after a newer controller has
// finished this step; ok is false in that case and the caller must give
// up. Otherwise the group is only unreachable for now, so try again.
func (sck *ShardCtrler) untilAnswered(num shardcfg.Tnum, op func() rpc.Err) (err rpc.Err, ok bool) {
	for {
		if err = op(); err != shardrpc.ErrUnreachable {
			return err, true
		}
		if sck.superseded(num) {
			return err, false
		}
	}
}

func (sck *ShardCtrler) applyConfig(cur *shardcfg.ShardConfig, new *shardcfg.ShardConfig) bool {
	// Diff the current and new configurations to determine which shards have changed ownership
	for shard := range shardcfg.Tshid(shardcfg.NShards) {
		curGid := cur.Shards[shard]
		newGid := new.Shards[shard]

		if curGid != newGid {
			// Shard ownership has changed, so we need to update kvraft for the new owner
			curOwner := sck.grpClerk(curGid, cur.Groups[curGid])
			newOwner := sck.grpClerk(newGid, new.Groups[newGid])

			// Freeze shard on current owner
			err, ok := sck.untilAnswered(new.Num, func() rpc.Err {
				return curOwner.FreezeShard(shard, new.Num)
			})
			if !ok {
				return false
			}
			switch err {
			case shardrpc.ErrOldConfig, rpc.ErrWrongGroup:
				return false
			case shardrpc.ErrDuplicateOp:
				// continue
			case rpc.OK:
				// continue
				println("ChangeConfigTo: froze shard", shard, "on current owner", curGid)
			default:
				panic("ChangeConfigTo: failed to freeze shard on current owner: " + string(err))
			}

			// Get the state of the shard from the current owner
			need_install := true
			var state []byte
			var owned bool
			err, ok = sck.untilAnswered(new.Num, func() (e rpc.Err) {
				state, owned, _, e = curOwner.GetShard(shard)
				return e
			})
			if !ok {
				return false
			}
			switch err {
			case rpc.ErrWrongGroup:
				// check newOwner
				var new_owned bool
				new_err, ok := sck.untilAnswered(new.Num, func() (e rpc.Err) {
					_, new_owned, _, e = newOwner.GetShard(shard)
					return e
				})
				if !ok {
					return false
				}
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
				err, ok = sck.untilAnswered(new.Num, func() rpc.Err {
					return newOwner.InstallShard(shard, state, new.Num)
				})
				if !ok {
					return false
				}
				switch err {
				case shardrpc.ErrOldConfig, rpc.ErrWrongGroup:
					return false
				case shardrpc.ErrDuplicateOp:
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
				err, ok = sck.untilAnswered(new.Num, func() rpc.Err {
					return curOwner.DeleteShard(shard, new.Num)
				})
				if !ok {
					return false
				}
				switch err {
				case shardrpc.ErrOldConfig, rpc.ErrWrongGroup:
					return false
				case shardrpc.ErrDuplicateOp:
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
	// Get the current configuration from kvraft
	config_, v, err := sck.Get(key)
	if err != rpc.OK {
		return nil, 0, err
	}
	config := shardcfg.FromString(config_)
	return config, v, rpc.OK
}

func (sck *ShardCtrler) put_config(key string, cfg *shardcfg.ShardConfig, version rpc.Tversion) bool {
	// Update kvraft for the new configuration
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
		panic("InitController: failed to get next_config from kvraft")
	}

	cur, v, err := sck.get_config("config")
	if err == rpc.OK {
		// continue apply it
		if next_config.Num > cur.Num {
			if !sck.applyConfig(cur, next_config) {
				sck.deposed = true
				return
			}
			put_success := sck.put_config("config", next_config, v)
			if put_success {
				println("InitController: applied next_config", next_config.Num, "to config")
			} else {
				sck.deposed = true
				println("InitController: failed to put config from", cur.Num, "to", next_config.Num)
			}
		}
	} else {
		panic("InitController: failed to get config from kvraft")
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in kvraft for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	sck.Put("config", cfg.String(), 0)
	// sck.Put("next_config", cfg.String(), 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller. A superseded controller stops: it returns without a
// change now and on every later call.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	if sck.deposed {
		return
	}
	// Each early return below means another controller changed the
	// configuration while this one was working on it.
	changed := false
	defer func() {
		if !changed {
			sck.deposed = true
		}
	}()

	// get current config from kvraft
	cur, v, err := sck.get_config("config")
	if err != rpc.OK {
		panic("ChangeConfigTo: failed to get current config from kvraft")
	}

	if cur.Num+1 != new.Num {
		return
	}

	next_config, nv, nerr := sck.get_config("next_config")
	if nerr == rpc.OK && next_config.Num >= new.Num {
		return
	} else if nerr == rpc.ErrNoKey || nerr == rpc.OK && next_config.Num < new.Num {
		put_success := sck.put_config("next_config", new, nv)
		if !put_success {
			return
		} else {
			next_config = new
		}
	} else if nerr != rpc.OK {
		panic("ChangeConfigTo: failed to get next_config from kvraft")
	}

	if !sck.applyConfig(cur, next_config) {
		return
	}

	put_success := sck.put_config("config", next_config, v)
	if put_success {
		changed = true
		println("ChangeConfigTo: Success from", cur.Num, "to", new.Num)
	} else {
		println("ChangeConfigTo: Failed to put config from", cur.Num, "to", new.Num)
	}
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	s, _, err := sck.get_config("config")
	if err != rpc.OK {
		panic("Query: failed to get current config from kvraft")
	}
	return s
}

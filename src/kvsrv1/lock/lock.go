package lock

import (
<<<<<<< HEAD
	"fmt"
=======
>>>>>>> 7b002c8 (feat: kvsrv1/lock)
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck   kvtest.IKVClerk
	name string

	// Hint: You will need a unique identifier for each lock client; call kvtest.RandValue(8) to generate a random string.
	// 只有 version 只能做到线性操作，需要附带 caller 检查才能保证由锁定者释放锁
	caller_id string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, name: lockname, caller_id: kvtest.RandValue(8)}
	return lk
}

func (lk *Lock) Acquire() {
	received_errmaybe := false
	for {
		s, v, err := lk.ck.Get(lk.name)
		if err == rpc.OK && s == lk.caller_id+"-locked" && received_errmaybe {
			// If we already hold the lock, then we can return.
			return
		}

		if err == rpc.ErrNoKey || (err == rpc.OK && s == "unlocked") {
			// Set locked
			put_err := lk.ck.Put(lk.name, lk.caller_id+"-locked", v)
			switch put_err {
			case rpc.OK:
				return
			case rpc.ErrMaybe:
				received_errmaybe = true
			}

			// fmt.Printf("Acquire: Caller %s Put Error: %v\n", lk.caller_id, put_err)
		}
		// fmt.Printf("Acquire: Caller %s Get Error: %v, s: %v, v: %v\n", lk.caller_id, err, s, v)

		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	received_errmaybe := false
	for {
		s, v, err := lk.ck.Get(lk.name)
		if err == rpc.OK {
			if s == "unlocked" && received_errmaybe {
				// If we received an ErrMaybe, then we may have already released the lock, then we can return.
				return
			}

			if s == lk.caller_id+"-locked" {
				// Set unlocked
				put_err := lk.ck.Put(lk.name, "unlocked", v)
				if put_err == rpc.OK {
					return
				} else if put_err == rpc.ErrMaybe {
					received_errmaybe = true
				}

				// fmt.Printf("Release: Caller %s Put Error: %v\n", lk.caller_id, put_err)
			}
		}

		// fmt.Printf("Release: Caller %s Get Error: %v, s: %v, v: %v\n", lk.caller_id, err, s, v)

		time.Sleep(100 * time.Millisecond)
	}
}

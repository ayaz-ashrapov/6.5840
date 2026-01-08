package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	clientId string
	lockName string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockName = l
	lk.clientId = kvtest.RandValue(8)
	lk.ck.Put(l, "", 0)
	return lk
}

func (lk *Lock) Acquire() {
	for true {
		owner, version, err := lk.ck.Get(lk.lockName)
		if err == rpc.OK {
			if owner == "" {
				putErr := lk.ck.Put(lk.lockName, lk.clientId, version)
				if putErr == rpc.OK {
					return
				} else if putErr == rpc.ErrMaybe {
					owner, version, err = lk.ck.Get(lk.lockName)
					if owner == lk.clientId {
						return
					}
				}
			} else {
				time.Sleep(10 * time.Millisecond)
			}
		} else if err == rpc.ErrNoKey {
			putErr := lk.ck.Put(lk.lockName, lk.clientId, version)
			if putErr == rpc.OK {
				return
			} else if putErr == rpc.ErrMaybe {
				owner, version, err = lk.ck.Get(lk.lockName)
				if owner == lk.clientId {
					return
				}
			}
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (lk *Lock) Release() {
	for true {
		owner, version, err := lk.ck.Get(lk.lockName)
		if err == rpc.OK {
			if owner == "" {
				return
			}
			if lk.clientId == owner {
				putError := lk.ck.Put(lk.lockName, "", version)
				if putError == rpc.OK {
					return
				} else if putError == rpc.ErrMaybe {
					continue
				}
			}
		} else if err == rpc.ErrNoKey {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

}

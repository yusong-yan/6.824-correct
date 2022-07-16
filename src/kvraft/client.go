package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"raft/labrpc"
)

type Clerk struct {
	servers      []*labrpc.ClientEnd
	clientId     int64
	commandId    int64
	serverNumber int
	leaderId     int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers:      servers,
		leaderId:     0,
		clientId:     nrand(),
		commandId:    1,
		serverNumber: len(servers),
	}
}

func (ck *Clerk) Get(key string) string {
	return ck.Command(&CommandArgs{Key: key, Op: Gett})
}

func (ck *Clerk) Put(key string, value string) {
	ck.Command(&CommandArgs{Key: key, Value: value, Op: Putt})
}
func (ck *Clerk) Append(key string, value string) {
	ck.Command(&CommandArgs{Key: key, Value: value, Op: Appendd})
}

func (ck *Clerk) Command(args *CommandArgs) string {
	args.ClientId, args.CommandId = ck.clientId, ck.commandId
	for {
		ch := make(chan *CommandReply, 1)
		go func() {
			reply := new(CommandReply)
			DPrintf("Client %v start CommandId %v", ck.clientId, args.CommandId)
			ck.servers[ck.leaderId].Call("KVServer.Command", args, reply)
			ch <- reply
		}()

		time_out := time.After(100 * time.Millisecond)
		select {
		case reply := <-ch:
			if (reply.Err == OK || reply.Err == ErrNoKey) && ck.commandId == args.CommandId {
				ck.commandId++
				return reply.Value
			}
			//else fail
		case <-time_out:
			//fail
		}
		//fail then retry
		ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
	}
}

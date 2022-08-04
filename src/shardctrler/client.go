package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"raft/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	id        int64
	CommandId uint64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.id = nrand()
	return ck
}

// num: query
// servers: Join
// gids: leave
// shard gid : move
func (ck *Clerk) Command(num int, servers map[int][]string, gids []int, shard int, gid int, commandType OpType) Config {
	args := &CommandArgs{}
	args.ClientId = ck.id
	args.CommandId = ck.CommandId
	ck.CommandId++
	args.Num = num
	args.GIDs = gids
	args.Servers = servers
	args.Shard = shard
	args.GID = gid
	args.Type = commandType
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply CommandReply
			ok := srv.Call("ShardCtrler.Command", args, &reply)
			if ok && !reply.WrongLeader {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Query(num int) Config {
	return ck.Command(num, nil, nil, 0, 0, QUERY)
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.Command(0, servers, nil, 0, 0, JOIN)
}

func (ck *Clerk) Leave(gids []int) {
	ck.Command(0, nil, gids, 0, 0, LEAVE)
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.Command(0, nil, nil, shard, gid, MOVE)
}

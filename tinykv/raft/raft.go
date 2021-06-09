// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout     int
	nextElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	logger *log.Logger
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	hardState, _, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	lastIndex, err := c.Storage.LastIndex()
	if err != nil {
		panic(err)
	}
	rlog := &RaftLog{
		storage:   c.Storage,
		committed: hardState.Commit,
		applied:   c.Applied,
		stabled:   lastIndex,
	}
	logger := log.New()
	prs := make(map[uint64]*Progress, 0)
	for _, pid := range c.peers {
		prs[pid] = &Progress{}
	}
	votes := make(map[uint64]bool)
	for _, pid := range c.peers {
		votes[pid] = false
	}
	r := &Raft{
		id:               c.ID,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		RaftLog:          rlog,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		Prs:              prs,
		votes:            votes,
		logger:           logger,
	}
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	nextIndex := r.Prs[to].Next
	appendEntries := make([]*pb.Entry, 0)
	prevIndex := nextIndex - 1
	prevTerm := r.Term
	if nextIndex < r.RaftLog.stabled {
		storageEntries, err := r.RaftLog.storage.Entries(nextIndex, r.RaftLog.stabled-1)
		if err != nil {
			panic(err)
		}
		for _, entry := range storageEntries {
			appendEntries = append(appendEntries, &entry)
		}
		prevTerm, err = r.RaftLog.Term(prevIndex)
		if err != nil {
			panic(err)
		}
	}
	unstableEntries := r.RaftLog.entries[nextIndex-r.RaftLog.stabled-1:]
	for _, entry := range unstableEntries {
		appendEntries = append(appendEntries, &entry)
	}
	appendMsg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		Entries: appendEntries,
		LogTerm: prevTerm,
		Index:   prevIndex,
	}
	r.msgs = append(r.msgs, appendMsg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	hbMsg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	r.push(hbMsg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	if r.State == StateFollower || r.State == StateCandidate {
		r.electionElapsed += 1
		if r.electionElapsed >= r.nextElectionTimeout {
			r.resetElectionTimeout()
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				From:    r.id,
				To:      r.id,
				Term:    r.Term,
			})
		}
	} else if r.State == StateLeader {
		r.heartbeatElapsed += 1
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				From:    r.id,
				To:      r.id,
				Term:    r.Term,
			})
		}
	}
}

func (r *Raft) resetElectionTimeout() {
	r.electionElapsed = 0
	r.nextElectionTimeout = (rand.Int() % r.electionTimeout) + r.electionTimeout
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.resetElectionTimeout()
	r.clearVotes()
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term += 1
	r.clearVotes()
	r.Vote = r.id
	r.resetElectionTimeout()
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Term:    r.Term,
		From:    r.id,
		To:      r.id,
		Reject:  false,
	})
}

func (r *Raft) sendRequestVotes() {
	for pid := range r.Prs {
		if pid == r.id {
			continue
		}
		r.push(pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			Term:    r.Term,
			From:    r.id,
			To:      pid,
		})
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.heartbeatElapsed = 0
	r.State = StateLeader
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{
			{Data: []byte{}},
		},
	})
	for pid := range r.Prs {
		if pid == r.id {
			continue
		}
		r.Prs[pid].Next = 1
		r.Prs[pid].Match = 0
		r.sendAppend(pid)
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.State != StateLeader {
			r.becomeCandidate()
			r.sendRequestVotes()
		}
		return nil
	case pb.MessageType_MsgBeat:
		if r.State != StateLeader {
			return errors.New("only leader can send heartbeats")
		}
		r.heartbeatElapsed = 0
		for pid := range r.Prs {
			if pid == r.id {
				continue
			}
			r.sendHeartbeat(pid)
		}
		return nil
	case pb.MessageType_MsgPropose:
		if r.State != StateLeader {
			return errors.New("only leader can propose new entry")
		}
	}

	if m.Term > r.Term {
		r.becomeFollower(m.Term, 0)
		//r.logger.Infof("%d become follower in Term %d", r.id, m.Term)
	}

	// `m.Term` should be less than or equal to `r.Term` at this point
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.resetElectionTimeout()
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.resetElectionTimeout()
	r.push(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		Term:    r.Term,
		From:    r.id,
		To:      m.From,
		Reject:  false,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVoteResponse:
		if m.Term != r.Term {
			break
		}
		if m.Reject {
			break
		}
		r.votes[m.From] = true
		if r.countVotes() >= r.majorityCount() {
			r.becomeLeader()
		}
	case pb.MessageType_MsgRequestVote:
		reject := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Term:    r.Term,
			From:    r.id,
			To:      m.From,
			Reject:  true,
		}
		r.push(reject)
	case pb.MessageType_MsgHeartbeat:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	}
}

func (r *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVote:
		reject := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Term:    r.Term,
			From:    r.id,
			To:      m.From,
			Reject:  true,
		}
		if m.Term < r.Term {
			r.push(reject)
			break
		}
		if r.Vote != 0 && r.Vote != m.From {
			r.push(reject)
			break
		}

		lastLogIndex := r.RaftLog.LastIndex()
		lastLogTerm, err := r.RaftLog.Term(lastLogIndex)
		if err != nil {
			panic(err)
		}
		if m.LogTerm < lastLogTerm || (m.LogTerm == lastLogTerm && m.Index < lastLogIndex) {
			r.push(reject)
			break
		}

		r.Vote = m.From
		rsp := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Term:    r.Term,
			From:    r.id,
			To:      m.From,
			Reject:  false,
		}
		r.push(rsp)

	case pb.MessageType_MsgHeartbeat:
		if m.Term < r.Term {
			r.push(pb.Message{
				MsgType: pb.MessageType_MsgHeartbeatResponse,
				Term:    r.Term,
				From:    r.id,
				To:      m.From,
				Reject:  true,
			})
			break
		}
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		if m.Term < r.Term {
			r.push(pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				Term:    r.Term,
				From:    r.id,
				To:      m.From,
				Reject:  true,
			})
			break
		}
		r.handleAppendEntries(m)
	default:

	}
}

func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgAppendResponse:
		if m.Term < r.Term {
			break
		}
	case pb.MessageType_MsgRequestVote:
		reject := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Term:    r.Term,
			From:    r.id,
			To:      m.From,
			Reject:  true,
		}
		r.push(reject)
	}
}

func (r *Raft) clearVotes() {
	r.Vote = 0
	for pid := range r.votes {
		r.votes[pid] = false
	}
}

func (r *Raft) countVotes() int {
	count := 0
	for _, b := range r.votes {
		if b {
			count += 1
		}
	}
	return count
}

func (r *Raft) majorityCount() int {
	return len(r.Prs)/2 + 1
}

func (r *Raft) push(msg pb.Message) {
	r.msgs = append(r.msgs, msg)
}

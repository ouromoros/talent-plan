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
	"github.com/cznic/mathutil"
	"github.com/cznic/sortutil"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
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

// Progress represents a follower???s progress in the view of the leader. Leader maintains
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
	votes   map[uint64]bool
	rejects map[uint64]bool

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
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	rlog := newLog(c.Storage)
	logger := log.New()
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	r := &Raft{
		id:               c.ID,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		RaftLog:          rlog,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		logger:           logger,
	}
	r.resetPeers(c.peers)
	return r
}

func (r *Raft) resetPeers(nodes []uint64) {
	prs := make(map[uint64]*Progress, 0)
	for _, pid := range nodes {
		prs[pid] = &Progress{}
	}
	votes := make(map[uint64]bool)
	for _, pid := range nodes {
		votes[pid] = false
	}
	rejects := make(map[uint64]bool)
	for _, pid := range nodes {
		rejects[pid] = false
	}
	r.Prs = prs
	r.votes = votes
	r.rejects = rejects
}

func (r *Raft) sendAppends() {
	for pid := range r.Prs {
		if pid == r.id {
			continue
		}
		r.sendAppend(pid)
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	nextIndex := r.Prs[to].Next
	if nextIndex <= r.RaftLog.snapIndex {
		r.sendSnapshot(to)
		return true
	}
	prevIndex := nextIndex - 1
	prevTerm, err := r.RaftLog.Term(prevIndex)
	if err != nil {
		panic(err)
	}
	ents := r.RaftLog.getEntries(nextIndex, r.RaftLog.LastIndex()+1)
	appendEntries := make([]*pb.Entry, 0, len(ents))
	for _, ent := range ents {
		ent := ent
		appendEntries = append(appendEntries, &ent)
	}
	if len(appendEntries) > 0 {
		if appendEntries[0].Index != prevIndex+1 {
			panic("prevIndex and Entries not match")
		}
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
	r.push(appendMsg)
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
	if term > r.Term {
		r.clearVotes()
		r.Term = term
	}
	r.State = StateFollower
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
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, err := r.RaftLog.Term(lastIndex)
		if err != nil {
			panic(err)
		}
		r.push(pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			Term:    r.Term,
			From:    r.id,
			To:      pid,
			LogTerm: lastTerm,
			Index:   lastIndex,
		})
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.heartbeatElapsed = 0
	r.State = StateLeader
	for pid := range r.Prs {
		if pid == r.id {
			continue
		}
		r.Prs[pid].Next = r.RaftLog.snapIndex + 1
		r.Prs[pid].Match = r.RaftLog.snapIndex
	}
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{
			{Data: nil},
		},
	})
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
		r.RaftLog.addEntry(r.Term, m.Entries[0].Data)
		r.Prs[r.id].Match = r.RaftLog.LastIndex()
		r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
		r.maybeIncrCommitIndex()
		for pid := range r.Prs {
			if pid == r.id {
				continue
			}
			r.sendAppend(pid)
		}
		return nil
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
	r.Lead = m.From
	ents := make([]pb.Entry, 0, len(m.Entries))
	for _, ent := range m.Entries {
		ents = append(ents, *ent)
	}
	match := r.RaftLog.appendEntries(m.LogTerm, m.Index, m.Commit, ents)
	var logTerm uint64
	if match {
		if len(m.Entries) > 0 {
			logTerm = m.Entries[len(m.Entries)-1].Term
		} else {
			logTerm = m.LogTerm
		}
		// When success, set LogTerm and Index to matched
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			LogTerm: logTerm,
			Index:   m.Index + uint64(len(m.Entries)),
			Term:    r.Term,
			From:    r.id,
			To:      m.From,
			Reject:  false,
		}
		r.push(msg)
	} else {
		// When reject, set Index to next expected
		if m.Index == r.RaftLog.snapIndex {
			panic("fail on snap")
		}
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			Index:   m.Index,
			LogTerm: m.LogTerm,
			Term:    r.Term,
			From:    r.id,
			To:      m.From,
			Reject:  true,
		}
		r.push(msg)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.resetElectionTimeout()
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, err := r.RaftLog.Term(lastLogIndex)
	if err != nil {
		panic(err)
	}
	r.push(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		Term:    r.Term,
		From:    r.id,
		To:      m.From,
		LogTerm: lastLogTerm,
		Index:   lastLogIndex,
		Reject:  false,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	r.Lead = m.From
	if m.Snapshot.Metadata.Index <= r.RaftLog.committed {
		log.Errorf("No need to apply Snapshot %v", m.Snapshot.Metadata)
		return
	}

	r.RaftLog.ApplySnapshot(m.Snapshot)
	nodes := m.Snapshot.Metadata.ConfState.Nodes
	r.resetPeers(nodes)
	// Set Vote to current leader to avoid corner case
	r.Vote = m.From
}

func (r *Raft) sendSnapshot(to uint64) {
	snap, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		log.Panicf("Getting Snapshot from storage failed with error: %v", err)
	}
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		Term:     r.Term,
		From:     r.id,
		To:       to,
		Snapshot: &snap,
	}
	r.push(msg)
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
			r.rejects[m.From] = true
			if r.countRejects() > r.majorityCount()-1 {
				r.becomeFollower(r.Term, 0)
			}
			break
		}
		r.votes[m.From] = true
		if r.countVotes() >= r.majorityCount() {
			r.becomeLeader()
			r.sendAppends()
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
				Index:   m.Index,
				LogTerm: m.LogTerm,
				Reject:  true,
			})
			break
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		if m.Term < r.Term {
			break
		}
		r.handleSnapshot(m)
	}
}

func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgAppendResponse:
		if m.Term < r.Term {
			break
		}
		if m.Reject {
			if m.Index <= r.RaftLog.snapIndex {
				//log.Panicf("append from snapIndex failed: %v", m)
				r.sendSnapshot(m.From)
				break
			}
			r.Prs[m.From].Next = mathutil.MinUint64Val(r.Prs[m.From].Next, m.Index)
		} else {
			if r.Prs[m.From].Match < m.Index {
				if m.Index > r.RaftLog.LastIndex() {
					log.Panicf("matched index larger than lastIndex: %v", m)
				}
				r.Prs[m.From].Match = m.Index
				r.Prs[m.From].Next = m.Index + 1
				if r.maybeIncrCommitIndex() {
					r.sendAppends()
				}
			}
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
	case pb.MessageType_MsgHeartbeatResponse:
		if m.Term < r.Term {
			break
		}
		if m.Index < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	}
}

func (r *Raft) maybeIncrCommitIndex() bool {
	matches := make([]uint64, 0, len(r.Prs))
	for _, prg := range r.Prs {
		matches = append(matches, prg.Match)
	}
	sort.Sort(sortutil.Uint64Slice(matches))
	halfIndex := (len(matches) - 1) / 2
	commitIndex := matches[halfIndex]
	if r.RaftLog.committed < commitIndex {
		commitTerm, err := r.RaftLog.Term(commitIndex)
		if err != nil {
			panic(err)
		}
		if commitTerm == r.Term {
			r.RaftLog.committed = commitIndex
			return true
		}
	}
	return false
}

func (r *Raft) clearVotes() {
	r.Vote = 0
	for pid := range r.votes {
		r.votes[pid] = false
	}
	for pid := range r.votes {
		r.rejects[pid] = false
	}
}

func (r *Raft) countRejects() int {
	count := 0
	for _, b := range r.rejects {
		if b {
			count += 1
		}
	}
	return count
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

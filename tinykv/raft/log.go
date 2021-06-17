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
	"github.com/cznic/mathutil"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	pendingEntries []pb.Entry

	snapIndex uint64
	snapTerm  uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hardState, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	snapIndex := firstIndex - 1
	snapTerm, err := storage.Term(snapIndex)
	if err != nil {
		panic(err)
	}
	log := &RaftLog{
		storage:        storage,
		committed:      hardState.Commit,
		applied:        snapIndex,
		stabled:        lastIndex,
		entries:        entries,
		pendingEntries: make([]pb.Entry, 0),
		snapIndex:      snapIndex,
		snapTerm:       snapTerm,
	}
	return log
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.pendingEntries
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return l.getEntries(l.applied+1, l.committed+1)
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.snapIndex + uint64(len(l.entries))
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i < l.snapIndex {
		panic("Index not available")
	}
	if i == l.snapIndex {
		return l.snapTerm, nil
	}
	return l.entries[i-l.snapIndex-1].Term, nil
}

// Used by propose new Entry
func (l *RaftLog) addEntry(term uint64, data []byte) {
	newEntry := pb.Entry{
		Index: l.LastIndex() + 1,
		Term:  term,
		Data:  data,
	}
	l.entries = append(l.entries, newEntry)
	l.pendingEntries = append(l.pendingEntries, newEntry)
}

// Used by follower for appendEntries
func (l *RaftLog) appendEntries(prevTerm uint64, prevIndex uint64, commitIndex uint64, ents []pb.Entry) bool {
	if prevIndex > l.LastIndex() {
		return false
	}
	t, err := l.Term(prevIndex)
	if err != nil {
		return false
	}
	if t != prevTerm {
		return false
	}
	if len(ents) <= 0 {
		if commitIndex > l.committed {
			canCommit := mathutil.MinUint64Val(commitIndex, prevIndex+uint64(len(ents)))
			l.committed = mathutil.MaxUint64(l.committed, canCommit)
		}
		return true
	}
	changeEnts := findMergeEntries(l.entries, ents)
	l.pendingEntries = mergeEntries(l.pendingEntries, changeEnts)
	l.entries = mergeEntries(l.entries, changeEnts)
	if commitIndex > l.committed {
		canCommit := mathutil.MinUint64Val(commitIndex, prevIndex+uint64(len(ents)))
		l.committed = mathutil.MaxUint64(l.committed, canCommit)
	}
	return true
}

func (l *RaftLog) getEntries(startIndex uint64, endIndex uint64) []pb.Entry {
	if startIndex > l.LastIndex() {
		return []pb.Entry{}
	}
	return append([]pb.Entry{}, l.entries[startIndex-l.snapIndex-1:endIndex-l.snapIndex-1]...)
}

func (l *RaftLog) Advance() {
	l.stabled = l.LastIndex()
	l.applied = l.committed
	l.pendingEntries = nil
}

func findMergeEntries(a []pb.Entry, b []pb.Entry) []pb.Entry {
	if len(b) == 0 {
		return a
	}
	if len(a) == 0 {
		return b
	}
	bStartIndex := b[0].Index
	bLastIndex := b[len(b)-1].Index
	aStartIndex := a[0].Index
	aLastIndex := a[len(a)-1].Index

	if bStartIndex > aLastIndex+1 {
		return nil
	} else if bStartIndex == aLastIndex+1 {
		return b
	}
	checkStartIndex := mathutil.MaxUint64(bStartIndex, aStartIndex)
	checkLastIndex := mathutil.MinUint64(bLastIndex, aLastIndex)
	for i := checkStartIndex; i <= checkLastIndex; i++ {
		if a[i-aStartIndex].Term != b[i-bStartIndex].Term {
			return b[i-bStartIndex:]
		}
	}
	return b[checkLastIndex-bStartIndex+1:]
}

func mergeEntries(a []pb.Entry, b []pb.Entry) []pb.Entry {
	if len(b) == 0 {
		return a
	}
	if len(a) == 0 {
		return b
	}
	bStartIndex := b[0].Index
	aStartIndex := a[0].Index
	aLastIndex := a[len(a)-1].Index

	if bStartIndex > aLastIndex+1 {
		panic("impossible")
	}
	return append(a[:bStartIndex-aStartIndex], b...)
}

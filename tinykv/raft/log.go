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
	log := &RaftLog{
		storage:   storage,
		committed: hardState.Commit,
		applied:   firstIndex - 1,
		stabled:   lastIndex,
		entries:   make([]pb.Entry, 0),
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
	if len(l.entries) < 0 {
		return nil
	}
	firstUnstable := l.stabled - l.entries[0].Index + 1
	if firstUnstable >= uint64(len(l.entries)) {
		return nil
	}
	return l.entries[firstUnstable:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return l.getEntries(l.applied+1, l.committed+1)
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	li := l.stabled
	if len(l.entries) > 0 {
		li = mathutil.MaxUint64(li, l.entries[len(l.entries)-1].Index)
	}
	return li
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i > l.stabled {
		return 0, errors.New("index out of range")
	}
	return l.storage.Term(i)
}

// Used by propose new Entry
func (l *RaftLog) addEntry(term uint64, data []byte) {
	l.entries = append(l.entries, pb.Entry{
		Index: l.LastIndex() + 1,
		Term:  term,
		Data:  data,
	})
}

// Used by follower for appendEntries
func (l *RaftLog) appendEntries(prevTerm uint64, prevIndex uint64, commitIndex uint64, ents []pb.Entry) bool {
	t, err := l.Term(prevIndex)
	if err != nil {
		return false
	}
	if t != prevTerm {
		return false
	}
	if len(ents) <= 0 {
		return true
	}
	mergeEnts := l.getMergeEntries(ents)
	l.entries = mergeEnts
	l.committed = mathutil.MaxUint64(commitIndex, l.committed)
	return true
}

// Return entries that are to be merged.
func (l *RaftLog) getMergeEntries(ents []pb.Entry) []pb.Entry {
	if len(ents) <= 0 {
		return nil
	}
	firstEntIndex := ents[0].Index
	lastEntIndex := ents[len(ents)-1].Index
	firstStorageIndex, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	checkFirstIndex := mathutil.MaxUint64(firstEntIndex, firstStorageIndex)
	checkLastIndex := mathutil.MinUint64(lastEntIndex, l.stabled)
	for i := checkFirstIndex; i < checkLastIndex; i++ {
		storageTerm, err := l.storage.Term(i)
		if err != nil {
			panic(err)
		}
		entTerm := ents[i-firstEntIndex].Index
		if entTerm != storageTerm {
			return ents[i-firstEntIndex:]
		}
	}
	return ents[checkLastIndex-firstEntIndex:]
}

func (l *RaftLog) getEntries(startIndex uint64, endIndex uint64) []pb.Entry {
	if startIndex > l.LastIndex() {
		return []pb.Entry{}
	}

	stableEntries := make([]pb.Entry, 0)
	unstableEntries := make([]pb.Entry, 0)
	var err error
	if startIndex < l.stabled {
		stableEntries, err = l.storage.Entries(startIndex, mathutil.MinUint64(endIndex-1, l.stabled)+1)
		if err != nil {
			panic(err)
		}
	}
	if len(l.entries) > 0 {
		start := mathutil.MaxUint64(l.entries[0].Index, startIndex)
		end := mathutil.MinUint64(l.entries[len(l.entries)-1].Index+1, endIndex)
		unstableEntries = l.entries[start-l.entries[0].Index : end-l.entries[0].Index]
	}
	return mergeEntries(stableEntries, unstableEntries)
}

func mergeEntries(stable []pb.Entry, unstable []pb.Entry) []pb.Entry {
	if len(unstable) == 0 {
		return stable
	}
	if len(stable) == 0 {
		return unstable
	}
	cut := unstable[0].Index - stable[0].Index
	return append(stable[:cut], unstable...)
}

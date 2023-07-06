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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
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
	truncatedIndex uint64
	truncatedTerm  uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	fi, err := storage.FirstIndex()
	if err != nil {
		panic(err.Error())
	}
	li, err := storage.LastIndex()
	if err != nil {
		panic(err.Error())
	}
	entries, err := storage.Entries(fi, li+1)
	if err != nil {
		panic(err.Error())
	}

	// LastIndex should have been applied
	var stabled, applied uint64 = li, li
	if len(entries) != 0 {
		stabled = entries[len(entries)-1].Index
		applied = entries[0].Index - 1
	}

	hs, _, err := storage.InitialState()
	if err != nil {
		panic(err.Error())
	}

	term, err := storage.Term(fi - 1)
	if err != nil {
		panic(err.Error())
	}

	return &RaftLog{
		storage:   storage,
		committed: hs.Commit,
		applied:   applied,
		stabled:   stabled,
		entries:   entries,

		pendingSnapshot: nil,

		truncatedIndex: fi - 1,
		truncatedTerm:  term,
	}
}

func (l RaftLog) getEntry(i uint64) *pb.Entry {
	offset := l.offset()
	return &l.entries[i-offset]
}

// return entries in range [args[0], args[1])
// or return entries in range [args[0]:]
func (l RaftLog) getEntries(args ...uint64) []pb.Entry {
	offset := l.offset()
	if len(args) == 1 {
		return l.entries[args[0]-offset:]
	} else if len(args) == 2 {
		return l.entries[args[0]-offset : args[1]-offset]
	}
	panic("")
}

func (l RaftLog) getEntriesRef(args ...uint64) []*pb.Entry {
	offset := l.offset()
	entries := make([]*pb.Entry, 0)
	if len(args) == 1 {
		for i := args[0] - offset; i < uint64(len(l.entries)); i++ {
			entries = append(entries, &l.entries[i])
		}
		return entries
	} else if len(args) == 2 {
		for _, entry := range l.entries[args[0]-offset : args[1]-offset] {
			entries = append(entries, &entry)
		}
		return entries
	}
	panic("")
}

// return the index of the last log entry
func (l RaftLog) logIndex() uint64 {
	if len(l.entries) == 0 {
		return l.truncatedIndex
	}
	return l.entries[len(l.entries)-1].Index
}

func (l RaftLog) offset() uint64 {
	if len(l.entries) == 0 {
		return l.truncatedIndex
	}
	return l.entries[0].Index
}

func (l RaftLog) logTerm() uint64 {
	if len(l.entries) == 0 {
		return l.truncatedTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return []pb.Entry{}
	}
	return l.entries[l.stabled-l.offset()+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	return l.entries[l.applied+1-l.offset() : l.committed+1-l.offset()]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return l.truncatedIndex
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	offset := l.offset()
	if i >= offset && i-offset < uint64(len(l.entries)) {
		return l.entries[i-offset].Term, nil
	} else if i == l.truncatedIndex {
		return l.truncatedTerm, nil
	}
	return 0, ErrUnavailable
}

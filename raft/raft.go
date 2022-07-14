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
	//"fmt"
	"math/rand"
	"sort"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
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
	electionTimeout int
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
	randelectionTimeout int
}

func Min(a, b uint64) uint64 {
	if a >= b {
		return b
	}
	return a
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	prs := make(map[uint64]*Progress)

	// votes records
	votes := make(map[uint64]bool)
	hardState, _, _ := c.Storage.InitialState()
	// msgs need to send
	raft := &Raft{
		id: c.ID, 
		Term: hardState.Term, 
		Vote: hardState.Vote,
		Prs: prs, State: StateFollower, 
		RaftLog: newLog(c.Storage), 
		votes: votes,
		Lead: None,
		heartbeatTimeout: c.HeartbeatTick, 
		electionTimeout: c.ElectionTick, 
		randelectionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
		electionElapsed: 0,
		heartbeatElapsed: 0,
	}
	if c.Applied > 0 {
		raft.RaftLog.applied = c.Applied
	}
	lastIndex := raft.RaftLog.LastIndex() 
	for _, idx := range c.peers {
		prs[idx] = &Progress{Next: lastIndex + 1, Match: 0}
	}
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)
	idx := r.Prs[to].Next - r.RaftLog.FirstIndex() 
	var entries []*pb.Entry
	for ; int(idx) < len(r.RaftLog.entries); idx++ {
		entries = append(entries, &r.RaftLog.entries[idx])
	}	
	msg := pb.Message {
		MsgType: pb.MessageType_MsgAppend,
		To: to,
		From: r.id,
		Term: r.Term,
		Index: prevLogIndex,
		LogTerm: prevLogTerm,
		Entries: entries,
		Commit: r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

func (r *Raft) sendAppendResponse(to uint64, reject bool) bool {
	msg := pb.Message {
		MsgType: pb.MessageType_MsgAppendResponse,
		To: to,
		From: r.id,
		Term: r.Term,
		Reject: reject, 
		Index: r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) bool{
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To: to,
		From: r.id,
		Term: r.Term,
		Commit: r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) bool{
	msg := pb.Message {
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To: to,
		From: r.id,
		Term: r.Term,
		Reject: reject, 
		Commit: r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

func (r *Raft) sendRequestVote(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To: to,
		From: r.id,
		Term: r.Term,
		LogTerm: r.RaftLog.LastTerm(),
		Index: r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, msg)
	return
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) bool {
	msg := pb.Message {
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To: to,
		From: r.id,
		Term: r.Term,
		Reject: reject,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randelectionTimeout {
			r.electionElapsed = 0
			m := pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id, Term: r.Term,}
			r.Step(m)
		}
	case StateLeader:
		r.heartbeatElapsed++
		r.electionElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			m := pb.Message{MsgType: pb.MessageType_MsgBeat, From: r.id, Term: r.Term,}
			r.Step(m)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if term >= r.Term {
		r.Term = term
		r.Lead = lead
		r.Vote = None
		r.State = StateFollower
		r.votes = make(map[uint64]bool)
		r.randelectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.Vote = r.id
	r.Lead = None
	r.randelectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.Lead = r.id
	r.randelectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	for _, pro := range r.Prs {
		pro.Next = r.RaftLog.LastIndex() + 1
		pro.Match = 0
	}
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1,})
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next++

	for key, _ := range r.Prs {
		if key != r.id {
			r.sendAppend(key)
		}
	}
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			for key, _ := range r.Prs {
				if key != r.id {
					r.sendRequestVote(key)
				}	
			}
			if len(r.Prs) == 1 {
				r.becomeLeader()
			}

		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)

		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)

		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)

		case pb.MessageType_MsgTimeoutNow:
			r.electionElapsed = 0
			r.becomeCandidate()
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			for key, _ := range r.Prs {
				if key != r.id {
					r.sendRequestVote(key)
				}
			}
			if len(r.Prs) == 1 {
				r.becomeLeader()
			}
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
			

		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)

		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)

		case pb.MessageType_MsgRequestVoteResponse:
			r.votes[m.From] = !m.Reject
			agree, reject := 0, 0
			for _, val := range r.votes {
				if val == true {
					agree++
				} else {
					reject++
				}
			}
			length := len(r.Prs)/2
			if agree > length {
				r.becomeLeader()
			} else if reject > length {
				r.becomeFollower(m.Term, None)
			}
		case pb.MessageType_MsgTimeoutNow:

		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
		case pb.MessageType_MsgBeat:
			for key, _ := range r.Prs {
				if key != r.id {
					r.sendHeartbeat(key)
				}
			}

		case pb.MessageType_MsgPropose:
			r.handlePropose(m)

		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)

		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)

		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m)

		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)

		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
			
		case pb.MessageType_MsgTransferLeader:

		case pb.MessageType_MsgTimeoutNow:

		}
	}
	return nil
}

func (r *Raft) handleRequestVote(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	if r.Vote == None || r.Vote == m.From {
		lastIndex := r.RaftLog.LastIndex()
		lastTerm := r.RaftLog.LastTerm()
		if (lastIndex <= m.Index && lastTerm == m.LogTerm) || lastTerm < m.LogTerm {
			r.Vote = m.From
			r.sendRequestVoteResponse(m.From, false)
			return
		} 
	}
	r.sendRequestVoteResponse(m.From, true)
	return
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	r.becomeFollower(m.Term, m.From)
	r.electionElapsed = 0
	r.heartbeatElapsed = 0

	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = Min(m.Index + uint64(len(m.Entries)), m.Commit)
		// for r.RaftLog.applied < r.RaftLog.committed {
		// 	r.RaftLog.applied++
		// }
	}
	r.sendHeartbeatResponse(m.From, false)
	return
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Term < r.Term {
		return 
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if m.Commit < r.RaftLog.committed {
		r.sendAppend(m.From)
	}
	//we need to update commit, too. like handleAppend Response
	match := make([]uint64, len(r.Prs))
	for i, val := range r.Prs {
		match[i - 1] = val.Match
	}
	sort.Slice(match, func(i, j int) bool {return match[i] <= match[j]})

	index := match[(len(r.Prs) - 1) / 2]
	term, _ := r.RaftLog.Term(index)

	if index > r.RaftLog.committed && term == r.Term {
		r.RaftLog.committed = index
		// for r.RaftLog.applied < r.RaftLog.committed {
		// 	r.RaftLog.applied++
		// }
		for key, _  := range r.Prs {
			if key != r.id {
				r.sendAppend(key)
			}
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true)
		return
	}
	r.becomeFollower(m.Term, m.From)
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	
	if m.Index <= r.RaftLog.LastTerm() {
		term, err := r.RaftLog.Term(m.Index) 
		if err != nil || term != m.LogTerm{
			r.sendAppendResponse(m.From, true)
			return
		}
		i, j := 0, 0
		if len(r.RaftLog.entries) != 0 && len(m.Entries) != 0{
			i = int(m.Entries[0].Index - r.RaftLog.entries[0].Index)
			for j < len(m.Entries) && i < len(r.RaftLog.entries) && 
				m.Entries[j].Index == r.RaftLog.entries[i].Index &&
				m.Entries[j].Term == r.RaftLog.entries[i].Term{
				i++
				j++
			}
			if j != len(m.Entries){//all matched don't need to delete some message so don't change it
				r.RaftLog.entries = r.RaftLog.entries[0: i]
				if i > 0 {
					r.RaftLog.stabled = Min(r.RaftLog.entries[i - 1].Index, r.RaftLog.stabled)
				}else {
					r.RaftLog.stabled = 0
				}
			}	
		}

		for ; j < len(m.Entries); j++ {
			r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[j])
		}
		if m.Commit > r.RaftLog.committed {
			r.RaftLog.committed = Min(m.Index + uint64(len(m.Entries)), m.Commit)
			// for r.RaftLog.applied < r.RaftLog.committed {
			// 	r.RaftLog.applied++
			// }
		}
		r.sendAppendResponse(m.From, false)
		return
	}
	r.sendAppendResponse(m.From, true)
	return
}



func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Term < r.Term {
		return
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if m.Reject {
		if r.Prs[m.From].Next - 1 > 0 {
			r.Prs[m.From].Next--
			r.sendAppend(m.From)
		}
		return
	}
	r.Prs[m.From].Next = m.Index + 1
	r.Prs[m.From].Match = m.Index

	match := make([]uint64, len(r.Prs))
	for i, val := range r.Prs {
		match[i - 1] = val.Match
	}
	sort.Slice(match, func(i, j int) bool {return match[i] <= match[j]})

	index := match[(len(r.Prs) - 1) / 2]
	term, _ := r.RaftLog.Term(index)

	if index > r.RaftLog.committed && term == r.Term {
		r.RaftLog.committed = index
		// for r.RaftLog.applied < r.RaftLog.committed {
		// 	r.RaftLog.applied++
		// }
		for key, _  := range r.Prs {
			if key != r.id {
				r.sendAppend(key)
			}
		}
	}
	return 
}

func (r *Raft) handlePropose(m pb.Message) {
	for _, entry := range m.Entries {
		entry.Index = r.RaftLog.LastIndex() + 1
		entry.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	for key, _ := range r.Prs {
		if key != r.id {
			r.sendAppend(key)
		}
	}
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
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

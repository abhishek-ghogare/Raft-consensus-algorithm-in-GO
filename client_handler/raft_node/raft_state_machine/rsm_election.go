package raft_state_machine

import (
    "math/rand"
)

/********************************************************************
 *                                                                  *
 *                          Vote Request                            *
 *                                                                  *
 ********************************************************************/
func (state *StateMachine) voteRequest(event RequestVoteEvent) (actions []interface{}) {

    actions = make([]interface{}, 0)

    // Track if persistent state of raft state machine changes
    state_changed_flag := false
    // Check and store state on persistent store
    defer func() {
        if state_changed_flag {
            // Prepend StateStore action
            actions = append(actions, state.GetStateStoreAction())
        }
    }()

    if event.Term < state.CurrentTerm {
        // In any state, if old termed candidate request vote, tell it to be a follower
        voteResp := RequestVoteRespEvent{FromId: state.server_id, Term: state.CurrentTerm, VoteGranted: false}
        resp := SendAction{ToId: event.FromId, Event: voteResp}
        actions = append(actions, resp)
        return actions
    } else if event.Term > state.CurrentTerm {
        // Request from more up-to-date node, so lets update our state
        state.CurrentTerm = event.Term
        state.myState = FOLLOWER
        state.VotedFor = -1
        state_changed_flag = true
    }

    // requester_term >= server.current_term
    // If not voted for this term
    if state.VotedFor == -1 {
        // votedFor will be -1 ONLY for follower state, in case of leader/candidate it will be set to self id
        if event.LastLogTerm > state.GetLastLogTerm() || event.LastLogTerm == state.GetLastLogTerm() && event.LastLogIndex >= state.GetLastLogIndex() {
            state.VotedFor = event.FromId
            state.CurrentTerm = event.Term
            state_changed_flag = true

            voteResp := RequestVoteRespEvent{FromId: state.server_id, Term: state.CurrentTerm, VoteGranted: true}
            resp := SendAction{ToId: event.FromId, Event: voteResp}
            actions = append(actions, resp)
            return actions
        }
    } else {
        // If voted for this term, check if request is from same candidate for which this node has voted
        if state.VotedFor == event.FromId {
            // Vote again to same candidate
            voteResp := RequestVoteRespEvent{FromId: state.server_id, Term: state.CurrentTerm, VoteGranted: true}
            resp := SendAction{ToId: event.FromId, Event: voteResp}
            actions = append(actions, resp)
            return actions
        }
    }

    // For already voted for same term to different candidate,
    // Or not voted but requester's logs are old,
    // reject all requests
    voteResp := RequestVoteRespEvent{FromId: state.server_id, Term: state.CurrentTerm, VoteGranted: false}
    resp := SendAction{ToId: event.FromId, Event: voteResp}
    actions = append(actions, resp)
    return actions
}

/********************************************************************
 *                                                                  *
 *                      Vote Request Response                       *
 *                                                                  *
 ********************************************************************/
func (state *StateMachine) voteRequestResponse(event RequestVoteRespEvent) (actions []interface{}) {

    actions = make([]interface{}, 0)

    // Track if persistent state of raft state machine changes
    state_changed_flag := false
    // Check and store state on persistent store
    defer func() {
        if state_changed_flag {
            // Prepend StateStore action
            actions = append(actions, state.GetStateStoreAction())
        }
    }()

    if state.CurrentTerm < event.Term {
        // This server term is not so up-to-date, so update
        state.myState = FOLLOWER
        state.CurrentTerm = event.Term
        state.VotedFor = -1
        state_changed_flag = true

        alarm := AlarmAction{Time: state.ElectionTimeout + rand.Intn(state.ElectionTimeout)} // slightly greater time to receive heartbeat
        actions = append(actions, alarm)
        return actions
    } else if state.CurrentTerm > event.Term {
        // Simply drop the response
        return actions
    }

    switch state.myState {
    case LEADER, FOLLOWER:
        return actions

    case CANDIDATE:
        // Refer comments @ receivedVote declaration
        // If vote received from a node, we are storing the term in receivedVote array for which the vote has received.
        // This way we don't need to reinitialise the voted for array every time new election starts
        vote := state.receivedVote[event.FromId]
        if vote < 0 {
            vote = -vote
        }

        if vote < event.Term {
            if event.VoteGranted {
                state.receivedVote[event.FromId] = event.Term
            } else {
                state.receivedVote[event.FromId] = -event.Term
            }
            count := 0
            ncount := 0
            for _, vote := range state.receivedVote {
                if vote == event.Term {
                    count++
                } else if vote == -event.Term {
                    ncount++
                }
            }
            //fmt.Printf("eventTerm:%v\n COUNTING : %v : %v : %v\n",event.term, count,ncount, server.receivedVote)

            if ncount > state.numberOfNodes/2 {
                // majority of -ve votes, so change to follower
                state.myState = FOLLOWER
                return actions
            } else if count > state.numberOfNodes/2 {
                // become leader

                state.log_info(3, "Leader has been elected : %v", state.server_id)
                state.initialiseLeader()

                appendReq := AppendRequestEvent{
                    FromId:       state.server_id,
                    Term:         state.CurrentTerm,
                    PrevLogIndex: state.GetLastLogIndex(),
                    PrevLogTerm:  state.GetLastLogTerm(),
                    Entries:      []LogEntry{},
                    LeaderCommit: state.commitIndex}

                alarm := AlarmAction{Time: state.HeartbeatTimeout}
                actions = append(actions, alarm)
                appendReqActions := state.broadcast(appendReq)
                actions = append(actions, appendReqActions...)
            }
        }
    }

    return actions
}

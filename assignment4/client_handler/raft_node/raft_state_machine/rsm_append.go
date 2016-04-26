package raft_state_machine

import (
    "sort"
    "math/rand"
)

/********************************************************************
 *                                                                  *
 *                     Append from client                           *
 *                                                                  *
 ********************************************************************/
func (state *StateMachine) appendClientRequest(event *[]AppendEvent) (actions []interface{}) {

    actions = []interface{}{}

    switch state.myState {
    case LEADER:
        prevLogIndex := state.GetLastLogIndex()
        prevLogTerm  := state.GetLastLogTerm()

        logs := []LogEntry{}
        for _, ev := range *event {
            log := LogEntry{Index: state.GetLastLogIndex() + 1, Term: state.CurrentTerm, Data: ev.Data}
            state.PersistentLog.Append(log)
            logs = append(logs, log)
        }

        appendReq := AppendRequestEvent{
            FromId:       state.server_id,
            Term:         state.CurrentTerm,
            PrevLogIndex: prevLogIndex,
            PrevLogTerm:  prevLogTerm,
            Entries:      logs,
            LeaderCommit: state.commitIndex}
        // Append to self log
        state.matchIndex[state.server_id] = state.GetLastLogIndex()    // Update self matchIndex


        actions = append(actions, state.broadcast(appendReq)...)
    case CANDIDATE:
        fallthrough
    case FOLLOWER:
        // Return not a leader commit action
        for _, ev := range *event {
            action := CommitAction{
                Index   :-1,
                Data    : ev.Data,
                Err     : Error_NotLeader{ LeaderId : state.GetCurrentLeader()} }

            actions = append(actions, action)
        }

        state.log_warning(3, "Not a leader, redirecting to leader")
    }
    return actions
}



/********************************************************************
 *                                                                  *
 *                          Append Request                          *
 *                                                                  *
 ********************************************************************/
func (state *StateMachine) appendRequest(event AppendRequestEvent) (actions []interface{}) {
    actions = make([]interface{}, 0)

    // Track if persistent state of raft state machine changes
    state_changed_flag := false
    // Check and store state on persistent store
    defer func() {
        if state_changed_flag {
            // Prepend StateStore action
            state.log_info(3, "Appending state store action")
            actions = append(actions, state.GetStateStoreAction())
        }
    }()


    if state.CurrentTerm > event.Term {
        // Append request is not from latest leader
        // In all states applicable
        appendResp := AppendRequestRespEvent{FromId: state.server_id, Term: state.CurrentTerm, Success: false, LastLogIndex: state.GetLastLogIndex()}
        resp := SendAction{ToId: event.FromId, Event: appendResp}
        actions = append(actions, resp)
        return actions
    }

    switch state.myState {
    case LEADER:
        // mystate == leader && term == currentTerm, this is impossible, as two leaders will never be elected at any term
        if event.Term == state.CurrentTerm {
            appendResp := AppendRequestRespEvent{FromId: state.server_id, Term: -1, Success: false, LastLogIndex: state.GetLastLogIndex()}
            resp := SendAction{ToId: event.FromId, Event: appendResp}
            actions = append(actions, resp)
            return actions
        }
        // continue flow to next case for server.currentTerm < event.term
        fallthrough
    case CANDIDATE:
        // Convert to follower if current state is candidate/leader
        state.myState = FOLLOWER
        // continue flow to next case
        fallthrough
    case FOLLOWER:
        // Reset heartbeat timeout
        alarm := AlarmAction{Time: state.ElectionTimeout + rand.Intn(state.ElectionTimeout)} // slightly greater time to receive heartbeat
        actions = append(actions, alarm)

        // Check term
        if state.CurrentTerm < event.Term {
            // This server term is not so up-to-date, so update
            state.CurrentTerm = event.Term
            state.VotedFor = -1
            state_changed_flag = true
        }

        // Check if the requester is leader
        if state.CurrentTerm == event.Term {
            state.currentLdr = event.FromId     // current leader is the one from whom msg received
        }


        requestLogsFrom := int64(-1)
        if state.GetLastLogIndex() < event.PrevLogIndex {   // Check if previous entries are missing
            requestLogsFrom = state.GetLastLogIndex()+1     // Request logs from (last log index + 1)
        } else if state.GetLogAt(event.PrevLogIndex).Term  !=  event.PrevLogTerm { // Last log terms does not match
            requestLogsFrom = event.PrevLogIndex            // Request logs from PrevLogIndex
        }
        if requestLogsFrom != int64(-1) {
            appendResp := AppendRequestRespEvent {          // Negative ack for
                FromId          : state.server_id,
                Term            : state.CurrentTerm,
                Success         : false,
                LastLogIndex    : requestLogsFrom-1 }       // Request logs from requestLogsFrom
            resp := SendAction{ToId: event.FromId, Event: appendResp}
            actions = append(actions, resp)
            return actions
        }


        logsToAppend := event.Entries

        // remove logs from logsToAppend which are present in our logs
        for len(logsToAppend)       >   0 &&                                        // logs to append is non empty list
        state.GetLastLogIndex() >=  logsToAppend[0].Index &&                    // if there is still intersection between our logs and logs to append
        logsToAppend[0].Term    ==  state.GetLogAt(logsToAppend[0].Index).Term {// if term match -> logs match -> we have this log
            logsToAppend = logsToAppend[1:]                                         // skip matched log
        }

        // Check if we have outdated/garbage logs
        if len(logsToAppend)>0 && state.GetLastLogIndex() >= logsToAppend[0].Index {
            // There are entries from last leaders
            // truncate them up to the end
            state.log_info(3, "Extra logs found, PrevLogIndex was %v, trucating logs from %v to %v", event.PrevLogIndex, logsToAppend[0].Index, state.GetLastLogIndex())
            truncatedLogs := state.truncateLogsFrom(logsToAppend[0].Index)
            for _, log := range *truncatedLogs {
                action := CommitAction{Index:-1, Data: log.Data, Err: Error_Commit{}}
                actions = append(actions, action)
            }
        }

        // Update log if entries are not present
        for _, log := range logsToAppend {
            state.PersistentLog.Append(log)
        }

        if event.LeaderCommit > state.commitIndex {
            var commitFrom, commitUpto int64
            // If leader has commited entries, so should this server
            if event.LeaderCommit <= state.GetLastLogIndex() {
                commitFrom = state.commitIndex + 1
                commitUpto = event.LeaderCommit
            } else {
                commitFrom = state.commitIndex + 1
                commitUpto = state.GetLastLogIndex()
            }

            // Loads logs from persistent store from commitIndex to end if not in in-memory logs
            state.commitIndex = commitUpto

            // Commit all logs from commitFrom to commitUpto
            state.log_info(3, "Commiting from index %v to %v", commitFrom, commitUpto)
            for i := commitFrom; i <= commitUpto; i++ {
                action := CommitAction{Index:i, Data: state.GetLogAt(i).Data, Err: nil}
                actions = append(actions, action)
            }
        }

    }

    // If the append request is heartbeat then ignore responding to it if we are up-to-date with leader
    // We are updating matchIndex and nextIndex on positive appendRequestResponse, so consume heartbeats
    if len(event.Entries) != 0 {
        appendResp := AppendRequestRespEvent{
            FromId      : state.server_id,
            Term        : state.CurrentTerm,
            Success     : true,
            LastLogIndex: state.GetLastLogIndex() }
        resp := SendAction{ToId: event.FromId, Event: appendResp}
        actions = append(actions, resp)
    }
    return actions
}



type int64Slice []int64
func (array int64Slice) Len() int {
    return len(array)
}
func (array int64Slice) Less(i int, j int) bool {
    return array[i] < array[j]
}
func (array int64Slice) Swap(i int, j int) {
    tmp := array[i]
    array[j] = array[i]
    array[i] = tmp
}
/********************************************************************
 *                                                                  *
 *                    Append Request Response                       *
 *                                                                  *
 ********************************************************************/
func (state *StateMachine) appendRequestResponse(event AppendRequestRespEvent) (actions []interface{}) {

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

    // Check term
    if state.CurrentTerm < event.Term {
        // This server term is not so up-to-date, so update
        state.myState = FOLLOWER
        state.CurrentTerm = event.Term
        state.VotedFor = -1
        state_changed_flag = true

        // reset alarm
        alarm := AlarmAction{Time: state.ElectionTimeout + rand.Intn(state.ElectionTimeout)} // slightly greater time to receive heartbeat
        actions = append(actions, alarm)
        return actions
    }

    switch state.myState {
    case LEADER:
        if !event.Success {
            // there are holes in follower's log

            // Do not upgrade nextIndex if last log index is greater than nextIndex for that node
            // since, this might be delayed response
            if state.nextIndex[event.FromId] > event.LastLogIndex {
                state.nextIndex[event.FromId] = event.LastLogIndex + 1
            }

            // Now send next batch of logs from nextIndex onwards
            if state.nextIndex[event.FromId] > state.PersistentLog.GetLastIndex()+1 {
                state.log_error(3, "Next index of any node will never be grater than (last log index + 1) of the leader")
            } else if state.nextIndex[event.FromId] <= state.PersistentLog.GetLastIndex() {
                // Resend next batch of logs from the nextIndex to the end
                prevLog := state.GetLogAt(state.nextIndex[event.FromId] - 1)
                startIndex := state.nextIndex[event.FromId]
                logs := state.getLogsFrom(startIndex)   // copy server.log from startIndex to the end to "logs"
                event1 := AppendRequestEvent{
                    FromId:       state.server_id,
                    Term:         state.CurrentTerm,
                    PrevLogIndex: prevLog.Index,
                    PrevLogTerm:  prevLog.Term,
                    Entries:      *logs,
                    LeaderCommit: state.commitIndex}
                action := SendAction{ToId: event.FromId, Event: event1}
                actions = append(actions, action)
                return actions
            }
        } else if state.matchIndex[event.FromId] < event.LastLogIndex {
            state.matchIndex[event.FromId] = event.LastLogIndex
            state.nextIndex[event.FromId] = event.LastLogIndex + 1

            // lets sort
            sorted := int64Slice(append([]int64{}, state.matchIndex[1:]...))
            sort.Sort(sorted)               // sort in ascending order

            // If there exists an N such that N > commitIndex, a majority
            // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
            // set commitIndex = N
            //state.log_info(3, "Sorted match indices : %v", sorted)
            for i := state.numberOfNodes / 2; i >= 0; i-- {
                if sorted[i] > state.commitIndex && state.GetLogAt(sorted[i]).Term == state.CurrentTerm {
                    // Commit all not committed eligible entries
                    state.log_info(3, "Commiting from index %v to %v", state.commitIndex + 1, sorted[i])
                    for k := state.commitIndex + 1; k <= sorted[i]; k++ {
                        action := CommitAction{
                            Index   : k,
                            Data    : state.GetLogAt(k).Data,
                            Err     : nil}
                        actions = append(actions, action)
                    }

                    //server.commitIndex = sorted[i]
                    state.commitIndex = sorted[i]
                    break
                }
            }
        }

        // Don't send next batch of logs from nextIndex when reply is true,
        // delaying append request only on false reply


        // continue flow to next case for server.currentTerm > event.term
        fallthrough
    case CANDIDATE:
        fallthrough
    case FOLLOWER:
    }

    return actions
}




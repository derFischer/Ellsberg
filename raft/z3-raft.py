from z3 import *
import rich

total_servers = 3

Type, (follower, candidate, leader) = EnumSort("all_states", ["follower", "candidate", "leader"])
Server, (s1, s2, s3) = EnumSort("all_servers", ["s1", "s2", "s3"])
VotedServer, (none, v_s1, v_s2, v_s3) = EnumSort("all_voted", ["none", "s1", "s2", "s3"])

log_entry, mk_log_entry, (EntryTerm, EntryContent) = TupleSort('logentry', [IntSort(), IntSort()])

state, mk_states, (Term, State, Voted, Log, LogLength, VotesGranted_s1, VotesGranted_s2, VotesGranted_s3, CommitIndex, MatchIndex_s1, MatchIndex_s2, MatchIndex_s3) = TupleSort('states', [IntSort(), Type, VotedServer, ArraySort(IntSort(), log_entry), IntSort(), BoolSort(), BoolSort(), BoolSort(), IntSort(), IntSort(), IntSort(), IntSort()])

requestVoteReqMsg, mk_requestVoteReqMsg, (RequestVoteReqMsgTerm, RequestVoteReqMsgFrom, RequestVoteReqMsgLastLogTerm, RequestVoteReqMsgLastLogIndex) = TupleSort('requestVoteReqMsg', [IntSort(), Server, IntSort(), IntSort()])
requestVoteResponseMsg, mk_requestVoteResponseMsg, (RequestVoteResponseMsgTerm, RequestVoteResponseMsgFrom, RequestVoteResponseMsgGranted) = TupleSort('requestVoteResponseMsg', [IntSort(), Server, BoolSort()])
appendEntriesReqMsg, mk_appendEntriesReqMsg, (AppendEntriesReqMsgTerm, AppendEntriesReqMsgFrom, AppendEntriesReqMsgPrevLogIndex, AppendEntriesReqMsgPrevLogTerm, AppendEntriesReqMsgEntry, AppendEntriesReqMsgCommitIndex) \
    = TupleSort('appendEntriesReqMsg', [IntSort(), Server, IntSort(), IntSort(), log_entry, IntSort()])
appendEntriesResponseMsg, mk_appendEntriesResponseMsg, (AppendEntriesResponseMsgTerm, AppendEntriesResponseMsgFrom, AppendEntriesResponseMsgSuccess, AppendEntriesResponseMsgMatchIndex) = TupleSort('appendEntriesResponseMsg', [IntSort(), Server, BoolSort(), IntSort()])
# clientReqMsg, mk_clientReqMsg, (clientReqContent) = TupleSort('clientreq', [IntSort()])

TERM = 1
STATE = 2
VOTED = 3
LOG = 4
LOGLENGTH = 5
VOTESGRANTED_1 = 6
VOTESGRANTED_2 = 7
VOTESGRANTED_3 = 8
COMMITINDEX = 9
MATCHINDEX_1 = 10
MATCHINDEX_2 = 11
MATCHINDEX_3 = 12

def Dominate(s1, s2):
    return And(State(s1) == State(s2), Term(s1) == Term(s2), Voted(s1) == Voted(s2), Log(s1) == Log(s2), LogLength(s1) == LogLength(s2), CommitIndex(s1) >= CommitIndex(s2), MatchIndex_s1(s1) >= MatchIndex_s1(s2), MatchIndex_s2(s1) >= MatchIndex_s2(s2), MatchIndex_s3(s1) >= MatchIndex_s3(s2), If(State(s1)==candidate, And(VotesGranted_s1(s1) == VotesGranted_s1(s2), VotesGranted_s2(s1) == VotesGranted_s2(s2), VotesGranted_s3(s1) == VotesGranted_s3(s2)), And(If(VotesGranted_s1(s2), VotesGranted_s1(s1), True), If(VotesGranted_s2(s2), VotesGranted_s2(s1), True), If(VotesGranted_s3(s2), VotesGranted_s3(s1), True))))

# helper functions:
def getAllStates(current_states):
    current_term = Term(current_states)
    current_state = State(current_states)
    current_voted = Voted(current_states)
    current_log = Log(current_states)
    current_log_length = LogLength(current_states)
    current_votes_granted_1 = VotesGranted_s1(current_states)
    current_votes_granted_2 = VotesGranted_s2(current_states)
    current_votes_granted_3 = VotesGranted_s3(current_states)
    current_commit_index = CommitIndex(current_states)
    current_match_index_1 = MatchIndex_s1(current_states)
    current_match_index_2 = MatchIndex_s2(current_states)
    current_match_index_3 = MatchIndex_s3(current_states)
    return current_term, current_state, current_voted, current_log, current_log_length, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, current_commit_index, current_match_index_1, current_match_index_2, current_match_index_3

def updateOneState(current_states, state_no, new_state):
    current_term, current_state, current_voted, current_log, current_log_length, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, current_commit_index, current_match_index_1, current_match_index_2, current_match_index_3 = getAllStates(current_states)
    if state_no == TERM:
        return mk_states(new_state, current_state, current_voted, current_log, current_log_length, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, current_commit_index, current_match_index_1, current_match_index_2, current_match_index_3)
    if state_no == STATE:
        return mk_states(current_term, new_state, current_voted, current_log, current_log_length, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, current_commit_index, current_match_index_1, current_match_index_2, current_match_index_3)
    if state_no == VOTED:
        return mk_states(current_term, current_state, new_state, current_log, current_log_length, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, current_commit_index, current_match_index_1, current_match_index_2, current_match_index_3)
    if state_no == LOG:
        return mk_states(current_term, current_state, current_voted, new_state, current_log_length, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, current_commit_index, current_match_index_1, current_match_index_2, current_match_index_3)
    if state_no == LOGLENGTH:
        return mk_states(current_term, current_state, current_voted, current_log, new_state, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, current_commit_index, current_match_index_1, current_match_index_2, current_match_index_3)
    if state_no == VOTESGRANTED_1:
        return mk_states(current_term, current_state, current_voted, current_log, current_log_length, new_state, current_votes_granted_2, current_votes_granted_3, current_commit_index, current_match_index_1, current_match_index_2, current_match_index_3)
    if state_no == VOTESGRANTED_2:
        return mk_states(current_term, current_state, current_voted, current_log, current_log_length, current_votes_granted_1, new_state, current_votes_granted_3, current_commit_index, current_match_index_1, current_match_index_2, current_match_index_3)
    if state_no == VOTESGRANTED_3:
        return mk_states(current_term, current_state, current_voted, current_log, current_log_length, current_votes_granted_1, current_votes_granted_2, new_state, current_commit_index, current_match_index_1, current_match_index_2, current_match_index_3)
    if state_no == COMMITINDEX:
        return mk_states(current_term, current_state, current_voted, current_log, current_log_length, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, new_state, current_match_index_1, current_match_index_2, current_match_index_3)
    if state_no == MATCHINDEX_1:
        return mk_states(current_term, current_state, current_voted, current_log, current_log_length, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, current_commit_index, new_state, current_match_index_2, current_match_index_3)
    if state_no == MATCHINDEX_2:
        return mk_states(current_term, current_state, current_voted, current_log, current_log_length, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, current_commit_index, current_match_index_1, new_state, current_match_index_3)
    if state_no == MATCHINDEX_3:
        return mk_states(current_term, current_state, current_voted, current_log, current_log_length, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, current_commit_index, current_match_index_1, current_match_index_2, new_state)

def updateStates(current_states, state_nos, new_states):
    for i, val in enumerate(state_nos):
        current_states = updateOneState(current_states, val, new_states[i])
    return current_states

def votedUpdate(current_states, serverID):
    return z3.If(serverID == s1, updateStates(current_states, [VOTED], [v_s1]), \
                    z3.If(serverID == s2, updateStates(current_states, [VOTED], [v_s2]), \
                        updateStates(current_states, [VOTED], [v_s3])))

def matchIndexUpdate(current_states, serverID, matchIndex):
    current_term, current_state, current_voted, current_log, current_log_length, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, current_commit_index, current_match_index_1, current_match_index_2, current_match_index_3 = getAllStates(current_states)

    current_index = z3.If(serverID == s1, current_match_index_1, \
                            z3.If(serverID == s2, current_match_index_2, \
                                current_match_index_3))
    return z3.If(current_index < matchIndex, z3.If(serverID == s1, updateStates(current_states, [MATCHINDEX_1], [matchIndex]), \
                            z3.If(serverID == s2, updateStates(current_states, [MATCHINDEX_2], [matchIndex]), \
                                updateStates(current_states, [MATCHINDEX_3], [matchIndex]))), current_states)

def grantVotes(current_states, serverID):
    return z3.If(serverID == s1, updateStates(current_states, [VOTESGRANTED_1], [True]), \
                    z3.If(serverID == s2, updateStates(current_states, [VOTESGRANTED_2], [True]), \
                        updateStates(current_states, [VOTESGRANTED_3], [True])))

def logUpdate(current_states, prevIndex, entry):
    current_term, current_state, current_voted, current_log, current_log_length, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, current_commit_index, current_match_index_1, current_match_index_2, current_match_index_3 = getAllStates(current_states)
    new_log = Store(current_log, prevIndex + 1, entry)
    return z3.If(current_log_length == prevIndex, updateStates(current_states, [LOG, LOGLENGTH], [new_log, current_log_length + 1]), updateStates(current_states, [LOG], [new_log]))


# msg handlers
# nil event:
def timeout(current_states, msg):
    current_term, current_state, current_voted, current_log, current_log_length, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, current_commit_index, current_match_index_1, current_match_index_2, current_match_index_3 = getAllStates(current_states)
    return z3.If(current_state == leader, current_states, mk_states(current_term + 1, candidate, none, current_log, current_log_length, False, False, False, current_commit_index, current_match_index_1, current_match_index_2, current_match_index_3))

def becomeLeader(current_states):
    current_term, current_state, current_voted, current_log, current_log_length, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, current_commit_index, current_match_index_1, current_match_index_2, current_match_index_3 = getAllStates(current_states)
    return mk_states(current_term, leader, current_voted, current_log, current_log_length, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, current_commit_index, 0, 0, 0)

def checkLeader(current_states, msg):
    current_term, current_state, current_voted, current_log, current_log_length, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, current_commit_index, current_match_index_1, current_match_index_2, current_match_index_3 = getAllStates(current_states)
    return z3.If(current_state != candidate, current_states, z3.If(Or(And(current_votes_granted_1 == True, current_votes_granted_2 == True), Or(And(current_votes_granted_1 == True, current_votes_granted_3 == True), And(current_votes_granted_2 == True, current_votes_granted_3 == True))), becomeLeader(current_states), current_states))


# messages:
def updateTerm(current_states, msg_term):
    term = Term(current_states)
    return z3.If(term < msg_term, updateStates(current_states, [TERM, STATE, VOTED], [msg_term, follower, none]), current_states)

def stepDownToFollower(current_states, msg_term):
    # a candidate that receives an AppendEntries request of its term returns to follower
    return z3.If(And(Term(current_states) == msg_term, State(current_states) == candidate), updateStates(current_states, [STATE], [follower]), current_states)

def requestVoteReqHandler(current_states, msg):
    message_term = RequestVoteReqMsgTerm(msg)
    message_from = RequestVoteReqMsgFrom(msg)
    message_last_log_term = RequestVoteReqMsgLastLogTerm(msg)
    message_last_log_index = RequestVoteReqMsgLastLogIndex(msg)

    current_states = updateTerm(current_states, message_term)
    current_term, current_state, current_voted, current_log, current_log_length, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, current_commit_index, current_match_index_1, current_match_index_2, current_match_index_3 = getAllStates(current_states)

    logOK = Or(message_last_log_term > EntryTerm(current_log[current_log_length]), \
                And(message_last_log_term == EntryTerm(current_log[current_log_length]), \
                        message_last_log_index >= current_log_length))
    grant = And(message_term == current_term, \
                And(logOK, \
                    Or(current_voted == none, 
                        Or(And(message_from == s1, current_voted == v_s1), 
                            Or(And(message_from == s2, current_voted == v_s2), 
                                And(message_from == s3, current_voted == v_s3))))))

    return z3.If(grant, votedUpdate(current_states, message_from), current_states)

def requestVoteResponseHandler(current_states, msg):
    message_term = RequestVoteResponseMsgTerm(msg)
    message_from = RequestVoteResponseMsgFrom(msg)
    message_grant = RequestVoteResponseMsgGranted(msg)

    current_states = updateTerm(current_states, message_term)
    current_term, current_state, current_voted, current_log, current_log_length, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, current_commit_index, current_match_index_1, current_match_index_2, current_match_index_3 = getAllStates(current_states)

    return z3.If(message_term != current_term, current_states, \
                z3.If(message_grant, grantVotes(current_states, message_from), current_states))

def logOK(current_states, msg):
    message_term = AppendEntriesReqMsgTerm(msg)
    message_from = AppendEntriesReqMsgFrom(msg)
    message_prevLogIndex = AppendEntriesReqMsgPrevLogIndex(msg)
    message_prevLogTerm = AppendEntriesReqMsgPrevLogTerm(msg)
    message_entry = AppendEntriesReqMsgEntry(msg)
    message_commitIndex = AppendEntriesReqMsgCommitIndex(msg)

    current_term, current_state, current_voted, current_log, current_log_length, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, current_commit_index, current_match_index_1, current_match_index_2, current_match_index_3 = getAllStates(current_states)

    return Or(message_prevLogIndex == 0, And(message_prevLogIndex > 0, And(message_prevLogIndex <= current_log_length, message_prevLogTerm == EntryTerm(current_log[message_prevLogIndex]))))

def appendEntryReqHandler(current_states, msg):
    message_term = AppendEntriesReqMsgTerm(msg)
    message_from = AppendEntriesReqMsgFrom(msg)
    message_prevLogIndex = AppendEntriesReqMsgPrevLogIndex(msg)
    message_prevLogTerm = AppendEntriesReqMsgPrevLogTerm(msg)
    message_entry = AppendEntriesReqMsgEntry(msg)
    message_commitIndex = AppendEntriesReqMsgCommitIndex(msg)

    current_states = updateTerm(current_states, message_term)
    current_states = stepDownToFollower(current_states, message_term)
    current_term, current_state, current_voted, current_log, current_log_length, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, current_commit_index, current_match_index_1, current_match_index_2, current_match_index_3 = getAllStates(current_states)

    LogOK = logOK(current_states, msg)
    
    return z3.If(message_term != current_term, current_states, \
                 z3.If(current_state != follower, current_states, \
                    z3.If(LogOK != True, current_states, \
                        z3.If(current_commit_index > message_commitIndex, logUpdate(current_states, message_prevLogIndex, message_entry), \
                            updateStates(logUpdate(current_states, message_prevLogIndex, message_entry), [COMMITINDEX], [message_commitIndex])))))

def updateCommitIndex(current_states):
    return z3.If(And(State(current_states) == leader, majorityIndex(current_states) >= CommitIndex(current_states)), updateStates(current_states, [COMMITINDEX], [majorityIndex(current_states)]), current_states)

def majorityIndex(current_states):
    s1_majority = z3.If(Or(And(MatchIndex_s1(current_states) >= MatchIndex_s2(current_states), MatchIndex_s1(current_states) <= MatchIndex_s3(current_states)), And(MatchIndex_s1(current_states) <= MatchIndex_s2(current_states), MatchIndex_s1(current_states) >= MatchIndex_s3(current_states))), True, False)

    s2_majority = z3.If(Or(And(MatchIndex_s2(current_states) >= MatchIndex_s1(current_states), MatchIndex_s2(current_states) <= MatchIndex_s3(current_states)), And(MatchIndex_s2(current_states) <= MatchIndex_s1(current_states), MatchIndex_s2(current_states) >= MatchIndex_s3(current_states))), True, False)

    return z3.If(s1_majority, MatchIndex_s1(current_states), \
                    z3.If(s2_majority, MatchIndex_s2(current_states), MatchIndex_s3(current_states)))

def appendEntryResponseHandler(current_states, msg):
    message_term = AppendEntriesResponseMsgTerm(msg)
    message_from = AppendEntriesResponseMsgFrom(msg)
    message_success = AppendEntriesResponseMsgSuccess(msg)
    message_matchIndex = AppendEntriesResponseMsgMatchIndex(msg)

    current_states = updateTerm(current_states, message_term)
    current_term, current_state, current_voted, current_log, current_log_length, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, current_commit_index, current_match_index_1, current_match_index_2, current_match_index_3 = getAllStates(current_states)
    return z3.If(message_term != current_term, current_states, \
                 z3.If(current_state != leader, current_states, \
                    z3.If(message_success == False, current_states, updateCommitIndex(matchIndexUpdate(current_states, message_from, message_matchIndex)))))

def clientReqHandler(current_states, msg):
    current_term, current_state, current_voted, current_log, current_log_length, current_votes_granted_1, current_votes_granted_2, current_votes_granted_3, current_commit_index, current_match_index_1, current_match_index_2, current_match_index_3 = getAllStates(current_states)
    return z3.If(current_state != leader, current_states, logUpdate(current_states, current_log_length, mk_log_entry(current_term, msg)))




# proof
states = mk_states(Int('term'), Const('state', Type), Const('voted', VotedServer), Array('log', IntSort(), log_entry), Int('loglength'), Bool('voted_s1'), Bool('voted_s2'), Bool('voted_s3'), Int('commitindex'), Int('matchIndex_s1'), Int('matchIndex_s2'), Int('matchIndex_s3'))

states_1 = mk_states(Int('term_1'), Const('state_1', Type), Const('voted_1', VotedServer), Array('log_1', IntSort(), log_entry), Int('loglength_1'), Bool('voted_s1_1'), Bool('voted_s2_1'), Bool('voted_s3_1'), Int('commitindex_1'), Int('matchIndex_s1_1'), Int('matchIndex_s2_1'), Int('matchIndex_s3_1'))

handlers = [requestVoteReqHandler, requestVoteResponseHandler, appendEntryReqHandler, appendEntryResponseHandler, clientReqHandler, checkLeader, timeout]
m1 = [mk_requestVoteReqMsg(Int('m1_term'), Const('m1_from', Server), Int('m1_last_log_term'), Int('m1_last_log_index')), \
      mk_requestVoteResponseMsg(Int('m1_term'), Const('m1_from', Server), Bool('m1_granted')), \
      mk_appendEntriesReqMsg(Int('m1_term'), Const('m1_from', Server), Int('m1_prelogindex'), Int('m1_prelogterm'), Const('entry1', log_entry), Int('m1_commitindex')), \
      mk_appendEntriesResponseMsg(Int('m1_term'), Const('m1_from', Server), Bool('m1_success'), Int('m1_matchindex')), \
      Int('m1_clientreq'), None, None]

TOTAL_MESSAGE_TYPE = 7

# we first need to filter out those states that are not valid in the implementation
def states_constraints(states):
    return []

# the states constraints should always be true in the implementation, but we still prove it here
def check_states_constraints(states_constraints):
    for i in range(TOTAL_MESSAGE_TYPE):
        # print("message type is: " + str(i))
        message = m1[i]
        handler = handlers[i]
        s = Solver()
        s.add(And(*(states_constraints(states))))
        s.add(Not(And(*(states_constraints(handler(states, message))))))
        if s.check() == sat:
            print("states constraints are violated")
            print(s.model())
            return
    print("states constraints are satisfied")

# check statement 1
def check_statement_1(state_constraints):
    for i in range(TOTAL_MESSAGE_TYPE):
        # print("message type is: " + str(i))
        message = m1[i]
        handler = handlers[i]
        s = Solver()
        s.add(And(*(states_constraints(states))))
        s.add(And(*(states_constraints(states_1))))
        s.add(Dominate(states, states_1))
        handler(states, message)
        s.add(Dominate(handler(states, message), handler(states_1, message))==False)
        if s.check() == sat:
            print("statement 1 is violated")
            rich.print(s.model().eval(states))
            rich.print(s.model().eval(states_1))
            print("after apply 1:")
            rich.print(s.model().eval(handler(states, message)))
            print("after apply 2:")
            rich.print(s.model().eval(handler(states_1, message)))
            rich.print(s.model().eval(message))
            return
    print("statement 1 is satisfied")
        

# The apply_asap? rules, one per message type: (name, message, handler, rule, message constraints).
# Each rule is checked with statements 2-4; message_constraints restricts the other message of
# statements 2 and 4.

def no_message_constraints(m1, m2):
    return True

# AppendEntriesResponse, the rule of the paper: a response from an earlier term, or from the current
# term (in any role). The second condition below is not part of the rule: it holds in every reachable
# state, because a server only receives responses to requests it sent as leader of that term, so a
# response's term is at most the server's term, and lower if the server is a candidate (becoming a
# candidate raises the term). Statement 4 checks that both conditions are preserved by every event.
m_append_entries_response = mk_appendEntriesResponseMsg(Int('m_term'), Const('m_from', Server), Bool('m_success'), Int('m_matchindex'))

def append_entries_response_constraints(m, states):
    return [z3.If(AppendEntriesResponseMsgTerm(m) <= Term(states), True, False), \
               z3.If(Or(State(states) == leader, Or(State(states) == follower, \
                                                    And(State(states) == candidate, Term(states) > AppendEntriesResponseMsgTerm(m)))), True, False)]

# RequestVoteResponse: a response from an earlier term, or from the current term to a server that is
# not a candidate.
m_request_vote_response = mk_requestVoteResponseMsg(Int('m_term'), Const('m_from', Server), Bool('m_granted'))

def request_vote_response_constraints(m, states):
    return [z3.If(Or(RequestVoteResponseMsgTerm(m) < Term(states), And(RequestVoteResponseMsgTerm(m) == Term(states), State(states) != candidate)), True, False)]

# AppendEntries request: a request from an earlier term, or from the current term to a follower whose
# log already has the request's entry.
m_append_entries = mk_appendEntriesReqMsg(Int('m_term'), Const('m_from', Server), Int('m_prelogindex'), Int('m_prelogterm'), mk_log_entry(Int('m_entry_term'), Int('m_entry_content')), Int('m_commitindex'))

# Two AppendEntries requests of the same term agree on the entries they both have.
def append_entries_message_constraints(m1, m2):
    if m2 == None:
        return True
    if m1.sort() != m2.sort():
        return True
    if m1.sort() != m_append_entries.sort():
        return True
    m3 = m1
    m1 = z3.If(AppendEntriesReqMsgPrevLogIndex(m1) > AppendEntriesReqMsgPrevLogIndex(m2), m1, m2)
    m2 = z3.If(AppendEntriesReqMsgPrevLogIndex(m3) > AppendEntriesReqMsgPrevLogIndex(m2), m2, m3)
    return z3.If(AppendEntriesReqMsgTerm(m1) != AppendEntriesReqMsgTerm(m2), True, \
                    z3.If(AppendEntriesReqMsgPrevLogIndex(m1) > AppendEntriesReqMsgPrevLogIndex(m2) + 1, True, \
                         z3.If(AppendEntriesReqMsgPrevLogIndex(m1) == AppendEntriesReqMsgPrevLogIndex(m2) + 1, \
                            AppendEntriesReqMsgPrevLogTerm(m1) == EntryTerm(AppendEntriesReqMsgEntry(m2)), \
                                And(EntryTerm(AppendEntriesReqMsgEntry(m1)) == EntryTerm(AppendEntriesReqMsgEntry(m2)), EntryContent(AppendEntriesReqMsgEntry(m1)) == EntryContent(AppendEntriesReqMsgEntry(m2))))))

def append_entries_constraints(m, states):
    return [z3.If(AppendEntriesReqMsgTerm(m) < Term(states), True, \
                  z3.If(AppendEntriesReqMsgTerm(m) > Term(states), False, \
                            z3.If(State(states) != follower, False, \
                            z3.If(AppendEntriesReqMsgPrevLogIndex(m) > LogLength(states) - 1, False, \
                                z3.If(logOK(states, m) != True, False, \
                                    z3.If(EntryTerm(Log(states)[AppendEntriesReqMsgPrevLogIndex(m) +  1]) != EntryTerm(AppendEntriesReqMsgEntry(m)), False, \
                                          z3.If(EntryContent(Log(states)[AppendEntriesReqMsgPrevLogIndex(m) + 1]) != EntryContent(AppendEntriesReqMsgEntry(m)), False, True)))))))]

# RequestVote request: a request from an earlier term, or from the current term to a server that has
# already voted.
m_request_vote = mk_requestVoteReqMsg(Int('m_term'), Const('m_from', Server), Int('m_last_log_term'), Int('m_last_log_index'))

def request_vote_constraints(m, states):
    return [z3.If(RequestVoteReqMsgTerm(m) < Term(states), True, \
                  z3.If(RequestVoteReqMsgTerm(m) > Term(states), False, \
                        Voted(states) != none))]

rules = [
    ("AppendEntriesResponse", m_append_entries_response, appendEntryResponseHandler, append_entries_response_constraints, no_message_constraints),
    ("RequestVoteResponse", m_request_vote_response, requestVoteResponseHandler, request_vote_response_constraints, no_message_constraints),
    ("AppendEntries", m_append_entries, appendEntryReqHandler, append_entries_constraints, append_entries_message_constraints),
    ("RequestVote", m_request_vote, requestVoteReqHandler, request_vote_constraints, no_message_constraints),
]

def check_statement_2(m, handler, state_constraints, constraints, message_constraints):
    for i in range(TOTAL_MESSAGE_TYPE):
        message_1 = m1[i]
        handler_1 = handlers[i]
        s = Solver()
        s.add(And(*(state_constraints(states))))
        s.add(*constraints)
        s.add(message_constraints(m, message_1))
        s.add(And(Not(Dominate(handler_1(handler(states, m), message_1), handler(handler_1(states, message_1), m))), 
                  Not(handler_1(states, message_1) == states)))
        if s.check() == sat:
            print("statement 2 is violated")
            print(s.model())
            return
    print("statement 2 is satisfied")
        
def check_statement_3(m, handler, state_constraints, constraints):
    s = Solver()
    s.add(And(*(state_constraints(states))))
    s.add(*constraints)
    s.add(Dominate(handler(states, m), states)==False)
    if s.check() == sat:
        print("statement 3 is violated")
        print(s.model())
        return
    print("statement 3 is satisfied")
    
def check_statement_4(m, state_constraints, constraints, message_constraints):
    cons = constraints(m, states)
    if len(cons) == 0:
        return
    for i in range(TOTAL_MESSAGE_TYPE):
        # print("message type is: " + str(i))
        message = m1[i]
        handler = handlers[i]
        s = Solver()
        s.add(And(*(state_constraints(states))))
        s.add(message_constraints(m, message))
        # initial constraints
        cons = constraints(m, states)
        s.add(*cons)
        cons_after = constraints(m, handler(states, message))
        s.add(Not(And(*cons_after)))
        if s.check() == sat:
            print("statement 4 is violated")
            print(s.model())
            return
    print("statement 4 is satisfied")

check_states_constraints(states_constraints)
check_statement_1(states_constraints)
for name, m, handler, constraints, message_constraints in rules:
    print(name + ":")
    check_statement_2(m, handler, states_constraints, constraints(m, states), message_constraints)
    check_statement_3(m, handler, states_constraints, constraints(m, states))
    check_statement_4(m, states_constraints, constraints, message_constraints)

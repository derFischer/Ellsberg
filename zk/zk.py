"""
Naming:
1. Sorts:
    - All sorts(including `EnumSort`s) are named in CamelCase. (`DiscoverVars`, `EntryType`)
    - All sort generating functions are named in CamelCase with `Sort` suffix. (`IntArraySort`, `TupleSort`)
2. Enum:
    - Each enum entry is named with capital letters and underscore. (`ENTRY_KV_PUT`, `ENTRY_KV_RO`)
3. Tuple:
    - Tuple making functions are named with underscore. (`mk_discover_vars`, `Command.mk`)
    - Tuple fields' accessing functions are named with underscore. (`command_index`, `command_op`)
"""

import rich
from z3 import *
from tuple_declarer import *
from utils import *

NUM_SERVERS = 3
SERVER_IDS = list(range(1, NUM_SERVERS + 1))
QUORUM_MIN = (len(SERVER_IDS) + 1) // 2

NUM_ENTRIES = 3
M_ACK = 1
M_COMMIT = 2
TEST = M_ACK

############################ Enums and Tuples
# Enums
StateType, (LEADING, FOLLOWING, LOOKING) = EnumSort('StateType', ('LEADING', 'FOLLOWING', 'LOOKING'))
ZabStateType, (DISCOVERY, SYNC, BROADCAST, ELECTION) = EnumSort('ZabStateType',
                                                                ('DISCOVERY', 'SYNC', 'BROADCAST', 'ELECTION'))
EntryType, (ENTRY_NORMAL, ENTRY_KV_PUT, ENTRY_KV_RO, ENTRY_CONF_CHANGE, ENTRY_EMPTY) = EnumSort('EntryType', (
'ENTRY_NORMAL', 'ENTRY_KV_PUT', 'ENTRY_KV_RO', 'ENTRY_CONF_CHANGE', 'ENTRY_EMPTY'))

TupleDeclarer.declared_enums.update([StateType, ZabStateType, EntryType])

# Zxid
Zxid = TupleDeclarer('Zxid', {'epoch': IntSort(), 'id': IntSort()})

# DiscoverVars
# NOTE: `elections` will have all the servers as key, but `value==-1` means no existing.
CI_NOONE = -1
CI = TupleDeclarer('ConnectInfo', {'sid': IntSort(), 'nl_rcv': BoolSort()}, short_name='CI')
DV = TupleDeclarer('DiscoverVars', 
                   {'state': StateType, 'temp_max_epoch': IntSort(), 'connect_info': CI.tuple},
                   arr_fields={'connecting': (BoolSort(), NUM_SERVERS), 'election': (Zxid.tuple, NUM_SERVERS), 'learners': (BoolSort(), NUM_SERVERS), 'ack_ld_recv': (BoolSort(), NUM_SERVERS)},
                   short_name='DV')


# Command
# NOTE: I add `log_length`, `last_proccessed` for convinience, remember to update.
Op = TupleDeclarer('Op', {'ent_type': EntryType, 'req_id': IntSort()})
Command = TupleDeclarer('Command', {'index': Zxid.tuple, 'op': Op.tuple}, arr_fields={'ackid': (BoolSort(), NUM_SERVERS)})
NoneCommand = Const("NoneCommand", Command.tuple)
FalseCommand = Command.mk(Zxid.mk(-1, -1), Op.mk(ENTRY_NORMAL, -1), *[False for _ in range(NUM_SERVERS)], NUM_SERVERS)
TrueCommand = Command.mk(Zxid.mk(-1, -1), Op.mk(ENTRY_NORMAL, -1), *[True for _ in range(NUM_SERVERS)], NUM_SERVERS)

# State
# NOTE: I removed `peers`
State = TupleDeclarer('State', 
                      {'me': IntSort(), 'election_state': DV.tuple, 'zab_state': ZabStateType, 'epoch': IntSort(), 'accept_epoch': IntSort(), 'last_committed': Zxid.tuple, 'last_processed': Zxid.tuple}, 
                      vec_fields={'log': (Command.tuple, NUM_ENTRIES)})


# Messages
MessageType, (
NOT, TCPCONNECT, FOLLOWERINFO, LEADERINFO, ACKEPOCH, DIFF, TRUNC, SNAP, NEWLEADER, UPTODATE, CLIENT, CLIENTRO, PROPOSAL, ACK,
COMMIT, CLIENTACK, CLIENTRORESP, ACKLD, TURNTOLEADER, TURNTOFOLLOWER, TIMEOUT) = EnumSort(
    "MessageType", (
    "NOT", "TCPCONNECT", "FOLLOWERINFO", "LEADERINFO", "ACKEPOCH", "DIFF", "TRUNC", "SNAP", "NEWLEADER", "UPTODATE", "CLIENT",
    "CLIENTRO", "PROPOSAL", "ACK", "COMMIT", "CLIENTACK", "CLIENTRORESP", "ACKLD", "TURNTOLEADER", "TURNTOFOLLOWER", "TIMEOUT")
)
Vote = TupleDeclarer('Vote', {'leader': IntSort(), 'zxid': Zxid.tuple, 'epoch': IntSort()})
## NOTE: I made req_id here as an Int.
# Message, mk_message, (m_type, m_from, m_vote, m_round, m_state, m_epoch, m_zxid, m_req_id,) = TupleSort(
    # 'Message', [MessageType, IntSort(), Vote.tuple, IntSort(), StateType, IntSort(), Zxid.tuple, IntSort(),]
# )
Message, mk_message, (m_type, m_from, m_vote, m_round, m_state, m_epoch, m_zxid, m_req_id, m_log__0, m_log__1, m_log__2, m_log__length, m_commit__index, m_commit__epoch) = TupleSort(
    'Message', [MessageType, IntSort(), Vote.tuple, IntSort(), StateType, IntSort(), Zxid.tuple, IntSort(), Command.tuple, Command.tuple, Command.tuple, IntSort(), IntSort(), IntSort()]
)

# Message, mk_message, (m_type, m_from, m_vote, m_round, m_state, m_epoch, m_zxid, m_req_id, m_log__0, m_log__1, m_log__2, m_log__length, m_commit__index) = TupleSort(
    # 'Message', [MessageType, IntSort(), Vote.tuple, IntSort(), StateType, IntSort(), Zxid.tuple, IntSort(), Command.tuple, Command.tuple, Command.tuple, IntSort(), IntSort()]
# )
# Message = TupleDeclarer('Message', {'mtype': MessageType, 'mfrom': IntSort(), 'vote': Vote.tuple, 'mround': IntSort(), 'state': StateType, 'epoch': IntSort(), 'zxid': Zxid.tuple, 'req_id': IntSort()})

# StateSummary
SS = TupleDeclarer('StateSummary', {'epoch': IntSort(), 'zxid': Zxid.tuple})


############################ Helper
# Helper Universal
def server_vec_values_generator(servers: list[int], init=None):
    if init is None:
        init = 0
    while True:
        yield [init for _ in servers] + [len(servers)]


def prefix_generator():
    i = 0
    while True:
        yield f'prefix_{i}'


prefix_gen = prefix_generator()

# Helper Zxid
def Follower_ACK_Entry(s, From, index):
    return Command.ackid__select((State.log__select(s, index, default_value=FalseCommand)), From-1)

def Previous_ACK(s, From, zxid):
    index = zxid_to_index(s, zxid)
    return Command.ackid__select((State.log__select(s, index-1, default_value=TrueCommand)), From-1)

def ACK_bound(epoch, last_committed):
    return If(epoch == Zxid.epoch(last_committed), Zxid.mk(epoch, Zxid.id(last_committed) + 1), Zxid.mk(epoch, 1))

def inc_zxid(epoch, zxid):
    return If(epoch == Zxid.epoch(zxid), Zxid.mk(epoch, Zxid.id(zxid) + 1), Zxid.mk(epoch, 1))


def zxid_l(zxid1, zxid2):
    return Or(Zxid.epoch(zxid1) < Zxid.epoch(zxid2),
              And(Zxid.epoch(zxid1) == Zxid.epoch(zxid2), Zxid.id(zxid1) < Zxid.id(zxid2)))


def zxid_g(zxid1, zxid2):
    return simplify(Not(zxid_le(zxid1, zxid2)))


def zxid_le(zxid1, zxid2):
    return Or(Zxid.epoch(zxid1) < Zxid.epoch(zxid2),
              And(Zxid.epoch(zxid1) == Zxid.epoch(zxid2), Zxid.id(zxid1) <= Zxid.id(zxid2)))


def zxid_ge(zxid1, zxid2):
    return simplify(Not(zxid_l(zxid1, zxid2)))


def zxid_eq(zxid1, zxid2):
    return And(Zxid.epoch(zxid1) == Zxid.epoch(zxid2), Zxid.id(zxid1) == Zxid.id(zxid2))


# Helper ack_id
ack_id_gen = server_vec_values_generator(SERVER_IDS, init=False)


def add_ack_id(entry, sid):
    return Command.ackid__store(entry, sid, True)


def has_quorum(obj, accessors):
    sum = 0
    for accessor in accessors:
        sum += If(accessor(obj) == True, 1, 0)
    return sum >= QUORUM_MIN

def ackid_subset(cmd1, cmd2):
    return And(*[If(Command.ackid[i](cmd1), If(Command.ackid[i](cmd2), True, False), True) for i in range(NUM_SERVERS)])


# Helper ElectionState
connecting_gen = server_vec_values_generator(SERVER_IDS, init=False)
election_gen = server_vec_values_generator(SERVER_IDS, init=-1)
learners_gen = server_vec_values_generator(SERVER_IDS, init=Zxid.mk(-1, -1))
ack_ld_recv_gen = server_vec_values_generator(SERVER_IDS, init=False)

def add_ack_ld(es, sid):
    return DV.ack_ld_recv__store(es, sid-1, True)

def add_learner(es, sid):
    return DV.learners__store(es, sid-1, True)


def add_connecting(es, sid):
    return DV.connecting__store(es, sid-1, True)

def waiting_for_new_epoch(es):
    return Not(has_quorum(es, DV.connecting))


def election_finished(es):
    sum = 0
    for i in range(NUM_SERVERS):
        zxid = DV.election[i](es)
        sum += If(And(Zxid.epoch(zxid) == -1, Zxid.id(zxid) == -1), 0, 1)
    return sum >= QUORUM_MIN


# Helper State
def add_log_entry(s, new_txn):
    log_length = State.log__length(s)
    new_state = State.update(s, {**{f'log__{i}': If(log_length == i, new_txn, State.log[i](s)) for i in range(NUM_ENTRIES)}, 
                                 'log__length': If(log_length == NUM_ENTRIES, NUM_ENTRIES, log_length + 1)})
    return new_state

def is_next_zxid(cur, next):
    return Or(And(Zxid.id(next) == 1, Zxid.epoch(cur) < Zxid.epoch(next)),
              And(Zxid.id(next) > 1, Zxid.epoch(cur) == Zxid.epoch(next), Zxid.id(cur) + 1 == Zxid.id(next)))


def is_state(s, target_election_state, target_zab_state):
    election_state = State.election_state(s)
    election_state_state = DV.state(election_state)
    zab_state = State.zab_state(s)
    if target_election_state is None and target_zab_state is None:
        return True
    elif target_election_state is None:
        return zab_state == target_zab_state
    elif target_zab_state is None:
        return election_state_state == target_election_state
    else:
        return And(election_state_state == target_election_state, zab_state == target_zab_state)


def get_last_zxid(s):
    index = State.log__length(s) - 1
    statement = None
    for i in range(NUM_ENTRIES):
        if statement is None:
            statement = If(index == i, Command.index(State.log[i](s)), Zxid.mk(0, 0)) # NOTE: by default, return <0,0>
        else:
            statement = If(index == i, Command.index(State.log[i](s)), statement)
    return statement


def is_my_leader(s, sid):
    return sid == CI.sid(DV.connect_info(State.election_state(s)))

def is_nl_received(s):
    return CI.nl_rcv(DV.connect_info(State.election_state(s)))

def is_my_learner(s, sid):
    return DV.learners__select(State.election_state(s), sid-1) == True


def zxid_to_index(s, zxid):
    statement = None
    for i in range(NUM_ENTRIES):
        if statement is None:
            statement = If(And(i < State.log__length(s), zxid_eq(Command.index(State.log[i](s)), zxid)), i, -1)
        else:
            statement = If(And(i < State.log__length(s), zxid_eq(Command.index(State.log[i](s)), zxid)), i, statement)
    return statement


# Helper StateSummary
def is_more_recent_than(ss1, ss2):
    return Or(And(SS.epoch(ss1) > SS.epoch(ss2)),
              And(SS.epoch(ss1) == SS.epoch(ss2), zxid_g(SS.zxid(ss1), SS.zxid(ss2))))


############################ Handler

def timeout(s, m):
    election_state = State.election_state(s)
    election_state = DV.update(election_state, {'state': LOOKING, 'connect_info': CI.mk(CI_NOONE, False)})
    election_state = DV.update(election_state, {f'connecting__{i}': False for i in range(NUM_SERVERS)})
    election_state = DV.update(election_state, {f'election__{i}': Zxid.mk(-1, -1) for i in range(NUM_SERVERS)})
    election_state = DV.update(election_state, {f'learners__{i}': False for i in range(NUM_SERVERS)})
    election_state = DV.update(election_state, {f'ack_ld_recv__{i}': False for i in range(NUM_SERVERS)})
    return State.update(s, {"zab_state": ELECTION, "election_state": election_state, "last_processed": get_last_zxid(s)})


def turn_to_leading(s, m):
    election_state = State.election_state(s)
    election_state = DV.update(election_state, {f'connecting__{i}': If(State.me(s) == i, True, False) for i in range(NUM_SERVERS)})
    election_state = DV.update(election_state, {f'election__{i}': If(State.me(s) == i, get_last_zxid(s), Zxid.mk(-1, -1)) for i in range(NUM_SERVERS)})
    election_state = DV.update(election_state, {f'learners__{i}': If(State.me(s) == i, True, False) for i in range(NUM_SERVERS)})
    election_state = DV.update(election_state, {f'ack_ld_recv__{i}': If(State.me(s) == i, True, False) for i in range(NUM_SERVERS)})
    election_state = DV.update(election_state, {'state': LEADING, 'temp_max_epoch': State.accept_epoch(s)+1})
    election_state = simplify(election_state)
    return If(is_state(s, LOOKING, None), State.update(s, {"zab_state": DISCOVERY, "election_state": election_state}), s)


def turn_to_following(s, m):
    election_state = State.election_state(s)
    election_state = DV.update(election_state, {"state": FOLLOWING})
    return State.update(s, {"zab_state": DISCOVERY, "election_state": election_state})

# in zookeeper, turn_to_leading and turn_to_following are always following TIMEOUT, to add this constraint when z3 explores the space, we wrap these two into one function
def turn_to_leader_after_timeout(s, m):
    state = timeout(s, m)
    return turn_to_leading(state, m)

def turn_to_follower_after_timeout(s, m):
    state = timeout(s, m)
    return turn_to_following(state, m)

def TCP_connect_handler(s, m):
    es1 = State.update(s, {'election_state': add_learner(State.election_state(s), m_from(m))})
    es2 = State.update(s, {
        "election_state": DV.update(State.election_state(s), {"connect_info": CI.mk(m_from(m), False)}),
    })
    return If(And(is_state(s, LEADING, None), Not(is_my_learner(s, m_from(m)))), es1, If(And(is_state(s, FOLLOWING, None), is_my_leader(s, CI_NOONE)), es2, s))

def follower_info_handler(s, m):
    msg_epoch = Zxid.epoch(m_zxid(m))
    es = State.election_state(s)
    es2 = DV.update(es, {
        'temp_max_epoch': If(msg_epoch >= DV.temp_max_epoch(es), msg_epoch + 1, DV.temp_max_epoch(es))
    })
    es2 = add_connecting(es2, m_from(m))
    return If(And(is_state(s, LEADING, None), is_my_learner(s, m_from(m))),
              If(waiting_for_new_epoch(es),
                 If(Not(waiting_for_new_epoch(es2)),
                    State.update(s, {'election_state': es2, 'accept_epoch': DV.temp_max_epoch(es2)}),
                    State.update(s, {'election_state': es2})),
                 s),
              s)

def leader_info_handler(s, m):
    new_epoch = Zxid.epoch(m_zxid(m))
    updated_state = State.update(s, {
        "accept_epoch": If(new_epoch > State.accept_epoch(s), new_epoch, State.accept_epoch(s)),
        "zab_state": SYNC,
    })
    return If(And(is_state(s, FOLLOWING, None),
                  is_my_leader(s, m_from(m)),
                  new_epoch > State.accept_epoch(s),
                  State.zab_state(s) == DISCOVERY),
              updated_state,
              s)


def ack_epoch_handler(s, m):
    leader_ss = SS.mk(State.epoch(s), get_last_zxid(s))
    follower_ss = SS.mk(m_epoch(m), m_zxid(m))
    log_ok = Not(is_more_recent_than(leader_ss, follower_ss))
    # NOTE: I merged countElectionQuorum and ElectionFinished together
    updated_election_state = DV.election__store(State.election_state(s), m_from(m)-1, m_zxid(m))
    return If(And(is_state(s, LEADING, None),
                  is_my_learner(s, m_from(m))),
              If(election_finished(State.election_state(s)),
                 State.update(s, {"election_state": updated_election_state}),
                 If(And(SS.epoch(follower_ss) == -1, State.zab_state(s) == DISCOVERY),
                    State.update(s, {"election_state": updated_election_state}),
                    If(And(log_ok, State.zab_state(s) == DISCOVERY),
                       If(election_finished(updated_election_state),
                          State.update(s, {"election_state": updated_election_state, "epoch": State.accept_epoch(s),
                                           "zab_state": SYNC}),
                          State.update(s, {"election_state": updated_election_state})),
                       s))),
              s)

# def trunc_handler(s, m):
#     trunc_zxid = m_zxid(m)
#     trunc_index = zxid_to_index(s, trunc_zxid)
#     last_committed_index = zxid_to_index(s, State.last_committed(s))
#     # trunc_ok = And(trunc_index >= last_committed_index,
#     #                trunc_index < State.log__length(s))
#     trunc_ok = And(trunc_index < State.log__length(s), trunc_index != -1)
#     return If(And(is_state(s, FOLLOWING, SYNC), m_type(m) == TRUNC, is_my_leader(s, m_from(m)),
#                   trunc_ok),
#               # It shall be fine to trunc by adjust length
#               # `last_processed` is got by `log__length`, so we don't need modify for it
#               State.update(s, {"log__length": trunc_index + 1, "last_committed": trunc_zxid}),
#               s)

# def sync_handler(s, m):

def clear_ackid(entry):
    return Command.mk(Command.index(entry), Command.op(entry), *[False for _ in range(NUM_SERVERS)], NUM_SERVERS)


def new_leader_handler(s, m):
    updated_election_state = DV.update(State.election_state(s), {"connect_info": CI.mk(m_from(m), True)})
    return If(And(is_state(s, FOLLOWING, SYNC), is_my_leader(s, m_from(m))),
              State.update(s, {"epoch": State.accept_epoch(s), "election_state": updated_election_state, 'log__0': clear_ackid(m_log__0(m)), 'log__1': clear_ackid(m_log__1(m)), 'log__2': clear_ackid(m_log__2(m)), 'log__length': m_log__length(m), 'last_committed': Zxid.mk(m_commit__epoch(m), m_commit__index(m)), 'last_processed': Zxid.mk(m_commit__epoch(m), m_commit__index(m))}),
              s)


def ack_ld_handler(s, m):
    updated_election_state = add_ack_ld(State.election_state(s), m_from(m))
    return If(And(is_state(s, LEADING, None), is_my_learner(s, m_from(m)),
                  Not(is_state(s, None, DISCOVERY)),
                  Zxid.epoch(m_zxid(m)) == State.accept_epoch(s)),
              If(And(Not(has_quorum(State.election_state(s), DV.ack_ld_recv)), has_quorum(updated_election_state, DV.ack_ld_recv)),
                 State.update(s, {"election_state": updated_election_state, "zab_state": BROADCAST, "last_committed": get_last_zxid(s), "last_processed": get_last_zxid(s)}),
                 State.update(s, {"election_state": updated_election_state})),
              s)

def up_to_date_handler(s, m):
    return If(And(is_state(s, FOLLOWING, None), is_my_leader(s, m_from(m))),
              State.update(s, {"zab_state": BROADCAST, "last_committed": get_last_zxid(s), "last_processed": get_last_zxid(s)}),
              s)


def request_handler(s, m):
    new_txn = Command.mk(inc_zxid(State.epoch(s), get_last_zxid(s)), Op.mk(ENTRY_NORMAL, m_req_id(m)), *[False for _ in range(NUM_SERVERS)], NUM_SERVERS)
    new_s = add_log_entry(s, new_txn)
    return If(And(is_state(s, LEADING, BROADCAST)), new_s, s)


def proposal_handler(s, m):
    last_proccessed = get_last_zxid(s)
    new_txn = Command.mk(m_zxid(m), Op.mk(ENTRY_NORMAL, m_req_id(m)), *[False for _ in range(NUM_SERVERS)], NUM_SERVERS)
    new_s = add_log_entry(s, new_txn)
    return If(And(is_state(s, FOLLOWING, BROADCAST), is_my_leader(s, m_from(m)),
                  is_next_zxid(last_proccessed, m_zxid(m))),
              new_s, s)


def ack_handler(s, m):
    outstanding = zxid_l(State.last_committed(s), get_last_zxid(s))
    has_committed = Not(zxid_g(m_zxid(m), State.last_committed(s)))
    index = zxid_to_index(s, m_zxid(m))
    exists = index >= 0

    new_entry = add_ack_id(State.log__select(s, index, NoneCommand), m_from(m)-1)
    new_s = State.log__store(s, index, new_entry)

    new_last_committed = If(has_quorum(new_entry, Command.ackid), m_zxid(m), State.last_committed(s))
    new_s = State.update(new_s, {"last_committed": new_last_committed, 'last_processed': new_last_committed})
    return If(And(is_state(s, LEADING, BROADCAST),
                  And(outstanding, Not(has_committed))),
              If(And(exists, Previous_ACK(s, m_from(m), m_zxid(m))),
                 If(Command.ackid__select(State.log__select(s, index, NoneCommand), m_from(m)-1) != BoolVal(False),
                    s,
                    new_s),
                 s),
              s)


def commit_handler(s, m):
    index = zxid_to_index(s, State.last_committed(s))
    first_element_zxid = Command.index(State.log__select(s, index+1, NoneCommand))
    return If(And(is_state(s, FOLLOWING, BROADCAST), is_my_leader(s, m_from(m)), 
                  And(zxid_l(State.last_committed(s), m_zxid(m)), zxid_ge(get_last_zxid(s), m_zxid(m))),
                  first_element_zxid == m_zxid(m)),
              State.update(s, {"last_committed": m_zxid(m), "last_processed": m_zxid(m)}),
              # Use m_zxid(m) instead of first_element_zxid for simplicity
              s)


#################################
def mk_sym_message(prefix: str):
    prefix = prefix + '.message'
    return mk_message(
        Const(f'{prefix}_type', MessageType),
        Int(f'{prefix}_from'),
        Vote.mk_sym(f'{prefix}_vote'),
        Int(f'{prefix}_round'),
        Const(f'{prefix}_state', StateType),
        Int(f'{prefix}_epoch'),
        Zxid.mk_sym(f'{prefix}_req_id'),
        Int(f'{prefix}_req_id'),
        Command.mk_sym(f'{prefix}_log__0'),
        Command.mk_sym(f'{prefix}_log__1'),
        Command.mk_sym(f'{prefix}_log__2'),
        Int(f'{prefix}_log__length'),
        Int(f'{prefix}_commit__index'),
        Int(f'{prefix}_commit__epoch'),
    )


def update_message(m, kv: dict[z3.FuncDeclRef]):
    accessors = (m_type, m_from, m_vote, m_round, m_state, m_epoch, m_zxid, m_req_id, m_log__0, m_log__1, m_log__2, m_log__length, m_commit__index, m_commit__epoch)
    new_values = []
    for accessor in accessors:
        if accessor in kv:
            new_values.append(kv[accessor])
        else:
            new_values.append(accessor(m))
    return simplify(mk_message(*new_values))


mh = {
    # NOT: None,
    TCPCONNECT: TCP_connect_handler,
    FOLLOWERINFO: follower_info_handler,
    LEADERINFO: leader_info_handler,
    ACKEPOCH: ack_epoch_handler,
    # DIFF: None,
    # TRUNC: trunc_handler,
    # SNAP: None,
    NEWLEADER: new_leader_handler,
    UPTODATE: up_to_date_handler,
    CLIENT: request_handler,
    CLIENTRO: request_handler,
    PROPOSAL: proposal_handler,
    ACK: ack_handler,
    COMMIT: commit_handler,
    # CLIENTACK: None,
    # CLIENTRORESP: None,
    ACKLD: ack_ld_handler,
    TURNTOLEADER: turn_to_leader_after_timeout,
    TURNTOFOLLOWER: turn_to_follower_after_timeout,
    TIMEOUT: timeout,
}
messages = [update_message(mk_sym_message(str(message_type)), {m_type: message_type}) for message_type in mh.keys()]
handlers = list(mh.values())
state1 = State.mk_sym('s1')
state2 = State.mk_sym('s2')
m_ack = update_message(mk_sym_message("m_ack"), {m_type: ACK})
m_commit = update_message(mk_sym_message("m_commit"), {m_type: COMMIT})
m_ackld = update_message(mk_sym_message("m_ackld"), {m_type: ACKLD})


def Dominate(s1, s2):
    es1 = State.election_state(s1)
    es2 = State.election_state(s2)   
    return And(
        # last_committed
        zxid_le(State.last_committed(s2), State.last_committed(s1)),
        # equal
        State.me(s1) == State.me(s2),
        # election_state
        And(*[accessor(es1) == accessor(es2) for accessor in DV.accessors.values() if accessor not in (DV.ack_ld_recv)]),
        # If(has_quorum(es1, DV.ack_ld_recv), has_quorum(es2, DV.ack_ld_recv), es1 == es2),
        If(is_state(s1, None, BROADCAST), And(*[If(DV.ack_ld_recv[i](es2), DV.ack_ld_recv[i](es1), True) for i in range(NUM_SERVERS)]), es1 == es2),
        State.zab_state(s1) == State.zab_state(s2),
        State.epoch(s1) == State.epoch(s2),
        State.accept_epoch(s1) == State.accept_epoch(s2),
        # log
        *[zxid_eq(Command.index(State.log[i](s1)), Command.index(State.log[i](s2))) for i in range(NUM_ENTRIES)],
        *[Command.op(State.log[i](s1)) == Command.op(State.log[i](s2)) for i in range(NUM_ENTRIES)],
        *[ackid_subset(State.log[i](s2), State.log[i](s1)) for i in range(NUM_ENTRIES)],
        State.log__length(s1) == State.log__length(s2),
        zxid_le(State.last_processed(s2), State.last_processed(s1))
        # If(is_state(s1, None, DISCOVERY), State.last_processed(s1) == State.last_processed(s2), True),
    )

# this is used to filter out only valid messages in real system. In general, it specify that when s receives a message in the real system, the message must satisfy some constraints. we don't test it in closure since these are always true in real system.
def message_constraints(message, state, prefix):
    constraints = []
    constraints.append(Or(*[m_from(message) == sid for sid in SERVER_IDS]))

    # NEWLEADER(SYNC) message's log's zxid must be in order
    constraints.append(If(m_type(message) == NEWLEADER, And(zxid_l(Command.index(m_log__0(message)), Command.index(m_log__1(message))), zxid_l(Command.index(m_log__1(message)), Command.index(m_log__2(message)))), True))
    # NEWLEADER(SYNC) message's commit index must be one of the log's zxid and the index < log_length
    constraints.append(If(m_type(message) == NEWLEADER, Or(And(m_log__length(message) >= 1, zxid_eq(Command.index(m_log__0(message)), Zxid.mk(m_commit__epoch(message), m_commit__index(message)))), And(m_log__length(message) >= 2, zxid_eq(Command.index(m_log__1(message)), Zxid.mk(m_commit__epoch(message), m_commit__index(message)))), And(m_log__length(message) >= 3, zxid_eq(Command.index(m_log__2(message)), Zxid.mk(m_commit__epoch(message), m_commit__index(message))))), True))
    # NEWLEADER(SYNC) message's all entires index should be >= 1
    constraints.append(If(m_type(message) == NEWLEADER, And(Zxid.id(Command.index(m_log__0(message))) >= 1, Zxid.id(Command.index(m_log__1(message))) >= 1, Zxid.id(Command.index(m_log__2(message))) >= 1), True))
    constraints.append(If(m_type(message) == NEWLEADER, m_log__length(message) <= NUM_ENTRIES, True))
    # NEWLEADER message's all log entries should satisfy that if a server ACK entry x+1, it must ACK entry x
    # And(*[If(And(i < State.log__length(state)-1, Follower_ACK_Entry(state, j, i)), Follower_ACK_Entry(state, j, i-1), True) for j in range(NUM_SERVERS) for i in range(NUM_ENTRIES)])

    # NEWLEADER(SYNC) message's all log entries's epoch should <= state's accept epoch
    constraints.append(If(And(m_type(message) == NEWLEADER), And(Zxid.epoch(Command.index(m_log__0(message))) <= State.accept_epoch(state), Zxid.epoch(Command.index(m_log__1(message))) <= State.accept_epoch(state), Zxid.epoch(Command.index(m_log__2(message))) <= State.accept_epoch(state)), True))
    constraints.append(If(And(m_type(message) == NEWLEADER, is_my_leader(state, m_from(message)), is_state(state, FOLLOWING, None)), zxid_ge(Zxid.mk(m_commit__epoch(message), m_commit__index(message)), State.last_committed(state)), True))
    # NEWLEADER(SYNC) must send after the 
    # since a leader will not send a proposal to follower A unless it has received ACKEPOCH from A. If A sends ACKEPOCH, it means that it already updates its acceptepoch to the leader's epoch.
    # so if s receives a PROPOSAL, then its local accept_epoch must be >= the proposal's zxid.epoch.
    constraints.append(If(m_type(message) == PROPOSAL, Zxid.epoch(m_zxid(message)) <= State.accept_epoch(state), True))

    # if s receives an TRUNC or UPTODATE from its leader, it means that it must receives NEWLEADER before.
    # constraints.append(If(And(m_type(message) == TRUNC, is_my_leader(state, m_from(message))), is_nl_received(state), True))
    constraints.append(If(And(m_type(message) == UPTODATE, is_my_leader(state, m_from(message))), is_nl_received(state), True))
    # ACKLD is only sent by follower to leader after receiving NEWLEADER from the leader. The leader must first change to SYNC phase before it sends out NEWLEADER.
    # so if a leader receives an ACKLD from a follower, then it must in SYNC or BROADCAST phase
    # constraints.append(If(And(m_type(message) == ACKLD, is_my_learner(state, m_from(message)), Zxid.epoch(m_zxid(message)) == State.accept_epoch(state), is_state(state, LEADING, None)), Or(is_state(state, LEADING, SYNC), is_state(state, LEADING, BROADCAST)), True))
    # a follower never receive a TRUNC whose zxid is > than its last_processed and <= its last_committed
    # constraints.append(If(And(m_type(message) == TRUNC, is_my_leader(state, m_from(message))), And(zxid_le(m_zxid(message), get_last_zxid(state)), zxid_g(m_zxid(message), State.last_committed(state))), True))

    # if a leader receives ACKEPOCH <= its accept epoch, then it must not wait for new epoch
    constraints.append(If(And(is_state(state, LEADING, None), m_type(message) == ACKEPOCH), Not(waiting_for_new_epoch(State.election_state(state))), True))

    # if a leader receives ACK, ACK's zxid must be in the leader's log
    constraints.append(If(And(is_state(state, LEADING, None), m_type(message) == ACK), zxid_to_index(state, m_zxid(message)) >= 0, True))


    return constraints

def state_constraints(state, prefix):
    constraints = []
    # constraints for fix-sized vector
    es = State.election_state(state)
    constraints.append(And(DV.connecting__length(es) == NUM_SERVERS,
                           DV.election__length(es) == NUM_SERVERS,
                           DV.learners__length(es) == NUM_SERVERS,
                           DV.ack_ld_recv__length(es) == NUM_SERVERS,
                           *[Command.ackid__length(State.log[i](state)) == NUM_SERVERS for i in range(NUM_ENTRIES)]))
    # if a server is in LOOKING state, then its election_state must be in ELECTION state
    constraints.append(If(is_state(state, LOOKING, None), is_state(state, None, ELECTION), True))
    # if a server is in ELECTION state, then its election_state must be in LOOKING state
    constraints.append(If(is_state(state, None, ELECTION), is_state(state, LOOKING, None), True))
    constraints.append(Or(*[State.me(state) == sid for sid in SERVER_IDS]))
    constraints.append(0 <= State.log__length(state))
    constraints.append(State.log__length(state) <= NUM_ENTRIES)
    # `last_committed` and 'last_processed' must be equal to some entry's zxid
    constraints.append(Or(*[And(i < State.log__length(state), zxid_eq(Command.index(accessor(state)), State.last_committed(state))) for i, accessor in enumerate(State.log)]))
    constraints.append(Or(*[And(i < State.log__length(state), zxid_eq(Command.index(accessor(state)), State.last_processed(state))) for i, accessor in enumerate(State.log)]))
    # `last_committed` and 'last_processed' must be equal if in BROADCAST state
    constraints.append(If(is_state(state, None, BROADCAST), State.last_committed(state) == State.last_processed(state), True))
    # # entry's zxid must be monotonically increasing
    constraints.append(And(*[If(i < State.log__length(state)-1, zxid_g(Command.index(accessor1(state)), Command.index(accessor2(state))), True) for i, (accessor1, accessor2) in enumerate(zip(State.log[1:], State.log[:-1]))]))
    # # all entries's index should be >= 1
    constraints.append(And(*[If(i < State.log__length(state), Zxid.id(Command.index(accessor(state))) >= 1, True) for i, accessor in enumerate(State.log)]))
    # accept epoch should >= all entries' zxid.acceptepoch
    constraints.append(And(*[Zxid.epoch(Command.index(accessor(state))) <= State.accept_epoch(state) for accessor in State.log]))
    # # # accept_epoch >= epoch
    constraints.append(State.accept_epoch(state) >= State.epoch(state))
    # if a leader in broadcast, then ack ld recv must be hasquorum
    constraints.append(If(is_state(state, LEADING, BROADCAST), has_quorum(State.election_state(state), DV.ack_ld_recv), True))
    # if a leader not in broadcast, it must not have quorum ack
    constraints.append(If(Not(is_state(state, LEADING, BROADCAST)), Not(has_quorum(State.election_state(state), DV.ack_ld_recv)), True))

    # # epoch should >= all entries' zxid.epoch if in broadcast state
    constraints.append(And(*[If(is_state(state, None, BROADCAST), Zxid.epoch(Command.index(accessor(state))) <= State.epoch(state), True) for accessor in State.log]))
    # # if a leader in sync/broadcast phase, or a follower received NEWLEADER, accept_epoch should be equal to epoch
    constraints.append(If(Or(is_state(state, LEADING, SYNC), is_state(state, LEADING, BROADCAST), And(is_state(state, FOLLOWING, None), is_nl_received(state))), State.accept_epoch(state) == State.epoch(state), True))
    # # if a follower in broadcast phase, it must receive NEWLEADER
    constraints.append(If(is_state(state, FOLLOWING, BROADCAST), And(Not(is_my_leader(state, CI_NOONE)), is_nl_received(state)), True))
    # # from last_committed to last_entry, all entries shouldn't have quorum ACK (otherwise it should update the last_committed)
    constraints.append(And(*[If(And(is_state(state, LEADING, BROADCAST), i < State.log__length(state), zxid_g(Command.index(accessor(state)), Zxid.mk(State.epoch(state), 0)), zxid_g(Command.index(accessor(state)), State.last_committed(state)), zxid_le(Command.index(accessor(state)), get_last_zxid(state))), Not(has_quorum(accessor(state), Command.ackid)), True) for i, accessor in enumerate(State.log)]))
    # # if a leader is in broadcast phase, it shouldn't have uncommitted entry whose zxid <= <epoch, 0>
    constraints.append(And(*[If(And(i < State.log__length(state), is_state(state, LEADING, BROADCAST), zxid_le(Command.index(accessor(state)), Zxid.mk(State.epoch(state), 0))), zxid_le(Command.index(accessor(state)), State.last_committed(state)), True) for i, accessor in enumerate(State.log)]))
    # a leader's should only be NOONE (it doens't use this)
    constraints.append(If(is_state(state, LEADING, None), is_my_leader(state, CI_NOONE), True))
    # if a follower is in DISCOVERY phase, it hasn't processed new leader yet(otherwise it should be in at least SYNC phase)
    constraints.append(If(is_state(state, FOLLOWING, DISCOVERY), Not(is_nl_received(state)), True))
    # temp max epoch should always be >= accept epoch
    constraints.append(If(is_state(state, LEADING, None), DV.temp_max_epoch(State.election_state(state)) >= State.accept_epoch(state), True))
    # if a leader is in SYNC, then it must not wait for new epoch
    constraints.append(If(Or(is_state(state, LEADING, SYNC), is_state(state, LEADING, BROADCAST)), Not(waiting_for_new_epoch(State.election_state(state))), True))
    # if a server ACK entry x+1, then it must also ACK entry x
    constraints.append(And(*[If(And(i < State.log__length(state), Follower_ACK_Entry(state, j+1, i)), Follower_ACK_Entry(state, j+1, i-1), True) for j in range(NUM_SERVERS) for i in range(NUM_ENTRIES)]))
    return constraints

def state_message_constraints(state, message, prefix):
    constraints = []
    # constraints.append(If(m_type(message) == ACK, Or(Not(is_state(state, LEADING, None)), Previous_ACK(state, m_from(message), m_zxid(message)), zxid_le(m_zxid(message), State.last_committed(state))), True))
    constraints.append(If(m_type(message) == ACKLD, Or(Zxid.epoch(m_zxid(message)) < State.accept_epoch(state), has_quorum(State.election_state(state), DV.ack_ld_recv)), True))
    return constraints

def can_swap(message1, message2):
    # Messages from the same sender arrive on one connection, in order, so they are never reordered.
    # Local events are not reordered with messages either: once a server times out, it clears its
    # connections (the messages pending on them are dropped), so there is nothing left to reorder with
    # the TIMEOUT. TURNTOLEADER and TURNTOFOLLOWER always follow a TIMEOUT (see
    # turn_to_leader_after_timeout), so the same holds for them.
    # return And(m_from(message1) != m_from(message2), m_type(message1) != TURNTOLEADER, m_type(message1) != TURNTOFOLLOWER, m_type(message2) != TURNTOLEADER, m_type(message2) != TURNTOFOLLOWER)
    return And(m_from(message1) != m_from(message2), m_type(message1) != TIMEOUT, m_type(message2) != TIMEOUT, m_type(message1) != TURNTOLEADER, m_type(message2) != TURNTOLEADER, m_type(message1) != TURNTOFOLLOWER, m_type(message2) != TURNTOFOLLOWER)

def check_message_constraints_valid(messages, handlers):
    for message_to_check in messages:
        for message, handler in zip(messages, handlers):
            s = Solver()
            s.add(And(*message_constraints(message_to_check, state1, 'm_to_check')))
            s.add(And(*message_constraints(message, state1, 'm')))
            s.add(And(*state_constraints(state1, 's1')))
            s.add(can_swap(message_to_check, message))
            s.add(And(*message_constraints(message_to_check, handler(state1, message), 'handler(s1, m)')) == False)

            if s.check() == sat:
                print(f'Valid message constraints violated for {simplify(m_type(message_to_check))}')
                model = s.model()
                # model_dict = model_to_dict(model)
                m_message = simplify(model.eval(message))
                rich.print(f'{m_message=}')

                m_state1 = simplify(model.eval(state1))
                rich.print(f'{m_state1=}')

                after_apply = simplify(handler(model.eval(state1), model.eval(message)))
                rich.print(f'{after_apply=}')

                m_to_check = simplify(model.eval(message_to_check))
                rich.print(f'{m_to_check=}')
                exit(0)
    print("Valid message constraints are satisfied.")

def check_constraints_valid(messages, handlers):
    for message, handler in zip(messages, handlers):
        s = Solver()
        s.add(And(*message_constraints(message, state1, 'm')))
        s.add(And(*state_constraints(state1, 's1')))
        s.add(And(*state_constraints(handler(state1, message), 'handler(s1)')) == False)
        # after_constraints = state_constraints(handler(state1, message), 'handler(s1)')
        # for i in range(len(after_constraints)):
        # s.add(after_constraints[i] == False)
        if s.check() == sat:
            # print('state constraint index is ', i)
            print(f'Constraints validity violated for {simplify(m_type(message))}')
            model = s.model()
            # model_dict = model_to_dict(model)
            m_message = simplify(model.eval(message))
            rich.print(f'{m_message=}')
            m_state1 = simplify(model.eval(state1))
            rich.print(f'{m_state1=}')
            after_apply = simplify(handler(model.eval(state1), model.eval(message)))
            rich.print(f'{after_apply=}')
            exit(0)
        # s = Solver()
            # s.add(And(*message_constraints(message, state1, 'm')))
            # s.add(And(*state_constraints(state1, 's1')))
    print("State constraints are valid.")


def check_statement_1(messages, handlers):
    for message, handler in zip(messages, handlers):
        s = Solver()
        s.add(And(*message_constraints(message, state1, 'm')))
        s.add(And(*message_constraints(message, state2, 'm')))
        s.add(And(*state_constraints(state1, 's1')))
        s.add(And(*state_constraints(state2, 's2')))

        s.add(Dominate(state1, state2))
        s.add(Dominate(handler(state1, message), handler(state2, message)) == False)
        if s.check() == sat:
            print(f'Statement 1 violated for {simplify(m_type(message))}')
            model = s.model()
            # model_dict = model_to_dict(model)
            rich.print(simplify(model.eval(state1)))
            rich.print(simplify(model.eval(state2)))
            rich.print(simplify(Dominate(model.eval(state1), model.eval(state2))))
            after_apply1 = simplify(model.eval(simplify(handler(model.eval(state1), model.eval(message)))))
            after_apply2 = simplify(model.eval(simplify(handler(model.eval(state2), model.eval(message)))))
            rich.print(f'{after_apply1=}')
            rich.print("----------------------------------")
            rich.print(f'{after_apply2=}')
            rich.print(simplify(Dominate(after_apply1, after_apply2)))
            exit(0)
    print("Statement 1 is satisfied")


def check_statement_2(messages, handlers, m_to_check, m_handler):
    for message, handler in zip(messages, handlers):
        s = Solver()
        s.add(And(*message_constraints(message, state1, 'message')))
        s.add(And(*message_constraints(m_to_check, state1, 'm_to_check')))
        s.add(And(*state_constraints(state1, 's1')))
        s.add(And(*state_message_constraints(state1, m_to_check, 'm_to_check')))
        s.add(can_swap(message, m_to_check))
        s.add(Dominate(handler(m_handler(state1, m_to_check), message), m_handler(handler(state1, message), m_to_check)) == False)
        if s.check() == sat:
            print(f"Statement 2 for {simplify(m_type(m_to_check))} is violated")
            model = s.model()
            rich.print(model.eval(message))
            rich.print(model.eval(m_to_check))
            rich.print(model.eval(state1))
            apply_1 = simplify(model.eval(simplify(m_handler(model.eval(state1), model.eval(m_to_check)))))
            rich.print(f'{apply_1=}')
            rich.print("----------------------------------")
            after_apply1 = simplify(model.eval(simplify(handler(m_handler(model.eval(state1), model.eval(m_to_check)), model.eval(message)))))
            after_apply2 = simplify(model.eval(simplify(m_handler(handler(model.eval(state1), model.eval(message)), model.eval(m_to_check)))))
            rich.print(f'{after_apply1=}')
            rich.print("----------------------------------")
            rich.print(f'{after_apply2=}')
            exit(0)
    print(f"Statement 2 for {simplify(m_type(m_to_check))} is satisfied")


def check_statement_3(m_to_check, handler_to_check):
    s = Solver()
    s.add(And(*message_constraints(m_to_check, state1, 'm_to_check')))
    s.add(And(*state_constraints(state1, 's1')))
    s.add(And(*state_message_constraints(state1, m_to_check, 'm_to_check')))
    s.add(Dominate(handler_to_check(state1, m_to_check), state1) == False)
    if s.check() == sat:
        print(f"Statement 3 for {simplify(m_type(m_to_check))} is violated")
        model = s.model()
        message = simplify(model.eval(m_to_check))
        rich.print(f'{message=}')
        state = simplify(model.eval(state1))
        rich.print(f'{state=}')
        after_apply = simplify(model.eval(simplify(handler_to_check(state, message))))
        rich.print(f'{after_apply=}')
        exit(0)
    print(f"Statement 3 for {simplify(m_type(m_to_check))} is satisfied")

def check_statement_4(m_to_check):
    cons = state_message_constraints(state1, m_to_check, 'm_to_check')
    if len(cons) == 0:
        return
    for message, handler in zip(messages, handlers):
        s = Solver()
        s.add(And(*message_constraints(message, state1, 'message')))
        s.add(And(*message_constraints(m_to_check, state1, 'm_to_check')))
        s.add(And(*(state_constraints(state1, 's1'))))
        s.add(can_swap(message, m_to_check))
        # initial constraints
        s.add(And(*state_message_constraints(state1, m_to_check, 'm_to_check')))
        cons_after = state_message_constraints(handler(state1, message), m_to_check, 'handler(s1, message)')
        s.add(Not(And(*cons_after)))
        if s.check() == sat:
            print(f"Statement 4 for {simplify(m_type(m_to_check))} is violated")
            model = s.model()
            rich.print(model.eval(message))
            rich.print(model.eval(m_to_check))
            rich.print(model.eval(state1))
            apply_1 = simplify(model.eval(simplify(handler(model.eval(state1), model.eval(message)))))
            rich.print(f'{apply_1=}')
            exit(0)
            return
    print("statement 4 is satisfied")

check_message_constraints_valid(messages, handlers)
check_constraints_valid(messages, handlers)
check_statement_1(messages, handlers)
check_statement_2(messages, handlers, m_ack, ack_handler)
check_statement_3(m_ack, ack_handler)
check_statement_4(m_ack)

check_statement_2(messages, handlers, m_ackld, ack_ld_handler)
check_statement_3(m_ackld, ack_ld_handler)
check_statement_4(m_ackld)

# s = State.update(state1, {'last_committed': Zxid.mk(1, 1),
#                           'log__0': Command.mk(Zxid.mk(1, 1), Op.mk(ENTRY_NORMAL, 0), *[False for _ in range(NUM_SERVERS)], NUM_SERVERS),
#                           'log__1': Command.mk(Zxid.mk(1, 2), Op.mk(ENTRY_NORMAL, 1), *[False for _ in range(NUM_SERVERS)], NUM_SERVERS),
#                           'log__2': Command.mk(Zxid.mk(1, 3), Op.mk(ENTRY_NORMAL, 2), *[False for _ in range(NUM_SERVERS)], NUM_SERVERS),
#                           'log__length': 3,
#                           'zab_state': BROADCAST,
#                           'election_state': DV.update(State.election_state(state1), {'state': FOLLOWING}),
#                           })
# m1 = update_message(mk_sym_message("m1"), {m_type: COMMIT, m_zxid: Zxid.mk(1, 3)})
# m2 = update_message(mk_sym_message("m2"), {m_type: COMMIT, m_zxid: Zxid.mk(1, 2)})

# print(simplify(commit_handler(commit_handler(s, m1), m2)))
# print(simplify(commit_handler(commit_handler(s, m2), m1)))
from enum import Enum
import sys
import random
import concurrent.futures
import threading
import time
from typing import Dict, List, Optional

import grpc

import raft_pb2_grpc as pb2_grpc
import raft_pb2 as pb2


[HEARTBEAT_DURATION, ELECTION_DURATION_FROM, ELECTION_DURATION_TO] = [x for x in [50, 150, 300]]


class ServerType(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class Node:
    def __init__(self, id: int, host: str, port: int, stub: Optional[pb2_grpc.RaftNodeStub]):
        self.id = id
        self.host = host
        self.port = port
        self.stub = stub

        # exclusively for the leader
        self.next_index = 0
        self.match_index = -1


# Log entity
class Log:
    def __init__(self, index: int, term_number: int, command: str):
        self.index = index
        self.term_number = term_number
        self.command = command


# Internal state of the leader
class State:
    def __init__(self, id: int, nodes: Dict[int, Node], debug: bool = False):
        self.id = id
        self.type: ServerType = ServerType.FOLLOWER
        self.term: int = 0
        self.leader_id: int = -1

        self.storage: Dict[str, str] = {}
        self.logs: List[Log] = []
        self.temp_logs: List[Log] = []

        self.commit_index: int = -1
        self.last_applied: int = -1

        self.is_suspended = False
        self.is_terminating = False
        self.state_lock = threading.Lock()
        self.election_timer_fired = threading.Event()
        self.heartbeat_events: Dict[int, threading.Event] = {}
        self.debug = debug

        self.election_campaign_timer: Optional[threading.Timer] = None
        self.election_timeout: float = -1
        self.nodes = nodes  # dictionary id : Node
        self.vote_count: int = 0
        self.voted_for_id: int = -1


def get_last_log() -> Log:
    global STATE

    last_log = Log(-1, -1, "")
    if len(STATE.logs) > 0:
        last_log = STATE.logs[-1]
    return last_log


def exit_with_error(error_msg: str):
    print(f"\nERROR: {error_msg}")
    sys.exit()


# Election timer function
def select_election_timeout():
    global ELECTION_DURATION_FROM, ELECTION_DURATION_TO

    return random.randrange(ELECTION_DURATION_FROM, ELECTION_DURATION_TO) * 0.001


def reset_election_campaign_timer():
    global STATE

    stop_election_campaign_timer()
    STATE.election_campaign_timer = threading.Timer(STATE.election_timeout, STATE.election_timer_fired.set)
    STATE.election_campaign_timer.start()


def select_new_election_timeout_duration():
    global STATE

    STATE.election_timeout = select_election_timeout()


def stop_election_campaign_timer():
    global STATE

    if STATE.election_campaign_timer:
        STATE.election_campaign_timer.cancel()


# Elections
def start_election():
    global STATE

    with STATE.state_lock:
        if STATE.is_terminating:
            return

        print("The leader is dead")
        STATE.type = ServerType.CANDIDATE
        STATE.leader_id = -1
        STATE.term += 1

        # vote for ourselves
        STATE.vote_count = 1
        STATE.voted_for_id = STATE.id

    print(f"I am a candidate. Term: {STATE.term}")
    for id in STATE.nodes.keys():
        if id != STATE.id:
            t = threading.Thread(target=request_vote_worker_thread, args=(id,))
            t.start()

    # now RequestVote threads have started,
    # lets set a timer for the end of the election
    reset_election_campaign_timer()


def has_enough_votes():
    global STATE

    required_votes = (len(STATE.nodes) // 2) + 1
    return STATE.vote_count >= required_votes


def finalize_election():
    global STATE

    stop_election_campaign_timer()
    with STATE.state_lock:
        if STATE.type != ServerType.CANDIDATE:
            return

        if has_enough_votes():

            # Become a leader
            STATE.type = ServerType.LEADER
            STATE.leader_id = STATE.id
            STATE.vote_count = 0
            STATE.voted_for_id = -1

            start_heartbeats()
            print("Votes received")
            display_type_term_info()
            return

        """If election was unsuccessful
        then pick new timeout duration"""
        become_a_follower()
        select_new_election_timeout_duration()
        reset_election_campaign_timer()


def become_a_follower():
    global STATE

    if STATE.type != ServerType.FOLLOWER:
        STATE.type = ServerType.FOLLOWER
        display_type_term_info()
        STATE.voted_for_id = -1
        STATE.vote_count = 0


# Heartbeats
def start_heartbeats():
    global STATE

    for event in STATE.heartbeat_events.values():
        event.set()


# Thread functions
def request_vote_worker_thread(id_to_request):
    global STATE

    ensure_connected(id_to_request)
    node = STATE.nodes[id_to_request]

    saved_current_term = STATE.term

    try:
        if not node.stub:
            return

        with STATE.state_lock:
            saved_last_log = get_last_log()

        response = node.stub.RequestVote(
            pb2.VoteArgs(
                term=saved_current_term,
                candidate_id=STATE.id,
                last_log_index=saved_last_log.index,
                last_log_term=saved_last_log.term_number,
            ),
            timeout=0.1,
        )

        if not response.result:
            reset_election_campaign_timer()

            with STATE.state_lock:
                if STATE.type != ServerType.CANDIDATE or STATE.is_suspended:
                    return

                if saved_current_term < response.term:
                    STATE.term = response.term
                    become_a_follower()
                    return
                elif saved_current_term == response.term:
                    STATE.vote_count += 1

            # got enough votes, no need to wait for the end of the timeout
            if has_enough_votes():
                finalize_election()

    except grpc.RpcError:
        return


def election_timeout_thread():
    global STATE

    while not STATE.is_terminating:
        if STATE.election_timer_fired.wait(timeout=0.5):

            STATE.election_timer_fired.clear()
            if STATE.is_suspended:
                continue

            # election timer just fired
            if STATE.type == ServerType.FOLLOWER:
                # node didn't receive any heartbeats on time
                # that's why it should become a candidate

                start_election()
            elif STATE.type == ServerType.CANDIDATE:
                # okay, election is over
                # we need to count votes
                finalize_election()
            # if somehow we got here while being a leader,
            # then do nothing


def update_commit_index():
    global STATE

    total_nodes = len(STATE.nodes)

    """
    find the maximum index of the log,
    which is committed to more than a half of followers
    """
    for i in range(STATE.commit_index + 1, len(STATE.logs)):
        log = STATE.logs[i]
        if log.term_number == STATE.term:
            match_count = 1
            for node in STATE.nodes.values():
                if node.id == STATE.id:
                    continue
                if node.match_index >= i:
                    match_count += 1
            if match_count * 2 > total_nodes:
                with STATE.state_lock:
                    STATE.commit_index = i

    with STATE.state_lock:
        if STATE.commit_index > STATE.last_applied:
            for l in STATE.logs[STATE.last_applied + 1 : STATE.commit_index + 1]:
                # apply committed changes to the storage
                add_to_storage(l.command)
            STATE.last_applied = STATE.commit_index


def heartbeat_thread(id_to_request):
    global STATE

    while not STATE.is_terminating:
        try:
            if STATE.heartbeat_events[id_to_request].wait(timeout=0.150):
                STATE.heartbeat_events[id_to_request].clear()

                if (STATE.type != ServerType.LEADER) or STATE.is_suspended:
                    continue

                ensure_connected(id_to_request)
                node = STATE.nodes[id_to_request]
                if not node.stub:
                    return

                with STATE.state_lock:
                    current_term = STATE.term
                    current_commit_index = STATE.commit_index
                    current_logs = STATE.logs

                prev_log_index = node.next_index - 1
                prev_log_term = -1
                if prev_log_index >= 0:
                    prev_log_term = current_logs[prev_log_index].term_number

                entries = list(map(lambda x: pb2.HeartbeatArgs.Entry(**x.__dict__), current_logs[node.next_index :]))

                payload = pb2.HeartbeatArgs(
                    term=current_term,
                    leader_id=STATE.id,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    entries=entries,
                    leader_commit=current_commit_index,
                )

                response = node.stub.AppendEntries(
                    payload,
                    timeout=0.050,
                )

                threading.Timer(HEARTBEAT_DURATION * 0.001, STATE.heartbeat_events[id_to_request].set).start()

                if (STATE.type != ServerType.LEADER) or STATE.is_suspended:
                    continue

                with STATE.state_lock:
                    if STATE.term < response.term:
                        reset_election_campaign_timer()
                        STATE.term = response.term
                        become_a_follower()

                if response.result:
                    node.next_index += len(entries)
                    node.match_index = node.next_index - 1

                    update_commit_index()
                else:
                    node.next_index = max(0, node.next_index - 1)

            elif STATE.type == ServerType.LEADER:
                STATE.heartbeat_events[id_to_request].set()

        except grpc.RpcError:
            # Create new channel if something went wrong
            create_stub(id_to_request)
            continue


# storage functions
def add_to_storage(command: str):
    key, value = parse_command(command)
    STATE.storage[key] = value


# helpers that sets timers running again
# when suspend has ended
def wake_up_after_suspend():
    global STATE

    print("Server is wake up now!")
    STATE.is_suspended = False
    if STATE.type == ServerType.LEADER:
        start_heartbeats()
    else:
        reset_election_campaign_timer()


# gRPC server handler
class Handler(pb2_grpc.RaftNodeServicer):
    def RequestVote(self, request, context):
        global STATE

        if STATE.is_suspended:
            return

        reset_election_campaign_timer()
        with STATE.state_lock:
            response = {"result": False, "term": STATE.term}
            if STATE.term < request.term:
                STATE.term = request.term
                become_a_follower()

            last_log = get_last_log()
            if (
                (STATE.term == request.term)
                and (STATE.voted_for_id == -1)
                and (request.last_log_index >= last_log.index)
                and not (last_log.index != -1 and request.last_log_term != last_log.term_number)
            ):
                response = {"result": True, "term": STATE.term}
                STATE.voted_for_id = request.candidate_id
                print(f"Voted for node {STATE.voted_for_id}")
            return pb2.ResultWithTerm(**response)

    def AppendEntries(self, request, context):
        global STATE

        if STATE.is_suspended:
            return

        reset_election_campaign_timer()

        with STATE.state_lock:
            response = {"result": False, "term": STATE.term}
            if STATE.term < request.term:
                STATE.term = request.term
                become_a_follower()

            if STATE.term == request.term:
                STATE.leader_id = request.leader_id
                if STATE.type != ServerType.FOLLOWER:
                    become_a_follower()

                last_log = get_last_log()
                if request.prev_log_index == -1 or (
                    request.prev_log_index < len(STATE.logs) and request.prev_log_term == last_log.term_number
                ):
                    response = {"result": True, "term": STATE.term}
                    log_insert_index = request.prev_log_index + 1
                    new_entries_index = 0

                    while True:
                        if log_insert_index >= len(STATE.logs) or new_entries_index >= len(request.entries):
                            break
                        if STATE.logs[log_insert_index].term_number != request.entries[new_entries_index].term_number:
                            break
                        log_insert_index += 1
                        new_entries_index += 1

                    if new_entries_index < len(request.entries):
                        STATE.logs[log_insert_index:] = list(
                            map(
                                lambda entry: Log(entry.index, entry.term_number, entry.command),
                                request.entries[new_entries_index:],
                            )
                        )

                    for log in STATE.logs[log_insert_index:]:
                        add_to_storage(log.command)

                    if request.leader_commit > STATE.commit_index:
                        STATE.commit_index = min(request.leader_commit, len(STATE.logs) - 1)

        return pb2.ResultWithTerm(**response)

    def GetLeader(self, request, context):
        global STATE

        if STATE.is_suspended:
            return

        leader_node = STATE.nodes[STATE.leader_id]
        response = {"leader_id": STATE.leader_id, "leader_addr": f"{leader_node.host}:{leader_node.port}"}
        return pb2.LeaderResp(**response)

    def Suspend(self, request, context):
        global STATE

        if STATE.is_suspended:
            return

        STATE.is_suspended = True
        print("Server is sleeping...")
        threading.Timer(request.duration, wake_up_after_suspend).start()
        return pb2.Empty()

    def GetVal(self, request, context):
        global STATE

        if STATE.is_suspended:
            return

        value = STATE.storage.get(request.key, None)
        return pb2.ResultWithVal(**{"result": value != None, "value": value or ""})

    def SetVal(self, request, context):
        global STATE

        if STATE.is_suspended:
            return

        if STATE.type == ServerType.LEADER:
            with STATE.state_lock:
                last_log = get_last_log()
                current_last_log_index = last_log.index + 1
                STATE.logs.append(Log(current_last_log_index, STATE.term, f"{request.key} {request.value}"))

                return pb2.Result(**{"result": True})

        if STATE.type == ServerType.CANDIDATE or STATE.leader_id == -1:
            return pb2.Result(**{"result": False})

        leader_node = STATE.nodes[STATE.leader_id]

        try:
            ensure_connected(STATE.leader_id)
            if not leader_node.stub:
                raise grpc.RpcError()

            response = leader_node.stub.SetVal(request, timeout=2)

        except grpc.RpcError:
            return pb2.Result(**{"result": False})

        return pb2.Result(**{"result": response.result})


def create_stub(id):
    node = STATE.nodes[id]
    channel = grpc.insecure_channel(f"{node.host}:{node.port}")
    node.stub = pb2_grpc.RaftNodeStub(channel)


def ensure_connected(id):
    global STATE

    if id == STATE.id:
        raise BaseException("Shouldn't try to connect to itself")

    node = STATE.nodes[id]
    if not node.stub:
        create_stub(id)


def start_server():
    global STATE

    self_node = STATE.nodes[STATE.id]
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_RaftNodeServicer_to_server(Handler(), server)
    server.add_insecure_port(f"{self_node.host}:{self_node.port}")
    server.start()
    return server


def get_cmd_args():
    try:
        id = int(sys.argv[1])
    except:
        exit_with_error("Expected one cmd argument: <id>")
    return id


def read_nodes(config_path: str) -> Dict[int, Node]:
    nodes = {}
    with open(config_path, "r") as f:
        parts = list(map(lambda line: line.split(), f.readlines()))

        for part in parts:
            nodes[int(part[0])] = Node(int(part[0]), part[1], int(part[2]), None)

    return nodes


def display_type_term_info():
    global STATE

    print(f"I am a {STATE.type.value}. Term: {STATE.term}")


def parse_command(command: str):
    parts = command.strip().split()
    return parts[0], " ".join(parts[1:])


if __name__ == "__main__":

    id = get_cmd_args()
    nodes = read_nodes("config.conf")
    STATE = State(id, nodes, True)

    heartbeat_events: Dict[int, threading.Event] = {}

    election_thread = threading.Thread(target=election_timeout_thread)
    election_thread.start()

    for node in STATE.nodes.values():
        if STATE.id != node.id:
            heartbeat_events[node.id] = threading.Event()

    STATE.heartbeat_events = heartbeat_events

    heartbeat_threads = []
    for node in STATE.nodes.values():
        if STATE.id != node.id:
            t = threading.Thread(target=heartbeat_thread, args=(node.id,))
            t.start()
            heartbeat_threads.append(t)

    server = start_server()

    self_node = STATE.nodes[STATE.id]
    print(f"The server starts at {self_node.host}:{self_node.port}")

    display_type_term_info()
    select_new_election_timeout_duration()
    reset_election_campaign_timer()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        with STATE.state_lock:
            STATE.is_terminating = True

        server.stop(0)
        print("\nShutting down")

        # Close threads
        election_thread.join()
        [t.join() for t in heartbeat_threads]

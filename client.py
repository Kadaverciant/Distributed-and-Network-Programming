from typing import Optional
import grpc

import raft_pb2_grpc as pb2_grpc
import raft_pb2 as pb2


class State:
    def __init__(self):
        self.working: bool = True
        self.node_address: Optional[str] = None
        self.stub: Optional[pb2_grpc.RaftNodeStub] = None


def ensure_connected(func):
    def wrapper(*args, **kwargs):
        state = args[-1]

        if not state.node_address:
            return "No address to connect to was specified"

        try:

            channel = grpc.insecure_channel(state.node_address)
            state.stub = pb2_grpc.RaftNodeStub(channel)

            response = func(*args, **kwargs)

        except grpc.RpcError as e:
            print(e)
            return "Server is unavailable"

        return response

    return wrapper


def cmd_connect(node_addr: str, state: State) -> Optional[str]:
    state.node_address = node_addr
    return ""


@ensure_connected
def cmd_get_leader(state: State):
    response = state.stub.GetLeader(pb2.Empty())  # type: ignore
    return f"{response.leader_id} {response.leader_addr}"


@ensure_connected
def cmd_suspend(duration, state: State):
    state.stub.Suspend(pb2.DurationArgs(duration=duration))  # type: ignore
    return ""


@ensure_connected
def cmd_set_val(key: str, value: str, state: State):
    response = state.stub.SetVal(pb2.SetArgs(key=key, value=value))  # type: ignore
    return ""


@ensure_connected
def cmd_get_val(key: str, state: State):
    response = state.stub.GetVal(pb2.GetArgs(key=key))  # type: ignore
    if not response.result:
        return "None"
    return response.value


def exec_cmd(line, state: State):
    parts = line.split()
    if parts[0] == "connect":
        if len(parts) < 3:
            return "Expected 2 arguments: <ip_addr> and <port>"
        return cmd_connect(f"{parts[1]}:{parts[2]}", state)

    elif parts[0] == "getleader":
        return cmd_get_leader(state)

    elif parts[0] == "suspend":
        if len(parts) < 2:
            return "Expected 1 argument: <seconds>"
        try:
            seconds = int(parts[1])
            if seconds < 0:
                raise BaseException
        except:
            return "Number of seconds should be a positive integer"
        return cmd_suspend(seconds, state)

    elif parts[0] == "setval":
        if len(parts) < 3:
            return "Expected 2 arguments: <key> <value>"
        return cmd_set_val(parts[1], parts[2], state)

    elif parts[0] == "getval":
        if len(parts) < 2:
            return "Expected 1 argument: <key>"
        return cmd_get_val(parts[1], state)

    elif parts[0] == "quit":
        state.working = False
        return "The client ends"

    return f"Unknown command {parts[0]}"


if __name__ == "__main__":
    state = State()
    try:
        while state.working:
            user_input = input("> ")
            output = exec_cmd(user_input, state)
            if output:
                print(output)
    except KeyboardInterrupt:
        print("\nShutting down")

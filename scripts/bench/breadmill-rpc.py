"""Socket RPC helper for the daemon-driven breadsearch benchmark.

Speaks newline JSON to a breadmill socket. Commands: status, query <text>,
unload, load. Prints one JSON line with the response. Exits nonzero on
socket errors, so failures are never silent passes.
"""

import json
import socket
import sys


def rpc(sock_path, payload):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stream:
        stream.settimeout(120)
        stream.connect(sock_path)
        stream.sendall((json.dumps(payload) + "\n").encode())
        chunks = []
        while True:
            data = stream.recv(65536)
            if not data:
                break
            chunks.append(data)
            if b"\n" in data:
                break
        return json.loads(b"".join(chunks).decode().split("\n")[0])


def main():
    sock_path = sys.argv[1]
    command = sys.argv[2]
    if command == "status":
        response = rpc(sock_path, {"type": "status"})
    elif command == "query":
        response = rpc(sock_path, {"type": "query", "query": sys.argv[3], "limit": 3})
    elif command == "unload":
        response = rpc(sock_path, {"type": "unload_model"})
    elif command == "load":
        response = rpc(sock_path, {"type": "load_model"})
    else:
        raise SystemExit(f"unknown command {command}")
    print(json.dumps(response))


main()

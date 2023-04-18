# DNP Final Project Solution 2022

## Improved RAFT system emulation

Written and tested on Windows 11 using Python 3.11

Done by

- Dmitry Beresnev **d.beresnev@innopolis.university**
- Vsevolod Klyushev **v.klyushev@innopolis.university**

## Useful commands

python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. --mypy_out=. raft.proto

python310 -m grpc_tools.protoc raft.proto --proto_path=. --python_out=. --grpc_python_out=.

# distributed-file-system

A centralized distributed file system in Go with:

- Master Tracker for metadata and orchestration
- Data Keeper nodes for file storage
- gRPC control-plane communication
- TCP data-plane file transfer

## Current Status

Initial implementation includes:

- Master gRPC service with heartbeat tracking, upload coordination, download target lookup, and periodic replication enforcement
- Data Keeper gRPC + TCP services for uploads/downloads and keeper-to-keeper replication
- Client upload/download workflow with round-robin target selection for uniform requests

## Run

Generate protobuf stubs (optional for now, because a manual API stub is already included in the baseline):

```bash
make -C proto gen
```

Start master:

```bash
go run ./cmd/master -addr :50051
```

Start data keepers in separate terminals:

```bash
go run ./cmd/datakeeper -id keeper-1 -host 127.0.0.1 -grpc :50061 -tcp 60061 -master 127.0.0.1:50051 -storage ./data/keeper-1
go run ./cmd/datakeeper -id keeper-2 -host 127.0.0.1 -grpc :50062 -tcp 60062 -master 127.0.0.1:50051 -storage ./data/keeper-2
go run ./cmd/datakeeper -id keeper-3 -host 127.0.0.1 -grpc :50063 -tcp 60063 -master 127.0.0.1:50051 -storage ./data/keeper-3
```

If you only have one terminal available, run keepers in background:

```bash
go run ./cmd/datakeeper -id keeper-1 -host 127.0.0.1 -grpc :50061 -tcp 60061 -master 127.0.0.1:50051 -storage ./data/keeper-1 &
go run ./cmd/datakeeper -id keeper-2 -host 127.0.0.1 -grpc :50062 -tcp 60062 -master 127.0.0.1:50051 -storage ./data/keeper-2 &
go run ./cmd/datakeeper -id keeper-3 -host 127.0.0.1 -grpc :50063 -tcp 60063 -master 127.0.0.1:50051 -storage ./data/keeper-3 &
wait
```

Upload:

```bash
go run ./cmd/client -master 127.0.0.1:50051 -upload /path/to/video.mp4
```

Download:

```bash
go run ./cmd/client -master 127.0.0.1:50051 -download video.mp4 -dest ./downloaded.mp4
```

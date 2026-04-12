package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/El-Maktab/distributed-file-system/pkg/datakeeper"
)

func main() {
	id := flag.String("id", "keeper-1", "data keeper id")
	host := flag.String("host", "127.0.0.1", "data keeper host advertised to clients")
	grpcAddr := flag.String("grpc", ":50061", "data keeper gRPC listen address")
	masterAddr := flag.String("master", "127.0.0.1:50051", "master gRPC address")
	tcpPort := flag.Int("tcp", 60061, "data keeper TCP file transfer port")
	storageDir := flag.String("storage", "./data/keeper-1", "data keeper storage directory")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	srv := datakeeper.NewServer(*id, *host, *grpcAddr, *masterAddr, *tcpPort, *storageDir)
	if err := srv.Run(ctx); err != nil {
		log.Fatalf("datakeeper exited with error: %v", err)
	}
}

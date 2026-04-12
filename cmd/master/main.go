package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/El-Maktab/distributed-file-system/pkg/master"
)

func main() {
	addr := flag.String("addr", ":50051", "master gRPC listen address")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	srv := master.NewServer()
	if err := srv.Run(ctx, *addr); err != nil {
		log.Fatalf("master exited with error: %v", err)
	}
}

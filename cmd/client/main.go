package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"path/filepath"

	"github.com/El-Maktab/distributed-file-system/pkg/client"
)

func main() {
	masterAddr := flag.String("master", "127.0.0.1:50051", "master gRPC address")
	uploadPath := flag.String("upload", "", "local mp4 path to upload")
	downloadName := flag.String("download", "", "mp4 filename to download from DFS")
	destPath := flag.String("dest", "", "destination path for download")
	flag.Parse()

	c := client.New(*masterAddr)
	ctx := context.Background()

	switch {
	case *uploadPath != "":
		if err := c.Upload(ctx, *uploadPath); err != nil {
			log.Fatalf("upload failed: %v", err)
		}
		fmt.Printf("upload successful: %s\n", filepath.Base(*uploadPath))
	case *downloadName != "":
		out := *destPath
		if out == "" {
			out = filepath.Base(*downloadName)
		}
		if err := c.Download(ctx, filepath.Base(*downloadName), out); err != nil {
			log.Fatalf("download failed: %v", err)
		}
		fmt.Printf("download successful: %s -> %s\n", filepath.Base(*downloadName), out)
	default:
		log.Fatalf("use -upload <file.mp4> or -download <file.mp4> [-dest path]")
	}
}

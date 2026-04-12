package client

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/El-Maktab/distributed-file-system/internal/config"
	"github.com/El-Maktab/distributed-file-system/internal/transfer"
	"github.com/El-Maktab/distributed-file-system/pkg/api/dfs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	masterAddr string

	mu         sync.Mutex
	nextTarget map[string]int
}

func New(masterAddr string) *Client {
	return &Client{
		masterAddr: masterAddr,
		nextTarget: make(map[string]int),
	}
}

func (c *Client) Upload(ctx context.Context, localPath string) error {
	filename := filepath.Base(localPath)
	if !transfer.IsMP4(filename) {
		return fmt.Errorf("only .mp4 files are accepted")
	}

	log.Printf("client: upload requested local_path=%s file=%s", localPath, filename)

	fi, err := os.Stat(localPath)
	if err != nil {
		return fmt.Errorf("stat local file: %w", err)
	}
	if fi.Size() <= 0 {
		return fmt.Errorf("file is empty")
	}

	checksum, err := transfer.ComputeSHA256File(localPath)
	if err != nil {
		return fmt.Errorf("compute checksum: %w", err)
	}

	rpcCtx, cancel := context.WithTimeout(ctx, config.RPCDeadline)
	defer cancel()
	masterConn, err := grpc.DialContext(rpcCtx, c.masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("dial master: %w", err)
	}
	defer masterConn.Close()
	master := dfs.NewMasterServiceClient(masterConn)

	alloc, err := master.RequestUpload(rpcCtx, &dfs.RequestUploadRequest{Filename: filename, Size: fi.Size(), Checksum: checksum})
	if err != nil {
		return fmt.Errorf("request upload: %w", err)
	}
	if !alloc.Ok {
		return fmt.Errorf("request upload rejected: %s", alloc.Error)
	}

	log.Printf("client: upload allocated upload_id=%s keeper=%s host=%s tcp=%d", alloc.UploadId, alloc.KeeperId, alloc.KeeperHost, alloc.KeeperTcpPort)

	addr := net.JoinHostPort(alloc.KeeperHost, fmt.Sprintf("%d", alloc.KeeperTcpPort))
	netConn, err := net.DialTimeout("tcp", addr, config.RPCDeadline)
	if err != nil {
		return fmt.Errorf("dial keeper tcp: %w", err)
	}
	defer netConn.Close()

	header := transfer.Header{
		Op:       transfer.OpPut,
		UploadID: alloc.UploadId,
		Filename: filename,
		Size:     fi.Size(),
		Checksum: checksum,
	}
	if err := transfer.WriteHeader(netConn, header); err != nil {
		return fmt.Errorf("send upload header: %w", err)
	}
	log.Printf("client: upload streaming started file=%s size=%d target=%s", filename, fi.Size(), addr)

	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open local file: %w", err)
	}
	defer file.Close()

	if _, err := io.CopyBuffer(netConn, file, make([]byte, config.TransferBufferSize)); err != nil {
		return fmt.Errorf("stream upload file: %w", err)
	}

	if tcpConn, ok := netConn.(*net.TCPConn); ok {
		_ = tcpConn.CloseWrite()
	}

	ackReader := bufio.NewReader(netConn)
	ack, err := transfer.ReadHeader(ackReader)
	if err != nil {
		return fmt.Errorf("read keeper ack: %w", err)
	}
	if ack.Op != transfer.OpAck {
		if ack.Error == "" {
			ack.Error = "upload rejected"
		}
		return fmt.Errorf("upload failed: %s", ack.Error)
	}

	log.Printf("client: upload acknowledged by keeper upload_id=%s", alloc.UploadId)

	deadline := time.Now().Add(config.UploadStatusTimeout)
	log.Printf("client: waiting for master confirmation upload_id=%s timeout=%s", alloc.UploadId, config.UploadStatusTimeout)
	for time.Now().Before(deadline) {
		statusResp, err := master.GetUploadStatus(context.Background(), &dfs.GetUploadStatusRequest{UploadId: alloc.UploadId})
		if err == nil && statusResp.Found {
			if statusResp.Completed {
				log.Printf("client: upload confirmed by master upload_id=%s", alloc.UploadId)
				return nil
			}
			if statusResp.Error != "" {
				return fmt.Errorf("master marked upload failed: %s", statusResp.Error)
			}
		}
		time.Sleep(config.UploadStatusPoll)
	}

	return fmt.Errorf("timed out waiting for master confirmation")
}

func (c *Client) Download(ctx context.Context, filename, destPath string) error {
	if !transfer.IsMP4(filename) {
		return fmt.Errorf("only .mp4 files are accepted")
	}

	log.Printf("client: download requested file=%s dest=%s", filename, destPath)

	rpcCtx, cancel := context.WithTimeout(ctx, config.RPCDeadline)
	defer cancel()
	masterConn, err := grpc.DialContext(rpcCtx, c.masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("dial master: %w", err)
	}
	defer masterConn.Close()
	master := dfs.NewMasterServiceClient(masterConn)

	resp, err := master.GetDownloadTargets(rpcCtx, &dfs.DownloadTargetsRequest{Filename: filename})
	if err != nil {
		return fmt.Errorf("get download targets: %w", err)
	}
	if !resp.Ok || len(resp.Targets) == 0 {
		return fmt.Errorf("download target lookup failed: %s", resp.Error)
	}

	log.Printf("client: download targets file=%s count=%d", filename, len(resp.Targets))

	start := c.nextTargetIndex(filename, len(resp.Targets))
	var lastErr error
	for i := 0; i < len(resp.Targets); i++ {
		t := resp.Targets[(start+i)%len(resp.Targets)]
		log.Printf("client: download attempt file=%s keeper=%s host=%s tcp=%d", filename, t.KeeperId, t.Host, t.TcpPort)
		if err := c.downloadFromTarget(ctx, t, filename, destPath); err == nil {
			c.bumpTargetIndex(filename, len(resp.Targets))
			log.Printf("client: download succeeded file=%s keeper=%s", filename, t.KeeperId)
			return nil
		} else {
			log.Printf("client: download attempt failed file=%s keeper=%s err=%v", filename, t.KeeperId, err)
			lastErr = err
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no target attempted")
	}
	return fmt.Errorf("all targets failed: %w", lastErr)
}

func (c *Client) downloadFromTarget(ctx context.Context, t *dfs.DownloadTarget, filename, destPath string) error {
	addr := net.JoinHostPort(t.Host, fmt.Sprintf("%d", t.TcpPort))
	netConn, err := net.DialTimeout("tcp", addr, config.RPCDeadline)
	if err != nil {
		return fmt.Errorf("dial target %s: %w", t.KeeperId, err)
	}
	defer netConn.Close()

	if err := transfer.WriteHeader(netConn, transfer.Header{Op: transfer.OpGet, Filename: filename}); err != nil {
		return fmt.Errorf("send get header: %w", err)
	}

	_ = netConn.SetReadDeadline(time.Now().Add(config.UploadStatusTimeout))
	reader := bufio.NewReader(netConn)
	h, err := transfer.ReadHeader(reader)
	if err != nil {
		return fmt.Errorf("read response header: %w", err)
	}
	if h.Op != transfer.OpData {
		if h.Error == "" {
			h.Error = "invalid data response"
		}
		return fmt.Errorf("target error: %s", h.Error)
	}

	tmp := destPath + ".part"
	file, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create dest file: %w", err)
	}
	written, copyErr := io.CopyBuffer(file, io.LimitReader(reader, h.Size), make([]byte, config.TransferBufferSize))
	closeErr := file.Close()
	if copyErr != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("download copy failed: %w", copyErr)
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close dest file: %w", closeErr)
	}
	if written != h.Size {
		_ = os.Remove(tmp)
		return fmt.Errorf("download size mismatch: got %d expected %d", written, h.Size)
	}

	checksum, err := transfer.ComputeSHA256File(tmp)
	if err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("download checksum failed: %w", err)
	}
	if h.Checksum != "" && checksum != h.Checksum {
		_ = os.Remove(tmp)
		return fmt.Errorf("download checksum mismatch")
	}

	if err := os.Rename(tmp, destPath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("finalize download file: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (c *Client) nextTargetIndex(filename string, n int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n <= 0 {
		return 0
	}
	idx := c.nextTarget[filename] % n
	return idx
}

func (c *Client) bumpTargetIndex(filename string, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n <= 0 {
		c.nextTarget[filename] = 0
		return
	}
	c.nextTarget[filename] = (c.nextTarget[filename] + 1) % n
}

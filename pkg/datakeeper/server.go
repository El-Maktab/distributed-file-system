package datakeeper

import (
	"bufio"
	"context"
	"errors"
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

type Server struct {
	dfs.UnimplementedDataKeeperServiceServer

	ID         string
	Host       string
	GRPCAddr   string
	MasterAddr string
	TCPPort    int
	StorageDir string

	mu                sync.Mutex
	heartbeatConnected bool
}

func NewServer(id, host, grpcAddr, masterAddr string, tcpPort int, storageDir string) *Server {
	return &Server{
		ID:         id,
		Host:       host,
		GRPCAddr:   grpcAddr,
		MasterAddr: masterAddr,
		TCPPort:    tcpPort,
		StorageDir: storageDir,
	}
}

func (s *Server) Run(ctx context.Context) error {
	if err := os.MkdirAll(s.StorageDir, 0o755); err != nil {
		return fmt.Errorf("create storage dir: %w", err)
	}

	grpcLis, err := net.Listen("tcp", s.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen keeper grpc: %w", err)
	}

	tcpLis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.TCPPort))
	if err != nil {
		return fmt.Errorf("listen keeper tcp: %w", err)
	}

	grpcServer := grpc.NewServer()
	dfs.RegisterDataKeeperServiceServer(grpcServer, s)

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
		_ = tcpLis.Close()
	}()

	go s.heartbeatLoop(ctx)
	go s.serveTCP(ctx, tcpLis)

	log.Printf("keeper[%s]: grpc=%s tcp=%d storage=%s", s.ID, s.GRPCAddr, s.TCPPort, s.StorageDir)
	if err := grpcServer.Serve(grpcLis); err != nil {
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		return fmt.Errorf("serve keeper grpc: %w", err)
	}
	return nil
}

func (s *Server) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sendHeartbeat(ctx)
		}
	}
}

func (s *Server) sendHeartbeat(ctx context.Context) {
	rpcCtx, cancel := context.WithTimeout(ctx, config.RPCDeadline)
	defer cancel()

	conn, err := grpc.DialContext(rpcCtx, s.MasterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		s.mu.Lock()
		s.heartbeatConnected = false
		s.mu.Unlock()
		log.Printf("keeper[%s]: heartbeat dial master failed: %v", s.ID, err)
		return
	}
	defer conn.Close()

	client := dfs.NewMasterServiceClient(conn)
	_, err = client.Heartbeat(rpcCtx, &dfs.HeartbeatRequest{
		KeeperId:          s.ID,
		GrpcAddr:          s.GRPCAddr,
		Host:              s.Host,
		TcpPort:           int32(s.TCPPort),
		TimestampUnixNano: time.Now().UnixNano(),
	})
	if err != nil {
		s.mu.Lock()
		s.heartbeatConnected = false
		s.mu.Unlock()
		log.Printf("keeper[%s]: heartbeat rpc failed: %v", s.ID, err)
		return
	}

	s.mu.Lock()
	wasConnected := s.heartbeatConnected
	s.heartbeatConnected = true
	s.mu.Unlock()
	if !wasConnected {
		log.Printf("keeper[%s]: heartbeat connected to master=%s", s.ID, s.MasterAddr)
	}
}

func (s *Server) serveTCP(ctx context.Context, lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				log.Printf("keeper[%s]: accept tcp failed: %v", s.ID, err)
				continue
			}
		}
		go s.handleTCPConn(ctx, conn)
	}
}

func (s *Server) handleTCPConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	h, err := transfer.ReadHeader(reader)
	if err != nil {
		log.Printf("keeper[%s]: read header failed: %v", s.ID, err)
		return
	}

	log.Printf("keeper[%s]: tcp request op=%s file=%s upload_id=%s from=%s", s.ID, h.Op, h.Filename, h.UploadID, conn.RemoteAddr())

	switch h.Op {
	case transfer.OpPut, transfer.OpReplicate:
		s.handlePutLike(ctx, conn, reader, h)
	case transfer.OpGet:
		s.handleGet(conn, h)
	case transfer.OpStat:
		s.handleStat(conn, h)
	default:
		_ = transfer.WriteHeader(conn, transfer.Header{Op: transfer.OpError, Error: "unsupported op"})
	}
}

func (s *Server) handlePutLike(ctx context.Context, conn net.Conn, reader *bufio.Reader, h transfer.Header) {
	if !transfer.IsMP4(h.Filename) {
		_ = transfer.WriteHeader(conn, transfer.Header{Op: transfer.OpError, Error: "only .mp4 files are accepted"})
		return
	}
	if h.Size <= 0 {
		_ = transfer.WriteHeader(conn, transfer.Header{Op: transfer.OpError, Error: "invalid file size"})
		return
	}

	log.Printf("keeper[%s]: receiving file op=%s file=%s size=%d", s.ID, h.Op, h.Filename, h.Size)

	relativePath := filepath.Base(h.Filename)
	absPath := filepath.Join(s.StorageDir, relativePath)
	tmpPath := absPath + ".part"

	f, err := os.Create(tmpPath)
	if err != nil {
		_ = transfer.WriteHeader(conn, transfer.Header{Op: transfer.OpError, Error: err.Error()})
		return
	}
	defer f.Close()

	limited := io.LimitReader(reader, h.Size)
	written, err := io.CopyBuffer(f, limited, make([]byte, config.TransferBufferSize))
	if err != nil {
		_ = transfer.WriteHeader(conn, transfer.Header{Op: transfer.OpError, Error: fmt.Sprintf("write file: %v", err)})
		_ = os.Remove(tmpPath)
		return
	}
	if written != h.Size {
		_ = transfer.WriteHeader(conn, transfer.Header{Op: transfer.OpError, Error: fmt.Sprintf("size mismatch: got %d expected %d", written, h.Size)})
		_ = os.Remove(tmpPath)
		return
	}

	if err := f.Sync(); err != nil {
		_ = transfer.WriteHeader(conn, transfer.Header{Op: transfer.OpError, Error: fmt.Sprintf("sync file: %v", err)})
		_ = os.Remove(tmpPath)
		return
	}
	if err := os.Rename(tmpPath, absPath); err != nil {
		_ = transfer.WriteHeader(conn, transfer.Header{Op: transfer.OpError, Error: fmt.Sprintf("finalize file: %v", err)})
		_ = os.Remove(tmpPath)
		return
	}

	checksum, err := transfer.ComputeSHA256File(absPath)
	if err != nil {
		_ = transfer.WriteHeader(conn, transfer.Header{Op: transfer.OpError, Error: fmt.Sprintf("checksum file: %v", err)})
		return
	}

	if h.Checksum != "" && h.Checksum != checksum {
		_ = os.Remove(absPath)
		_ = transfer.WriteHeader(conn, transfer.Header{Op: transfer.OpError, Error: "checksum mismatch"})
		return
	}

	if err := transfer.WriteHeader(conn, transfer.Header{Op: transfer.OpAck}); err != nil {
		log.Printf("keeper[%s]: write ack failed: %v", s.ID, err)
		return
	}

	if h.Op == transfer.OpPut {
		s.notifyMasterUploadComplete(ctx, h.UploadID, h.Filename, relativePath, h.Size, checksum, false)
	} else {
		s.notifyMasterUploadComplete(ctx, h.UploadID, h.Filename, relativePath, h.Size, checksum, true)
	}

	log.Printf("keeper[%s]: stored file op=%s file=%s path=%s size=%d", s.ID, h.Op, h.Filename, relativePath, h.Size)
}

func (s *Server) handleGet(conn net.Conn, h transfer.Header) {
	log.Printf("keeper[%s]: download requested file=%s", s.ID, h.Filename)

	relativePath := filepath.Base(h.Filename)
	absPath := filepath.Join(s.StorageDir, relativePath)

	fi, err := os.Stat(absPath)
	if err != nil {
		log.Printf("keeper[%s]: download file not found file=%s", s.ID, h.Filename)
		_ = transfer.WriteHeader(conn, transfer.Header{Op: transfer.OpError, Error: "file not found"})
		return
	}

	checksum, err := transfer.ComputeSHA256File(absPath)
	if err != nil {
		_ = transfer.WriteHeader(conn, transfer.Header{Op: transfer.OpError, Error: fmt.Sprintf("checksum failed: %v", err)})
		return
	}

	if err := transfer.WriteHeader(conn, transfer.Header{Op: transfer.OpData, Filename: h.Filename, Size: fi.Size(), Checksum: checksum}); err != nil {
		log.Printf("keeper[%s]: write data header failed: %v", s.ID, err)
		return
	}

	f, err := os.Open(absPath)
	if err != nil {
		log.Printf("keeper[%s]: open file failed: %v", s.ID, err)
		return
	}
	defer f.Close()

	if sent, err := io.CopyBuffer(conn, f, make([]byte, config.TransferBufferSize)); err != nil {
		log.Printf("keeper[%s]: stream file failed: %v", s.ID, err)
	} else {
		log.Printf("keeper[%s]: download served file=%s bytes=%d", s.ID, h.Filename, sent)
	}
}

func (s *Server) handleStat(conn net.Conn, h transfer.Header) {
	relativePath := filepath.Base(h.Filename)
	absPath := filepath.Join(s.StorageDir, relativePath)

	fi, err := os.Stat(absPath)
	if err != nil {
		_ = transfer.WriteHeader(conn, transfer.Header{Op: transfer.OpError, Error: "file not found"})
		return
	}

	if err := transfer.WriteHeader(conn, transfer.Header{Op: transfer.OpData, Filename: h.Filename, Size: fi.Size()}); err != nil {
		log.Printf("keeper[%s]: write stat header failed: %v", s.ID, err)
	}
}

func (s *Server) notifyMasterUploadComplete(ctx context.Context, uploadID, filename, relativePath string, size int64, checksum string, isReplication bool) {
	rpcCtx, cancel := context.WithTimeout(ctx, config.RPCDeadline)
	defer cancel()

	conn, err := grpc.DialContext(rpcCtx, s.MasterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		log.Printf("keeper[%s]: notify dial master failed: %v", s.ID, err)
		return
	}
	defer conn.Close()

	client := dfs.NewMasterServiceClient(conn)
	resp, err := client.NotifyUploadComplete(rpcCtx, &dfs.NotifyUploadCompleteRequest{
		UploadId:      uploadID,
		KeeperId:      s.ID,
		Filename:      filename,
		RelativePath:  relativePath,
		Size:          size,
		Checksum:      checksum,
		IsReplication: isReplication,
	})
	if err != nil {
		log.Printf("keeper[%s]: notify upload complete failed: %v", s.ID, err)
		return
	}
	if !resp.Ok {
		log.Printf("keeper[%s]: notify upload complete rejected: %s", s.ID, resp.Error)
		return
	}

	log.Printf("keeper[%s]: notify upload complete accepted upload_id=%s file=%s replication=%v", s.ID, uploadID, filename, isReplication)
}

func (s *Server) ReplicateFile(ctx context.Context, req *dfs.ReplicateFileRequest) (*dfs.ReplicateFileResponse, error) {
	if req == nil || req.Filename == "" || req.SourcePath == "" || req.DestinationHost == "" || req.DestinationTcpPort == 0 {
		return &dfs.ReplicateFileResponse{Ok: false, Error: "invalid replication request"}, nil
	}

	log.Printf("keeper[%s]: replication requested file=%s destination=%s:%d", s.ID, req.Filename, req.DestinationHost, req.DestinationTcpPort)

	src := filepath.Join(s.StorageDir, filepath.Base(req.SourcePath))
	f, err := os.Open(src)
	if err != nil {
		return &dfs.ReplicateFileResponse{Ok: false, Error: fmt.Sprintf("open source file: %v", err)}, nil
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return &dfs.ReplicateFileResponse{Ok: false, Error: fmt.Sprintf("stat source file: %v", err)}, nil
	}

	addr := net.JoinHostPort(req.DestinationHost, fmt.Sprintf("%d", req.DestinationTcpPort))
	netConn, err := net.DialTimeout("tcp", addr, config.RPCDeadline)
	if err != nil {
		return &dfs.ReplicateFileResponse{Ok: false, Error: fmt.Sprintf("dial destination: %v", err)}, nil
	}
	defer netConn.Close()

	header := transfer.Header{
		Op:       transfer.OpReplicate,
		UploadID: fmt.Sprintf("repl-%d", time.Now().UnixNano()),
		Filename: req.Filename,
		Size:     fi.Size(),
		Checksum: req.Checksum,
	}
	if err := transfer.WriteHeader(netConn, header); err != nil {
		return &dfs.ReplicateFileResponse{Ok: false, Error: fmt.Sprintf("send header: %v", err)}, nil
	}

	written, err := io.CopyBuffer(netConn, f, make([]byte, config.TransferBufferSize))
	if err != nil {
		return &dfs.ReplicateFileResponse{Ok: false, Error: fmt.Sprintf("stream file: %v", err)}, nil
	}

	if tcpConn, ok := netConn.(*net.TCPConn); ok {
		_ = tcpConn.CloseWrite()
	}

	ackReader := bufio.NewReader(netConn)
	ack, err := transfer.ReadHeader(ackReader)
	if err != nil {
		return &dfs.ReplicateFileResponse{Ok: false, Error: fmt.Sprintf("read destination ack: %v", err)}, nil
	}
	if ack.Op != transfer.OpAck {
		if ack.Error == "" {
			ack.Error = "destination rejected replication"
		}
		return &dfs.ReplicateFileResponse{Ok: false, Error: ack.Error}, nil
	}

	log.Printf("keeper[%s]: replication completed file=%s destination=%s:%d bytes=%d", s.ID, req.Filename, req.DestinationHost, req.DestinationTcpPort, written)

	return &dfs.ReplicateFileResponse{
		Ok:                      true,
		DestinationRelativePath: filepath.Base(req.Filename),
		BytesTransferred:        written,
	}, nil
}

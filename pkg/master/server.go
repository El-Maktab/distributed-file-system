package master

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/El-Maktab/distributed-file-system/internal/config"
	"github.com/El-Maktab/distributed-file-system/internal/transfer"
	"github.com/El-Maktab/distributed-file-system/pkg/api/dfs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type KeeperInfo struct {
	ID       string
	Host     string
	TCPPort  int
	GRPCAddr string
	LastSeen time.Time
	Alive    bool
}

type ReplicaRecord struct {
	KeeperID string
	Path     string
	Size     int64
	Checksum string
}

type UploadSession struct {
	UploadID  string
	Filename  string
	KeeperID  string
	CreatedAt time.Time
	Completed bool
	Err       string
}

type Server struct {
	dfs.UnimplementedMasterServiceServer

	mu      sync.RWMutex
	keepers map[string]*KeeperInfo
	files   map[string][]ReplicaRecord
	uploads map[string]*UploadSession

	rng *rand.Rand
}

var errStaleSourceReplica = errors.New("stale source replica")

func NewServer() *Server {
	return &Server{
		keepers: make(map[string]*KeeperInfo),
		files:   make(map[string][]ReplicaRecord),
		uploads: make(map[string]*UploadSession),
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *Server) Run(ctx context.Context, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen master grpc: %w", err)
	}

	grpcServer := grpc.NewServer()
	dfs.RegisterMasterServiceServer(grpcServer, s)

	go s.heartbeatMonitor(ctx)
	go s.replicationLoop(ctx)

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	log.Printf("master: listening grpc on %s", addr)
	if err := grpcServer.Serve(lis); err != nil {
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		return fmt.Errorf("serve master grpc: %w", err)
	}
	return nil
}

func (s *Server) RequestUpload(_ context.Context, req *dfs.RequestUploadRequest) (*dfs.RequestUploadResponse, error) {
	if req == nil {
		return &dfs.RequestUploadResponse{Ok: false, Error: "empty request"}, nil
	}
	if req.Filename == "" {
		return &dfs.RequestUploadResponse{Ok: false, Error: "filename required"}, nil
	}

	log.Printf("master: upload requested file=%s size=%d", req.Filename, req.Size)

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.files[req.Filename]; exists {
		log.Printf("master: upload rejected file=%s reason=filename already exists", req.Filename)
		return &dfs.RequestUploadResponse{Ok: false, Error: "filename already exists"}, nil
	}

	keeper := s.pickAliveKeeper(nil)
	if keeper == nil {
		log.Printf("master: upload rejected file=%s reason=no alive data keeper", req.Filename)
		return &dfs.RequestUploadResponse{Ok: false, Error: "no alive data keeper"}, nil
	}

	uploadID := fmt.Sprintf("up-%d-%06d", time.Now().UnixNano(), s.rng.Intn(1_000_000))
	s.uploads[uploadID] = &UploadSession{
		UploadID:  uploadID,
		Filename:  req.Filename,
		KeeperID:  keeper.ID,
		CreatedAt: time.Now(),
	}

	log.Printf("master: upload allocated upload_id=%s file=%s keeper=%s keeper_tcp=%d", uploadID, req.Filename, keeper.ID, keeper.TCPPort)

	return &dfs.RequestUploadResponse{
		Ok:            true,
		UploadId:      uploadID,
		KeeperId:      keeper.ID,
		KeeperHost:    keeper.Host,
		KeeperTcpPort: int32(keeper.TCPPort),
	}, nil
}

func (s *Server) Heartbeat(_ context.Context, req *dfs.HeartbeatRequest) (*dfs.SimpleResponse, error) {
	if req == nil || req.KeeperId == "" || req.Host == "" || req.TcpPort == 0 {
		return &dfs.SimpleResponse{Ok: false, Error: "invalid heartbeat"}, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	k, ok := s.keepers[req.KeeperId]
	created := !ok
	wasAlive := false
	if !ok {
		k = &KeeperInfo{ID: req.KeeperId}
		s.keepers[req.KeeperId] = k
	} else {
		wasAlive = k.Alive
	}
	k.Host = req.Host
	k.TCPPort = int(req.TcpPort)
	k.GRPCAddr = req.GrpcAddr
	k.LastSeen = now
	k.Alive = true

	if created {
		log.Printf("master: keeper registered keeper=%s grpc=%s tcp=%d host=%s", k.ID, k.GRPCAddr, k.TCPPort, k.Host)
	} else if !wasAlive {
		log.Printf("master: keeper revived keeper=%s grpc=%s tcp=%d host=%s", k.ID, k.GRPCAddr, k.TCPPort, k.Host)
	}

	return &dfs.SimpleResponse{Ok: true}, nil
}

func (s *Server) NotifyUploadComplete(_ context.Context, req *dfs.NotifyUploadCompleteRequest) (*dfs.SimpleResponse, error) {
	if req == nil || req.UploadId == "" || req.KeeperId == "" || req.Filename == "" || req.RelativePath == "" {
		return &dfs.SimpleResponse{Ok: false, Error: "invalid notify request"}, nil
	}

	s.mu.Lock()
	upload, ok := s.uploads[req.UploadId]
	if !ok {
		s.mu.Unlock()
		return &dfs.SimpleResponse{Ok: false, Error: "unknown upload id"}, nil
	}

	upload.Completed = true
	upload.Err = ""

	record := ReplicaRecord{
		KeeperID: req.KeeperId,
		Path:     req.RelativePath,
		Size:     req.Size,
		Checksum: req.Checksum,
	}
	s.upsertReplicaLocked(req.Filename, record)
	s.mu.Unlock()

	log.Printf("master: upload complete upload_id=%s file=%s keeper=%s replication=%v", req.UploadId, req.Filename, req.KeeperId, req.IsReplication)
	return &dfs.SimpleResponse{Ok: true}, nil
}

func (s *Server) GetUploadStatus(_ context.Context, req *dfs.GetUploadStatusRequest) (*dfs.GetUploadStatusResponse, error) {
	if req == nil || req.UploadId == "" {
		return &dfs.GetUploadStatusResponse{Found: false, Completed: false, Error: "upload_id required"}, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	upload, ok := s.uploads[req.UploadId]
	if !ok {
		return &dfs.GetUploadStatusResponse{Found: false}, nil
	}

	return &dfs.GetUploadStatusResponse{Found: true, Completed: upload.Completed, Error: upload.Err}, nil
}

func (s *Server) GetDownloadTargets(_ context.Context, req *dfs.DownloadTargetsRequest) (*dfs.DownloadTargetsResponse, error) {
	if req == nil || req.Filename == "" {
		return &dfs.DownloadTargetsResponse{Ok: false, Error: "filename required"}, nil
	}

	s.pruneStaleReplicas(req.Filename)

	s.mu.RLock()
	replicas := append([]ReplicaRecord(nil), s.files[req.Filename]...)
	targets := make([]*dfs.DownloadTarget, 0, len(replicas))
	for _, r := range replicas {
		k, ok := s.keepers[r.KeeperID]
		if !ok || !k.Alive {
			continue
		}
		targets = append(targets, &dfs.DownloadTarget{KeeperId: k.ID, Host: k.Host, TcpPort: int32(k.TCPPort)})
	}
	s.mu.RUnlock()

	if len(targets) == 0 {
		log.Printf("master: download targets not found file=%s reason=no alive replicas", req.Filename)
		return &dfs.DownloadTargetsResponse{Ok: false, Error: "no alive replicas for file"}, nil
	}

	sort.Slice(targets, func(i, j int) bool { return targets[i].KeeperId < targets[j].KeeperId })
	log.Printf("master: download targets file=%s count=%d", req.Filename, len(targets))
	return &dfs.DownloadTargetsResponse{Ok: true, Targets: targets}, nil
}

func (s *Server) heartbeatMonitor(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			s.mu.Lock()
			for _, k := range s.keepers {
				if now.Sub(k.LastSeen) > config.HeartbeatTimeout {
					if k.Alive {
						k.Alive = false
						log.Printf("master: keeper timed out keeper=%s timeout=%s", k.ID, config.HeartbeatTimeout)
					}
				}
			}
			s.mu.Unlock()
		}
	}
}

func (s *Server) replicationLoop(ctx context.Context) {
	ticker := time.NewTicker(config.ReplicationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reconcileReplication(ctx)
		}
	}
}

func (s *Server) reconcileReplication(ctx context.Context) {
	s.mu.RLock()
	filenames := make([]string, 0, len(s.files))
	for fn := range s.files {
		filenames = append(filenames, fn)
	}
	s.mu.RUnlock()

	if len(filenames) > 0 {
		log.Printf("master: replication reconcile start files=%d target_replicas=%d", len(filenames), config.ReplicationFactor)
	}

	for _, filename := range filenames {
		s.pruneStaleReplicas(filename)

		for {
			source, dest, ok := s.chooseReplicationPair(filename)
			if !ok {
				break
			}
			if err := s.triggerReplication(ctx, filename, source, dest); err != nil {
				if errors.Is(err, errStaleSourceReplica) {
					continue
				}
				log.Printf("master: replication failed file=%s source=%s dest=%s err=%v", filename, source.KeeperID, dest.ID, err)
				break
			}
		}
	}
}

func (s *Server) chooseReplicationPair(filename string) (ReplicaRecord, *KeeperInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	replicas := s.files[filename]
	aliveReplicas := make([]ReplicaRecord, 0, len(replicas))
	already := make(map[string]struct{}, len(replicas))
	for _, r := range replicas {
		already[r.KeeperID] = struct{}{}
		k, ok := s.keepers[r.KeeperID]
		if ok && k.Alive {
			aliveReplicas = append(aliveReplicas, r)
		}
	}

	if len(aliveReplicas) >= config.ReplicationFactor || len(aliveReplicas) == 0 {
		if len(aliveReplicas) == 0 {
			log.Printf("master: replication blocked file=%s reason=no alive source replica", filename)
		}
		return ReplicaRecord{}, nil, false
	}

	source := aliveReplicas[0]
	var destinations []*KeeperInfo
	for _, k := range s.keepers {
		if !k.Alive {
			continue
		}
		if _, exists := already[k.ID]; exists {
			continue
		}
		destinations = append(destinations, k)
	}
	if len(destinations) == 0 {
		log.Printf("master: replication blocked file=%s alive_replicas=%d need=%d reason=no eligible alive destination keeper", filename, len(aliveReplicas), config.ReplicationFactor)
		return ReplicaRecord{}, nil, false
	}

	dest := destinations[s.rng.Intn(len(destinations))]
	return source, dest, true
}

func (s *Server) triggerReplication(ctx context.Context, filename string, source ReplicaRecord, dest *KeeperInfo) error {
	s.mu.RLock()
	sourceKeeper, ok := s.keepers[source.KeeperID]
	s.mu.RUnlock()
	if !ok || !sourceKeeper.Alive || sourceKeeper.GRPCAddr == "" {
		return fmt.Errorf("source keeper unavailable")
	}

	log.Printf("master: triggering replication file=%s source=%s dest=%s", filename, source.KeeperID, dest.ID)

	rpcCtx, cancel := context.WithTimeout(ctx, config.RPCDeadline)
	defer cancel()

	conn, err := grpc.DialContext(rpcCtx, sourceKeeper.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("dial source keeper: %w", err)
	}
	defer conn.Close()

	client := dfs.NewDataKeeperServiceClient(conn)
	resp, err := client.ReplicateFile(rpcCtx, &dfs.ReplicateFileRequest{
		Filename:            filename,
		SourcePath:          source.Path,
		DestinationKeeperId: dest.ID,
		DestinationHost:     dest.Host,
		DestinationTcpPort:  int32(dest.TCPPort),
		Size:                source.Size,
		Checksum:            source.Checksum,
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		if isMissingSourceError(resp.Error) {
			s.mu.Lock()
			removed := s.removeReplicaLocked(filename, source.KeeperID)
			s.mu.Unlock()
			if removed {
				log.Printf("master: pruned stale source replica file=%s keeper=%s reason=%s", filename, source.KeeperID, resp.Error)
			}
			return errStaleSourceReplica
		}
		return fmt.Errorf(resp.Error)
	}

	// Source keeper notifies completion in destination through master API too, but ensure state catches up.
	s.mu.Lock()
	s.upsertReplicaLocked(filename, ReplicaRecord{
		KeeperID: dest.ID,
		Path:     resp.DestinationRelativePath,
		Size:     source.Size,
		Checksum: source.Checksum,
	})
	s.mu.Unlock()

	log.Printf("master: replicated file=%s source=%s dest=%s bytes=%d", filename, source.KeeperID, dest.ID, resp.BytesTransferred)
	return nil
}

func (s *Server) pruneStaleReplicas(filename string) {
	s.mu.RLock()
	replicas := append([]ReplicaRecord(nil), s.files[filename]...)
	aliveKeepers := make(map[string]KeeperInfo, len(replicas))
	for _, r := range replicas {
		k, ok := s.keepers[r.KeeperID]
		if !ok || !k.Alive {
			continue
		}
		aliveKeepers[r.KeeperID] = *k
	}
	s.mu.RUnlock()

	if len(replicas) == 0 {
		return
	}

	staleByKeeper := make(map[string]struct{})
	for _, r := range replicas {
		k, ok := aliveKeepers[r.KeeperID]
		if !ok {
			continue
		}

		probePath := r.Path
		if probePath == "" {
			probePath = filename
		}

		exists, err := s.probeReplicaExists(k.Host, k.TCPPort, probePath)
		if err != nil {
			log.Printf("master: replica probe failed file=%s keeper=%s err=%v", filename, r.KeeperID, err)
			continue
		}
		if !exists {
			staleByKeeper[r.KeeperID] = struct{}{}
		}
	}

	if len(staleByKeeper) == 0 {
		return
	}

	s.mu.Lock()
	removedCount := 0
	for keeperID := range staleByKeeper {
		if s.removeReplicaLocked(filename, keeperID) {
			removedCount++
			log.Printf("master: pruned stale replica file=%s keeper=%s", filename, keeperID)
		}
	}
	s.mu.Unlock()

	if removedCount > 0 {
		log.Printf("master: stale replica prune summary file=%s removed=%d", filename, removedCount)
	}
}

func (s *Server) probeReplicaExists(host string, tcpPort int, relativePath string) (bool, error) {
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", tcpPort))
	conn, err := net.DialTimeout("tcp", addr, config.RPCDeadline)
	if err != nil {
		return false, fmt.Errorf("dial keeper tcp: %w", err)
	}
	defer conn.Close()

	if err := transfer.WriteHeader(conn, transfer.Header{Op: transfer.OpStat, Filename: relativePath}); err != nil {
		return false, fmt.Errorf("write stat request: %w", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(config.RPCDeadline))
	reader := bufio.NewReader(conn)
	h, err := transfer.ReadHeader(reader)
	if err != nil {
		return false, fmt.Errorf("read stat response: %w", err)
	}

	switch h.Op {
	case transfer.OpData:
		return true, nil
	case transfer.OpError:
		if strings.Contains(strings.ToLower(h.Error), "file not found") {
			return false, nil
		}
		return false, fmt.Errorf("stat error: %s", h.Error)
	default:
		return false, fmt.Errorf("unexpected stat op: %s", h.Op)
	}
}

func (s *Server) removeReplicaLocked(filename, keeperID string) bool {
	replicas := s.files[filename]
	filtered := make([]ReplicaRecord, 0, len(replicas))
	removed := false
	for _, r := range replicas {
		if r.KeeperID == keeperID {
			removed = true
			continue
		}
		filtered = append(filtered, r)
	}
	if !removed {
		return false
	}
	s.files[filename] = filtered
	return true
}

func isMissingSourceError(msg string) bool {
	lower := strings.ToLower(msg)
	if !strings.Contains(lower, "open source file") {
		return false
	}
	return strings.Contains(lower, "no such file") || strings.Contains(lower, "not found") || strings.Contains(lower, "cannot find")
}

func (s *Server) upsertReplicaLocked(filename string, rec ReplicaRecord) {
	replicas := s.files[filename]
	for i := range replicas {
		if replicas[i].KeeperID == rec.KeeperID {
			replicas[i] = rec
			s.files[filename] = replicas
			return
		}
	}
	s.files[filename] = append(replicas, rec)
}

func (s *Server) pickAliveKeeper(exclude map[string]struct{}) *KeeperInfo {
	var alive []*KeeperInfo
	for _, k := range s.keepers {
		if !k.Alive {
			continue
		}
		if exclude != nil {
			if _, exists := exclude[k.ID]; exists {
				continue
			}
		}
		alive = append(alive, k)
	}
	if len(alive) == 0 {
		return nil
	}
	return alive[s.rng.Intn(len(alive))]
}

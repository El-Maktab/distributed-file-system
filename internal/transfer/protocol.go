package transfer

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	OpPut       = "PUT"
	OpGet       = "GET"
	OpStat      = "STAT"
	OpReplicate = "REPL"
	OpData      = "DATA"
	OpAck       = "ACK"
	OpError     = "ERROR"
)

type Header struct {
	Op       string `json:"op"`
	UploadID string `json:"upload_id,omitempty"`
	Filename string `json:"filename,omitempty"`
	Size     int64  `json:"size,omitempty"`
	Checksum string `json:"checksum,omitempty"`
	Error    string `json:"error,omitempty"`
}

func WriteHeader(w io.Writer, h Header) error {
	encoded, err := json.Marshal(h)
	if err != nil {
		return fmt.Errorf("marshal header: %w", err)
	}
	encoded = append(encoded, '\n')
	if _, err := w.Write(encoded); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	return nil
}

func ReadHeader(r *bufio.Reader) (Header, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return Header{}, fmt.Errorf("read header: %w", err)
	}
	var h Header
	if err := json.Unmarshal(line, &h); err != nil {
		return Header{}, fmt.Errorf("unmarshal header: %w", err)
	}
	return h, nil
}

func IsMP4(name string) bool {
	return strings.EqualFold(filepath.Ext(name), ".mp4")
}

func ComputeSHA256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	return ComputeSHA256Reader(f)
}

func ComputeSHA256Reader(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

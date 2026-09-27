package coap

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// stateFileName 为状态快照文件名。
const stateFileName = "coap-state.json"

// stateSnapshot 为崩溃恢复用的可序列化状态：
// 去重记录 + 观察订阅。
type stateSnapshot struct {
	Dedup   []dedupRecord   `json:"dedup"`
	Observe []observeRecord `json:"observe"`
}

// observeRecord 为一条观察订阅的持久化记录。
type observeRecord struct {
	Addr  string `json:"addr"`
	Token []byte `json:"token"`
	Path  string `json:"path"`
	Seq   uint32 `json:"seq"`
}

// saveState 将快照原子写入目录（写临时文件后 rename），
// 保证崩溃时不会留下半截文件。
func saveState(dir string, snap *stateSnapshot) error {
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".coap-state-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, filepath.Join(dir, stateFileName))
}

// loadState 读取快照；文件不存在时返回空快照而非错误，
// 内容损坏时返回错误（调用方应视为无状态启动）。
func loadState(dir string) (*stateSnapshot, error) {
	snap := &stateSnapshot{}
	if dir == "" {
		return snap, nil
	}
	data, err := os.ReadFile(filepath.Join(dir, stateFileName))
	if errors.Is(err, os.ErrNotExist) {
		return snap, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, snap); err != nil {
		return nil, err
	}
	return snap, nil
}

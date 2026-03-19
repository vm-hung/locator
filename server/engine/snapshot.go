package engine

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/hunkvm/locator/server/transport"
	"go.uber.org/zap"

	"go.etcd.io/raft/v3/raftpb"
)

const snapshotPrefix = "snapshot-"

func (e *LocatorEngine) saveSnapshotMessage(msg transport.SnapshotMessage) {
	snapIndex := msg.Message.Snapshot.Metadata.Index
	name := fmt.Sprintf("%s%d", snapshotPrefix, snapIndex)
	dir := filepath.Join(e.dataDirectory, name)

	if err := os.MkdirAll(dir, 0755); err != nil {
		e.logger.Fatal("failed to create temp dir", zap.Error(err))
	}

	tarReader := tar.NewReader(msg.ReadCloser)
	for {
		header, err := tarReader.Next()

		if err == io.EOF {
			break
		}
		if err != nil {
			e.logger.Error("failed to read tar", zap.Error(err))
			msg.CloseWithError(err)
			return
		}

		path := filepath.Join(dir, header.Name)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(path, os.FileMode(header.Mode)); err != nil {
				e.logger.Fatal("failed to create dir", zap.Error(err))
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
				e.logger.Fatal("failed to create dir", zap.Error(err))
			}
			flag := os.O_CREATE | os.O_RDWR
			perm := os.FileMode(header.Mode)
			file, err := os.OpenFile(path, flag, perm)
			if err != nil {
				e.logger.Fatal("failed to open file", zap.Error(err))
			}
			if _, err := io.Copy(file, tarReader); err != nil {
				file.Close()
				e.logger.Fatal("failed to copy file", zap.Error(err))
			}
			file.Close()
		}
	}

	msg.CloseWithError(nil)
}

func (e *LocatorEngine) createSnapshotMessage(
	msg raftpb.Message, pg *LocatorProgress,
) transport.SnapshotMessage {
	pipeReader, pipeWriter := io.Pipe()

	msg.Snapshot = &raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     pg.appliedIndex,
			Term:      pg.appliedTerm,
			ConfState: pg.configState,
		},
		Data: nil,
	}

	go func() {
		name := fmt.Sprintf("%s%d", snapshotPrefix, pg.appliedIndex)
		dir := filepath.Join(e.dataDirectory, name)

		if err := e.storage.Checkpoint(dir); err != nil {
			e.logger.Fatal("failed to create checkpoint", zap.Error(err))
		}

		tarWriter := tar.NewWriter(pipeWriter)
		err := filepath.Walk(dir, func(
			path string, info os.FileInfo, err error,
		) error {
			if err != nil {
				return err
			}

			header, err := tar.FileInfoHeader(info, path)
			if err != nil {
				return err
			}

			relPath, err := filepath.Rel(dir, path)
			if err != nil {
				return err
			}
			header.Name = relPath

			if err := tarWriter.WriteHeader(header); err != nil {
				return err
			}

			if !info.IsDir() {
				file, err := os.Open(path)
				if err != nil {
					return err
				}
				defer file.Close()
				if _, err := io.Copy(tarWriter, file); err != nil {
					return err
				}
			}
			return nil
		})
		tarWriter.Close()
		pipeWriter.CloseWithError(err)
		os.RemoveAll(dir)
	}()

	return *transport.NewSnapshotMessage(msg, pipeReader)
}

func (e *LocatorEngine) sendSnapshotMessage(msg transport.SnapshotMessage) {
	e.inflightSnapshots.Add(1)

	e.raftNode.transport.SendSnapshot(msg)
	e.logger.Info("sending snapshot data")

	go func(m transport.SnapshotMessage) {
		select {
		case ok := <-m.CloseNotify():
			if ok {
				select {
				case <-time.After(delayAfterSnapshot):
				case <-e.stopping:
				}
			}

			e.inflightSnapshots.Add(-1)
			e.logger.Info("snapshot data sent")
		case <-e.stopping:
			e.logger.Info("canceled sending snapshot")
			return
		}
	}(msg)
}

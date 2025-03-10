package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	MaxSegmentSize = 10 * 1024 * 1024 // 10MB
	LogEntryHeader = 8                // 4 bytes CRC + 4 bytes length
)

type WAL struct {
	dir             string
	currentSeg      *os.File
	currentSegIndex int
	currentSegSize  int64
}

type WALMetadata struct {
	LastSegment int `json:"last_segment"`
}

func writeWALMetadata(walDir string, segment int) error {
	metaFilePath := filepath.Join(walDir, "wal_metadata.json")
	meta := WALMetadata{LastSegment: segment}

	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	return os.WriteFile(metaFilePath, data, 0644)
}

func readWALMetadata(walDir string) (int, error) {
	metaFilePath := filepath.Join(walDir, "wal_metadata.json")

	data, err := os.ReadFile(metaFilePath)
	if err != nil {
		// If file doesn't exist, assume this is a fresh WAL and start from segment 0
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	var meta WALMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return 0, err
	}

	return meta.LastSegment, nil
}

func NewWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	wal := &WAL{dir: dir, currentSegIndex: 1}
	if err := wal.openSegment(); err != nil {
		return nil, err
	}
	writeWALMetadata(wal.dir, wal.currentSegIndex)
	return wal, nil
}

func LoadPrevWAL(dir string) (*WAL, error) {
	// Ensure WAL directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// Read last segment number from metadata
	lastSegment, err := readWALMetadata(dir)
	if err != nil {
		fmt.Println("Error reading WAL metadata:", err)
		return nil, err
	}

	// Construct WAL with correct segment index
	wal := &WAL{dir: dir, currentSegIndex: lastSegment}

	// Determine segment file path
	segmentFile := filepath.Join(dir, fmt.Sprintf("wal_%d.log", lastSegment))

	// Calculate current segment size
	fileInfo, err := os.Stat(segmentFile)
	if err == nil {
		wal.currentSegSize = fileInfo.Size() // Update WAL with segment size
	} else if os.IsNotExist(err) {
		// If segment file doesn't exist, start fresh
		wal.currentSegSize = 0
	} else {
		return nil, err
	}

	// Open segment for writing
	if err := wal.openSegment(); err != nil {
		return nil, err
	}

	return wal, nil
}

func (w *WAL) openSegment() error {
	filename := filepath.Join(w.dir, fmt.Sprintf("wal_%d.log", w.currentSegIndex))
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	w.currentSeg = file
	w.currentSegSize = 0
	writeWALMetadata(w.dir, w.currentSegIndex)
	return nil
}

func (w *WAL) rotateSegment() error {
	w.currentSegIndex++
	w.currentSeg.Close()
	return w.openSegment()
}

func (w *WAL) Write(entry []byte) error {
	if w.currentSegSize+int64(len(entry))+LogEntryHeader > MaxSegmentSize {
		if err := w.rotateSegment(); err != nil {
			return err
		}
	}

	crc := crc32.ChecksumIEEE(entry)
	buf := make([]byte, LogEntryHeader+len(entry))
	//Since Go runs on different CPU architectures (which may use different endianness), we explicitly use LittleEndian to ensure consistency across all platforms.
	binary.LittleEndian.PutUint32(buf[0:4], crc)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(entry)))
	copy(buf[8:], entry)

	n, err := w.currentSeg.Write(buf)
	if err != nil {
		return err
	}
	w.currentSegSize += int64(n)
	return nil
}

func (w *WAL) ReadAll() ([][]byte, error) {
	var entries [][]byte
	files, err := filepath.Glob(filepath.Join(w.dir, "wal_*.log"))
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			return nil, err
		}
		defer f.Close()

		for {
			header := make([]byte, LogEntryHeader)
			_, err := io.ReadFull(f, header)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, err
			}

			crc := binary.LittleEndian.Uint32(header[0:4])
			length := binary.LittleEndian.Uint32(header[4:8])

			data := make([]byte, length)
			_, err = io.ReadFull(f, data)
			if err != nil {
				return nil, err
			}

			if crc32.ChecksumIEEE(data) != crc {
				return nil, errors.New("CRC mismatch, corrupted entry")
			}

			entries = append(entries, data)
		}
	}
	return entries, nil
}

func (w *WAL) Close() error {
	return w.currentSeg.Close()
}

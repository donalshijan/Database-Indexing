package hashindex

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

type HashIndex struct {
	dataFile  string
	indexFile string
	index     map[string]int64 // map from key to position in dataFile
	mu        sync.RWMutex
}

func NewHashIndex(dataFilename, indexFilename string) *HashIndex {
	hi := &HashIndex{
		dataFile:  dataFilename,
		indexFile: indexFilename,
		index:     make(map[string]int64),
	}
	hi.loadIndex()
	return hi
}

func (hi *HashIndex) loadIndex() {
	hi.mu.Lock()
	defer hi.mu.Unlock()

	file, err := os.Open(hi.indexFile)
	if err != nil {
		if !os.IsNotExist(err) {
			panic(err)
		}
		return // Index file does not exist, no data to load
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue // Skip invalid lines
		}
		key := parts[0]
		offset, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			continue // Skip invalid offsets
		}
		hi.index[key] = offset
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
}

func (hi *HashIndex) Insert(key, value string) {
	hi.mu.Lock()
	defer hi.mu.Unlock()

	// Open the data file in append mode
	file, err := os.OpenFile(hi.dataFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Write the value to the data file
	offset, err := file.Seek(0, io.SeekEnd) // Get current end position
	if err != nil {
		panic(err)
	}
	_, err = file.WriteString(value + "\n")
	if err != nil {
		panic(err)
	}

	// Update the index with the new offset
	hi.index[key] = offset

	// Write the updated index to the index file
	indexFile, err := os.OpenFile(hi.indexFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer indexFile.Close()

	indexEntry := fmt.Sprintf("%s:%d\n", key, offset)
	_, err = indexFile.WriteString(indexEntry)
	if err != nil {
		panic(err)
	}
}

func (hi *HashIndex) Search(key string) (string, bool) {
	hi.mu.RLock()
	defer hi.mu.RUnlock()

	offset, ok := hi.index[key]
	if !ok {
		return "", false
	}

	file, err := os.Open(hi.dataFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	file.Seek(offset, io.SeekStart) // Seek to the position of the record
	reader := bufio.NewReader(file)
	value, err := reader.ReadString('\n')
	if err != nil {
		return "", false
	}

	return strings.TrimSpace(value), true
}

func (hi *HashIndex) Delete(key string) {
	hi.mu.Lock()
	defer hi.mu.Unlock()

	// Check if the key exists in the index map
	offset, exists := hi.index[key]
	if !exists {
		// Key does not exist
		return
	}

	// Open the data file for reading
	file, err := os.Open(hi.dataFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Create a temporary file for writing
	tempFile, err := os.Create(hi.dataFile + ".tmp")
	if err != nil {
		panic(err)
	}
	defer tempFile.Close()

	// Create a reader and writer
	reader := bufio.NewReader(file)
	writer := bufio.NewWriter(tempFile)

	// Skip over the entry at the specified offset
	currentOffset := int64(0)
	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			panic(err)
		}
		if err == io.EOF {
			break
		}

		if currentOffset == offset {
			// Skip this entry
			currentOffset += int64(len(line))
			continue
		}

		// Write to the temporary file
		writer.WriteString(line)
		currentOffset += int64(len(line))
	}

	// Flush the writer to ensure all data is written
	writer.Flush()

	// Update the index map to remove the entry for the deleted key
	hi.removeKeyFromIndexFile(key)

	// Replace the original file with the temporary file
	err = os.Rename(hi.dataFile+".tmp", hi.dataFile)
	if err != nil {
		panic(err)
	}
}

func (hi *HashIndex) removeKeyFromIndexFile(key string) error {
	// Open the index file for reading
	file, err := os.Open(hi.indexFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create a temporary file for writing
	tempFile, err := os.Create(hi.indexFile + ".tmp")
	if err != nil {
		return err
	}
	defer tempFile.Close()

	scanner := bufio.NewScanner(file)
	writer := bufio.NewWriter(tempFile)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		if parts[0] != key {
			writer.WriteString(line + "\n")
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	delete(hi.index, key)

	return writer.Flush()
}

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

const MaxMemoryUsage = 1024 * 1024 // 1MB in bytes

func (hi *HashIndex) GetMaxMemoryUsage() int64 {
	return MaxMemoryUsage
}

type HashIndex struct {
	dataFile           string
	indexFile          string
	index              map[string]int64 // map from key to position in dataFile
	mu                 sync.RWMutex
	currentMemoryUsage int
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

func (hi *HashIndex) StopIndexing() {
	hi.mu.Lock()
	defer hi.mu.Unlock()

	// Persist the index to disk (if needed)
	// err := hi.saveIndex()
	// if err != nil {
	// 	fmt.Println("Warning: Failed to save index:", err)
	// } else {
	// 	fmt.Println("Index file saved to disk successfully.")
	// }
	// Clear the index map to free memory
	hi.index = nil

	fmt.Println("HashIndex cleared and removed from memory.")
}

// saveIndex persists the in-memory index to the index file
func (hi *HashIndex) saveIndex() error {
	file, err := os.Create(hi.indexFile) // Overwrite the existing file
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for key, offset := range hi.index {
		_, err := writer.WriteString(fmt.Sprintf("%s:%d\n", key, offset))
		if err != nil {
			return err
		}
	}

	return writer.Flush() // Ensure everything is written to disk
}

// Estimate memory usage of a key-value pair
func estimateEntrySize(key string) int {
	return len(key) + 8 + 16 // key size + int64 (8 bytes) + map overhead (16 bytes approx)
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

func (hi *HashIndex) ClearIndex() {
	hi.index = make(map[string]int64)
}

func (hi *HashIndex) Insert(key, value string) string {
	hi.mu.Lock()
	defer hi.mu.Unlock()

	// Check if the key already exists in the index
	if _, exists := hi.index[key]; exists {
		return fmt.Sprintf("Failed: Key '%s' already exists", key)
	}

	// Check if adding this entry exceeds the memory limit
	entrySize := estimateEntrySize(key)
	if hi.currentMemoryUsage+entrySize > MaxMemoryUsage {
		return "Failed: Memory limit exceeded"
	}

	// Open the data file in append mode
	file, err := os.OpenFile(hi.dataFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Sprintf("Failed: Could not open data file: %v", err)
	}
	defer file.Close()

	// Write the value to the data file
	offset, err := file.Seek(0, io.SeekEnd) // Get current end position
	if err != nil {
		return fmt.Sprintf("Failed: Could not seek to end of file: %v", err)
	}
	_, err = file.WriteString(value + "\n")
	if err != nil {
		return fmt.Sprintf("Failed: Could not write value to data file: %v", err)
	}

	// Update the index with the new offset
	hi.index[key] = offset

	hi.currentMemoryUsage += entrySize // Update memory usage

	// Write the updated index to the index file
	indexFile, err := os.OpenFile(hi.indexFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Sprintf("Failed: Could not open index file: %v", err)
	}
	defer indexFile.Close()

	indexEntry := fmt.Sprintf("%s:%d\n", key, offset)
	_, err = indexFile.WriteString(indexEntry)
	if err != nil {
		return fmt.Sprintf("Failed: Could not write to index file: %v", err)
	}

	return fmt.Sprintf("Success: Key '%s' inserted", key)
}

func (hi *HashIndex) Update(key, value string) string {
	// First, attempt to delete the key
	deleteResult := hi.Delete(key)
	if !strings.HasPrefix(deleteResult, "Success") {
		// If deletion fails, return the failure message
		return fmt.Sprintf("Failed: %s", deleteResult)
	}

	// If deletion succeeds, attempt to insert the new value
	insertResult := hi.Insert(key, value)
	if !strings.HasPrefix(insertResult, "Success") {
		// If insertion fails, return the failure message
		return fmt.Sprintf("Failed: %s", insertResult)
	}

	// If both delete and insert succeeded, return success
	return fmt.Sprintf("Success: Key '%s' updated with new value", key)
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

func (hi *HashIndex) Delete(key string) string {
	hi.mu.Lock()
	defer hi.mu.Unlock()

	// Check if the key exists in the index map
	offset, exists := hi.index[key]
	if !exists {
		return fmt.Sprintf("Failed: Key '%s' does not exist", key)
	}

	// Open the data file for reading
	file, err := os.Open(hi.dataFile)
	if err != nil {
		return fmt.Sprintf("Failed: Could not open data file: %v", err)
	}
	defer file.Close()

	// Create a temporary file for writing
	tempFile, err := os.Create(hi.dataFile + ".tmp")
	if err != nil {
		return fmt.Sprintf("Failed: Could not create temporary file: %v", err)
	}
	defer tempFile.Close()

	// Create a reader and writer
	reader := bufio.NewReader(file)
	writer := bufio.NewWriter(tempFile)

	// Skip over the entry at the specified offset
	currentOffset := int64(0)
	removedSize := 0 // Track the size of the deleted entry
	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return fmt.Sprintf("Failed: Error reading data file: %v", err)
		}
		if err == io.EOF {
			break
		}

		if currentOffset == offset {
			// Skip this entry (mark it as deleted)
			removedSize = len(line)
			currentOffset += int64(len(line))
			continue
		}

		// Write to the temporary file
		writer.WriteString(line)
		currentOffset += int64(len(line))
	}

	// Flush the writer to ensure all data is written
	writer.Flush()

	// Remove the key from the index file
	err = hi.removeKeyFromIndexFile(key)
	if err != nil {
		return fmt.Sprintf("Failed: Could not update index file: %v", err)
	}

	// Replace the original data file with the temporary file
	err = os.Rename(hi.dataFile+".tmp", hi.dataFile)
	if err != nil {
		return fmt.Sprintf("Failed: Could not replace data file: %v", err)
	}
	// Update offsets in memory
	for k, v := range hi.index {
		if v > offset {
			hi.index[k] = v - int64(removedSize)
		}
	}
	return fmt.Sprintf("Success: Key '%s' deleted", key)
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

	// Track the size of the removed entry
	removedOffset := int64(0)
	removedSize := 0

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		currentKey := parts[0]
		offset, _ := strconv.ParseInt(parts[1], 10, 64)

		if currentKey == key {
			// Track the size of the removed entry
			removedOffset = offset
			removedSize = len(line) + 1 // Include the newline
			continue
		}

		// Adjust the offset for subsequent entries
		if offset > removedOffset {
			offset -= int64(removedSize)
		}

		// Write updated entry to the temp file
		writer.WriteString(fmt.Sprintf("%s:%d\n", currentKey, offset))
	}

	writer.Flush()
	delete(hi.index, key)

	// Confirm key deletion
	if _, exists := hi.index[key]; !exists {
		entrySize := estimateEntrySize(key)
		hi.currentMemoryUsage -= entrySize
	} else {
		fmt.Printf("Warning: Attempted to delete key '%s', but it's still in the index\n", key)
	}
	// Replace the original file with the temporary file
	err = os.Rename(hi.indexFile+".tmp", hi.indexFile)
	if err != nil {
		return err
	}
	return nil
}

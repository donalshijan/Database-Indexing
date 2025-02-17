package btreeindex

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Define the B-tree structure
type BTreeNode struct {
	Entries  []IndexEntry
	Children []ChildPointer
	IsLeaf   bool
	Parent   *BTreeNode
}

type ChildPointer struct {
	IndexFile string     // Path to child index file (for internal nodes)
	ChildNode *BTreeNode // In-memory reference (nil when loaded from disk)
}

// Define the B-tree structure
type BTree struct {
	Root               *BTreeNode
	currentMemoryUsage int
	maxEntries         int
}
type IndexEntry struct {
	Key        string
	FileOffset int64 // Offset in the file for this node
}
type BTreeIndex struct {
	dataFile              string
	encodedBTreeIndexFile string
	btree                 *BTree
	mu                    sync.RWMutex
}

var indexFiles []string

const MaxMemoryUsage = 1024 * 1024 // 1MB in bytes

func (bti *BTreeIndex) GetMaxMemoryUsage() int64 {
	return MaxMemoryUsage
}

func NewBTreeIndex(dataFilename, encodedBTreeIndexFileFilename string) *BTreeIndex {
	btree, err := NewBTree() // Call NewBTree to get the BTree
	if err != nil {
		panic(fmt.Sprintf("Failed to create BTree: %v", err)) // Handle error (or return it)
	}

	btreeIndex := &BTreeIndex{
		dataFile:              dataFilename,
		encodedBTreeIndexFile: encodedBTreeIndexFileFilename,
		btree:                 btree, // Assign the BTree to the index field
	}
	btreeIndex.loadIndex()
	return btreeIndex
}

func (btreeIndex *BTreeIndex) loadIndex() {
	btreeIndex.mu.Lock()
	defer btreeIndex.mu.Unlock()

	decodedBtree, err := btreeIndex.DecodeAndReconstructBTreeIndexFromEncodedBTreeIndexFile(btreeIndex.encodedBTreeIndexFile)
	if err != nil { // Check if err is NOT nil
		fmt.Println(err) // Print the error
	} else {
		btreeIndex.btree = decodedBtree
	}
}

// NewBTree creates a new B-tree with an empty root
func NewBTree() (*BTree, error) {
	return &BTree{
		Root:               nil, // The root is initially set to nil
		currentMemoryUsage: 0,
		maxEntries:         5,
	}, nil
}

func (bti *BTreeIndex) ClearIndex() {
	bti.mu.Lock()
	defer bti.mu.Unlock()

	// Delete all index files from disk
	for _, indexFile := range indexFiles {
		err := os.Remove(indexFile)
		if err != nil {
			fmt.Printf("Warning: Failed to delete index file %s: %v\n", indexFile, err)
		} else {
			// fmt.Printf("Deleted index file: %s\n", indexFile)
		}
	}

	// Clear the index files list
	indexFiles = []string{}

	// Reset the B-tree structure
	bti.btree.Root = nil
	bti.btree.currentMemoryUsage = 0

	fmt.Println("B-tree index cleared and index files removed successfully.")
}

func (bti *BTreeIndex) StopIndexing() {
	bti.mu.Lock()
	defer bti.mu.Unlock()

	// Persist the index to disk (if needed)
	err := bti.EncodeBTreeIndexToFile()
	if err != nil {
		fmt.Println("Warning: Failed to save index:", err)
	} else {
		fmt.Println("BTree Index has been encoded to string and saved to file on disk successfully.")
	}
	// Clear the B-tree index from memory
	bti.btree.Root = nil
	bti.btree = nil

	// Delete all index files from disk
	for _, filename := range indexFiles {
		err := os.Remove(filename)
		if err != nil {
			fmt.Printf("Warning: Failed to delete index file %s: %v\n", filename, err)
		} else {
			fmt.Printf("Deleted index file: %s\n", filename)
		}
	}
	// Clear the global index files array
	indexFiles = nil

	fmt.Println("BTree Index cleared and removed from memory.")
}

func (bt *BTree) CheckInvarianceViolation() bool {
	if bt.Root == nil {
		fmt.Println("Tree is empty - No invariance violations detected.")
		return false
	}
	return checkNodeInvariance(bt.Root)
}

func checkNodeInvariance(node *BTreeNode) bool {
	if node == nil {
		return false
	}

	// Check if entries in the current node are sorted
	for i := 1; i < len(node.Entries); i++ {
		if node.Entries[i-1].Key > node.Entries[i].Key {
			fmt.Printf("❌ Error: Entries are not sorted at node level! Found [%s] > [%s]\n",
				node.Entries[i-1].Key, node.Entries[i].Key)
			return true
		}
	}
	for i := 0; i < len(node.Entries); i++ {
		leftChildNode := node.Children[i].ChildNode
		rightChildNode := node.Children[i+1].ChildNode
		if leftChildNode != nil {
			for _, entry := range leftChildNode.Entries {
				if entry.Key > node.Entries[i].Key {
					fmt.Printf("❌ Error: Left child key [%s] > Parent key [%s]\n", entry.Key, node.Entries[i].Key)
					return true
				}
			}
		}
		if rightChildNode != nil {
			for _, entry := range rightChildNode.Entries {
				if entry.Key < node.Entries[i].Key {
					fmt.Printf("❌ Error: Right child key [%s] < Parent key [%s]\n", entry.Key, node.Entries[i].Key)
					return true
				}
			}
		}

	}

	// Recursively check child nodes
	for _, child := range node.Children {
		if child.ChildNode != nil {
			if checkNodeInvariance(child.ChildNode) {
				return true // Propagate error upwards
			}
		}
	}

	// If no violations are found
	return false
}

func (bt *BTree) PrintTree() {
	if bt.Root == nil {
		fmt.Println("Tree is empty")
		return
	}
	printNode(bt.Root, 0)
}

func printNode(node *BTreeNode, level int) {
	if node == nil {
		return
	}

	// Indentation for levels
	indent := strings.Repeat("  ", level)

	// Print current node entries
	fmt.Printf("%sLevel %d: ", indent, level)
	for _, entry := range node.Entries {
		fmt.Printf("[%s] ", entry.Key)
	}
	fmt.Println()

	// Recursively print children
	for _, child := range node.Children {
		if child.ChildNode != nil {
			printNode(child.ChildNode, level+1)
		} else {
			fmt.Printf("%s  └── [Index File: %s]\n", indent, child.IndexFile)
		}
	}
}

// Helper function to create a new index file
func createNewIndexFile() string {
	// Generate a unique filename
	indexFile := fmt.Sprintf("index_%d.db", time.Now().UnixNano())
	// file, err := os.CreateTemp("./", indexFile)
	file, err := os.Create(indexFile)
	if err != nil {
		panic(fmt.Sprintf("Failed to create index file: %v", err))
	}
	file.Close()
	// Store the filename globally
	indexFiles = append(indexFiles, indexFile)
	return file.Name()
}

func appendToIndexFile(indexFile string, indexEntry IndexEntry, checkIfEntryAlreadyExists bool) string {
	// Open the index file in read+write mode (to check for duplicate keys)
	file, err := os.OpenFile(indexFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Sprintf("Failed: Could not open index file: %v", err)
	}
	defer file.Close()

	if checkIfEntryAlreadyExists {
		// Check if the key already exists in the file
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.SplitN(line, ":", 2)
			if len(parts) != 2 {
				continue // Skip malformed lines
			}
			if parts[0] == indexEntry.Key {
				return fmt.Sprintf("Failed: Key '%s' already exists in index file", indexEntry.Key)
			}
		}
		if err := scanner.Err(); err != nil {
			return fmt.Sprintf("Failed: Error reading index file: %v", err)
		}
	}
	// Append the key and offset to the file
	_, err = file.WriteString(fmt.Sprintf("%s:%d\n", indexEntry.Key, indexEntry.FileOffset))
	if err != nil {
		return fmt.Sprintf("Failed: Could not write to index file: %v", err)
	}

	return fmt.Sprintf("Success: Key '%s' added to index file", indexEntry.Key)
}

func (bti *BTreeIndex) Insert(key string, value string) string {
	bti.mu.Lock()
	defer bti.mu.Unlock()

	// Open the data file in append mode
	file, err := os.OpenFile(bti.dataFile, os.O_APPEND|os.O_WRONLY, 0644)
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
	indexEntry := IndexEntry{
		Key:        key,
		FileOffset: offset,
	}

	return bti.btree.Insert(indexEntry)

}

// Insert adds a new key to the B-tree
func (bt *BTree) Insert(indexEntry IndexEntry) string {
	violation := bt.CheckInvarianceViolation()
	if violation {
		return "Failed: Invariance Violation Detected"
	}
	if bt.Root == nil {
		// Tree is empty, create a new root
		bt.Root = &BTreeNode{
			Entries:  []IndexEntry{indexEntry},
			IsLeaf:   true,
			Children: make([]ChildPointer, 2), // Two child pointers for before and after the key
			Parent:   nil,
		}

		// Create two new index files for before and after the key
		beforeIndexFile := createNewIndexFile()
		afterIndexFile := createNewIndexFile()

		// Assign child pointers
		bt.Root.Children[0] = ChildPointer{IndexFile: beforeIndexFile}
		bt.Root.Children[1] = ChildPointer{IndexFile: afterIndexFile}
		// Update memory usage
		entrySize := len(indexEntry.Key) + 8 // Key length + int64 offset
		childPointerSize := 2 * 16           // Two child pointers (each ~16 bytes)
		bt.currentMemoryUsage += int(entrySize + childPointerSize)
		return fmt.Sprintf("Success: Key '%s' inserted", indexEntry.Key)
	}

	// Insert key into the appropriate node
	root := bt.Root
	if len(root.Entries) >= bt.maxEntries {
		// Root is full, need to split
		newRoot := &BTreeNode{
			IsLeaf:   false,
			Children: make([]ChildPointer, 1),
			Parent:   nil,
		}

		// Point first child pointer to previous root
		newRoot.Children[0] = ChildPointer{ChildNode: root}
		root.Parent = newRoot
		bt.Root = newRoot
		success, message := bt.splitNode(newRoot, 0)
		if !success {
			return message
		}
	}

	return bt.insertNonFull(bt.Root, indexEntry)
}

// splitNode splits a full node into two nodes and adjusts the parent
func (bt *BTree) splitNode(parent *BTreeNode, index int) (bool, string) {
	//estimate memory usage
	childPointerSize := 1 * 16 // One new child pointer ( ~16 bytes)
	if bt.currentMemoryUsage+childPointerSize > MaxMemoryUsage {
		return false, "Failed: Memory limit exceeded"
	}
	fullNode := parent.Children[index].ChildNode
	midIndex := len(fullNode.Entries) / 2
	midKey := fullNode.Entries[midIndex].Key
	midFileOffset := fullNode.Entries[midIndex].FileOffset

	midElement := IndexEntry{
		Key:        midKey,
		FileOffset: midFileOffset,
	}

	// Calculate new node sizes explicitly
	rightEntryCount := len(fullNode.Entries) - (midIndex + 1)
	rightChildCount := len(fullNode.Children) - (midIndex + 1)

	// Create a new node for the right half
	newNode := &BTreeNode{
		Entries:  make([]IndexEntry, rightEntryCount),
		Children: make([]ChildPointer, rightChildCount),
		IsLeaf:   fullNode.IsLeaf,
		Parent:   parent,
	}

	// Copy entries manually
	for i := 0; i < rightEntryCount; i++ {
		newNode.Entries[i] = fullNode.Entries[midIndex+1+i]
	}

	// Copy children manually
	for i := 0; i < rightChildCount; i++ {
		newNode.Children[i] = fullNode.Children[midIndex+1+i]
		if newNode.Children[i].ChildNode != nil {
			newNode.Children[i].ChildNode.Parent = newNode // Update parent pointer
		}
	}

	// Adjust the original node explicitly
	newEntryCount := midIndex     // Only keep left side entries
	newChildCount := midIndex + 1 // Keep left side children

	oldEntries := make([]IndexEntry, newEntryCount)
	oldChildren := make([]ChildPointer, newChildCount)

	for i := 0; i < newEntryCount; i++ {
		oldEntries[i] = fullNode.Entries[i]
	}

	for i := 0; i < newChildCount; i++ {
		oldChildren[i] = fullNode.Children[i]
	}

	fullNode.Entries = oldEntries
	fullNode.Children = oldChildren

	// Convert newNode into a ChildPointer
	newChildPointer := ChildPointer{
		ChildNode: newNode,
	}

	// Update memory usage
	bt.currentMemoryUsage += int(childPointerSize)

	// Create new slices for parent's updated entries and children
	newEntries := make([]IndexEntry, len(parent.Entries)+1)
	newChildren := make([]ChildPointer, len(parent.Children)+1)

	// Insert entries into the new slice while maintaining order
	for i := 0; i < index; i++ {
		newEntries[i] = parent.Entries[i]
	}
	newEntries[index] = midElement
	for i := index + 1; i < len(newEntries); i++ {
		newEntries[i] = parent.Entries[i-1]
	}

	// Insert children into the new slice while maintaining order
	for i := 0; i <= index; i++ {
		newChildren[i] = parent.Children[i]
	}
	newChildren[index+1] = newChildPointer
	for i := index + 2; i < len(newChildren); i++ {
		newChildren[i] = parent.Children[i-1]
	}

	// Update the parent node
	parent.Entries = newEntries
	parent.Children = newChildren

	return true, ""
}

// insertNonFull inserts a key into a node that is not full
func (bt *BTree) insertNonFull(node *BTreeNode, indexEntry IndexEntry) string {
	if node.IsLeaf {
		i := 0
		for i < len(node.Entries) && node.Entries[i].Key < indexEntry.Key {
			i++
		}

		if i < len(node.Entries) && indexEntry.Key == node.Entries[i].Key {
			return fmt.Sprintf("Failed: Key '%s' already exists", indexEntry.Key)
		}

		// Case 1: If the key is out of bounds (before first or after last key)
		if i == 0 || i == len(node.Entries) {
			// Check if we have enough keys already
			if len(node.Entries) < bt.maxEntries { // Assuming max of 5 keys per node before splitting
				//estimate memory usage
				entrySize := len(indexEntry.Key) + 8 // Key length + int64 offset
				childPointerSize := 1 * 16           // One child pointer (each ~16 bytes)
				if bt.currentMemoryUsage+entrySize+childPointerSize > MaxMemoryUsage {
					return "Failed: Memory limit exceeded"
				}
				if i == 0 {
					// Check smallest key in the first index file
					indexFile := node.Children[0].IndexFile
					if indexFile != "" {
						smallestKey, _, found := getMinEntryFromIndexFile(indexFile)
						if found && indexEntry.Key > smallestKey {
							// Append to index file instead
							return appendToIndexFile(indexFile, indexEntry, true)
						}
					}
					// Manually inserting `indexEntry` at the beginning of `node.Entries`
					newEntries := make([]IndexEntry, len(node.Entries)+1)
					newChildren := make([]ChildPointer, len(node.Children)+1)

					// Insert the new entry at the beginning
					newEntries[0] = indexEntry

					// Copy the existing entries to their new position
					for i := 0; i < len(node.Entries); i++ {
						newEntries[i+1] = node.Entries[i]
					}

					// Create an index file for the range before the first key
					rangeIndexFile := createNewIndexFile()
					// Create a new index file and insert it at the beginning of `Children`
					newChildren[0] = ChildPointer{IndexFile: rangeIndexFile}

					// Copy the existing children to their new position
					for i := 0; i < len(node.Children); i++ {
						newChildren[i+1] = node.Children[i]
					}

					// Assign back to the node
					node.Entries = newEntries
					node.Children = newChildren
				} else {
					// Check largest key in the last index file
					indexFile := node.Children[len(node.Entries)].IndexFile
					if indexFile != "" {
						largestKey, _, found := getMaxEntryFromIndexFile(indexFile)
						if found && indexEntry.Key < largestKey {
							// Append to index file instead
							return appendToIndexFile(indexFile, indexEntry, true)
						}
					}
					// Manually inserting `indexEntry` at the end of `node.Entries`
					newEntries := make([]IndexEntry, len(node.Entries)+1)
					newChildren := make([]ChildPointer, len(node.Children)+1)

					// Copy existing entries
					for i := 0; i < len(node.Entries); i++ {
						newEntries[i] = node.Entries[i]
					}
					// Insert the new entry at the end
					newEntries[len(newEntries)-1] = indexEntry

					// Copy existing children
					for i := 0; i < len(node.Children); i++ {
						newChildren[i] = node.Children[i]
					}

					// Create an index file for the range after the last key
					rangeIndexFile := createNewIndexFile()
					// Insert the new child pointer at the end
					newChildren[len(newChildren)-1] = ChildPointer{IndexFile: rangeIndexFile}

					// Assign back to the node
					node.Entries = newEntries
					node.Children = newChildren
				}
				// Update memory usage
				bt.currentMemoryUsage += int(entrySize + childPointerSize)
				return fmt.Sprintf("Success: Key '%s' inserted", indexEntry.Key)
			}
		}

		// Case 2: Look up the respective index file and append the entry
		indexFile := node.Children[i].IndexFile

		return appendToIndexFile(indexFile, indexEntry, true)
	}

	// Case 3: Insert into an internal node
	i := 0
	for i < len(node.Entries) && node.Entries[i].Key < indexEntry.Key {
		i++
	}

	if i < len(node.Entries) && indexEntry.Key == node.Entries[i].Key {
		return fmt.Sprintf("Failed: Key '%s' already exists", indexEntry.Key)
	}
	child := node.Children[i].ChildNode
	if len(child.Entries) >= bt.maxEntries { // Assuming max of 5 keys per node before splitting
		success, message := bt.splitNode(node, i)
		if !success {
			return message
		}
		if indexEntry.Key > node.Entries[i].Key {
			child = node.Children[i+1].ChildNode
		}
	}
	return bt.insertNonFull(child, indexEntry)
}

// traverse recursively visits each node and prints keys
func (bt *BTree) Traverse() {
	if bt.Root != nil {
		bt.traverseNode(bt.Root)
	}
}

func (bt *BTree) traverseNode(node *BTreeNode) {
	// Traverse the subtree rooted at this node
	for i := 0; i < len(node.Entries); i++ {
		if !node.IsLeaf {
			bt.traverseNode(node.Children[i].ChildNode)
		}
		fmt.Printf("%s,%d", node.Entries[i].Key, node.Entries[i].FileOffset)
	}
	if !node.IsLeaf {
		bt.traverseNode(node.Children[len(node.Entries)].ChildNode)
	}
}

func retrieveDataAtOffset(dataFile string, offset int64) (string, bool) {
	file, err := os.Open(dataFile)
	if err != nil {
		panic(fmt.Sprintf("Failed to open data file: %v", err))
	}
	defer file.Close()

	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return "", false
	}

	reader := bufio.NewReader(file)
	value, err := reader.ReadString('\n')
	if err != nil {
		return "", false
	}

	return strings.TrimSpace(value), true
}

// search recursively searches for a key in the B-tree
func (btreeIndex *BTreeIndex) Search(key string) (string, bool) {
	offset := btreeIndex.btree.searchKey(btreeIndex.btree.Root, key)
	if offset != -1 {
		return retrieveDataAtOffset(btreeIndex.dataFile, offset)
	}
	return "", false
}

func searchIndexFile(indexFile, key string) int64 {
	file, err := os.Open(indexFile)
	if err != nil {
		panic(fmt.Sprintf("Failed to open index file: %v", err))
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		currentKey := parts[0]
		offset, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			continue
		}

		if currentKey == key {
			return offset // Key found, return offset
		}
	}

	if err := scanner.Err(); err != nil {
		panic(fmt.Sprintf("Failed to read index file: %v", err))
	}

	return -1 // Key not found
}

func (bt *BTree) searchKey(node *BTreeNode, key string) int64 {
	if node == nil {
		return -1
	}
	i := 0
	for i < len(node.Entries) && key > node.Entries[i].Key {
		i++
	}
	if i < len(node.Entries) && node.Entries[i].Key == key {
		return node.Entries[i].FileOffset
	}
	// If we've reached a leaf node, check the corresponding index file
	if node.IsLeaf {
		if i < len(node.Children) {
			indexFile := node.Children[i].IndexFile
			if indexFile == "" {
				panic("Index File missing for range")
			}
			if indexFile != "" {
				return searchIndexFile(indexFile, key)
			}
		}
		return -1 // Key not found
	}
	return bt.searchKey(node.Children[i].ChildNode, key)
}

func (bt *BTree) LoadBTree(encodedBTreeIndexFilefilename string) (*BTree, error) {
	file, err := os.Open(encodedBTreeIndexFilefilename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		return nil, scanner.Err()
	}

	data := scanner.Text()
	bt, err = parseNode(data, bt)
	return bt, err
}

func parseNode(data string, bt *BTree) (*BTree, error) {
	for strings.HasPrefix(data, "}") {
		data = data[1:]
	}
	for strings.HasPrefix(data, "{") {
		data = data[1:]
	}
	parts := strings.SplitN(data, ":", 2)
	if len(parts) != 2 {
		return bt, nil
	}
	key := parts[0]

	value, rest := splitAtFirstCurly(parts[1])
	valueInt, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return bt, err
	}
	node := IndexEntry{
		Key:        key,
		FileOffset: valueInt,
	}
	//insert node to btree
	bt.Insert(node)
	if rest != "" {
		bt, err = parseNode(rest, bt)
		return bt, err
	}
	return bt, nil
}

func splitAtFirstCurly(s string) (string, string) {
	// Find the first occurrence of either '{' or '}' in the string
	openCurly := strings.Index(s, "{")
	closeCurly := strings.Index(s, "}")

	// Determine the smallest index (the first occurrence)
	if openCurly == -1 && closeCurly == -1 {
		return s, "" // No delimiters found
	} else if openCurly == -1 {
		return s[:closeCurly], s[closeCurly:]
	} else if closeCurly == -1 {
		return s[:openCurly], s[openCurly:]
	} else if openCurly < closeCurly {
		return s[:openCurly], s[openCurly:]
	} else {
		return s[:closeCurly], s[closeCurly:]
	}
}

func (bti *BTreeIndex) EncodeBTreeIndexToFile() error {
	file, err := os.Create(bti.encodedBTreeIndexFile)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	err = encodeNode(writer, bti.btree.Root)
	if err != nil {
		return err
	}
	return writer.Flush()
}

func encodeNode(writer *bufio.Writer, node *BTreeNode) error {
	if node == nil {
		return nil
	}

	// Start encoding this node
	writer.WriteString("{")

	for i := 0; i < len(node.Entries); i++ {
		// If it's a leaf node, write index file contents
		if node.IsLeaf {
			indexFile := node.Children[i].IndexFile
			if indexFile != "" {
				entries, err := readIndexFile(indexFile)
				if err != nil {
					return err
				}
				writer.WriteString("#" + entries + "#")
			}
		} else {
			// If it's not a leaf, recursively encode the child node
			if node.Children[i].ChildNode != nil {
				err := encodeNode(writer, node.Children[i].ChildNode)
				if err != nil {
					return err
				}
			}
		}
		// Write the current entry
		writer.WriteString(fmt.Sprintf("%s:%d", node.Entries[i].Key, node.Entries[i].FileOffset))
	}

	// Encode last child pointer if it's a leaf node
	if node.IsLeaf {
		indexFile := node.Children[len(node.Entries)].IndexFile
		if indexFile != "" {
			entries, err := readIndexFile(indexFile)
			if err != nil {
				return err
			}
			writer.WriteString("#" + entries + "#")
		}
	} else {
		// Encode last child pointer for internal nodes
		if node.Children[len(node.Entries)].ChildNode != nil {
			err := encodeNode(writer, node.Children[len(node.Entries)].ChildNode)
			if err != nil {
				return err
			}
		}
	}

	// Close this node
	writer.WriteString("}")
	return nil
}

// Reads all entries from an index file as a string
func readIndexFile(indexFile string) (string, error) {
	file, err := os.Open(indexFile)
	if err != nil {
		return "", err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func (bti *BTreeIndex) Delete(key string) string {
	return bti.btree.delete(key)
}

func (bti *BTreeIndex) Update(key string, value string) string {
	bti.Delete(key)
	return bti.Insert(key, value)
}
func (bt *BTree) delete(key string) string {
	violation := bt.CheckInvarianceViolation()
	if violation {
		return "Failed: violation detected"
	}
	if bt.Root == nil {
		return fmt.Sprintf("Failed: Key '%s' does not exist", key)
	}
	return bt.deleteRecursive(bt.Root, key)
}

func (bt *BTree) deleteRecursive(node *BTreeNode, key string) string {
	// Find the index where the key should be in the node
	i := 0
	for i < len(node.Entries) && key > node.Entries[i].Key {
		i++
	}

	// Case 1: If the node is a LEAF NODE
	if node.IsLeaf {
		// Key is found in this node itself, delete it directly
		if i < len(node.Entries) && node.Entries[i].Key == key {
			return bt.deleteLeafNodeEntry(node, i)
		}
		// If key is not in the node, search in the corresponding index file
		return bt.deleteFromIndexFile(node.Children[i].IndexFile, key)
	}

	// Case 2: If the node is an INTERNAL NODE
	if i < len(node.Entries) && node.Entries[i].Key == key {
		return bt.deleteInternalNodeEntry(node, i, key)
	}

	// Case 3: Key is in a subtree, recurse into the correct child
	if node.Children[i].ChildNode == nil {
		return fmt.Sprintf("Failed: Key '%s' not found in tree", key)
	}
	return bt.deleteRecursive(node.Children[i].ChildNode, key)
}

func DeleteIndexFile(indexFile string) {
	// Count valid entries first to determine the exact size
	count := 0
	for _, file := range indexFiles {
		if file != indexFile {
			count++
		}
	}

	// Allocate a new slice with the exact required size
	newIndexFiles := make([]string, count)
	index := 0
	for _, file := range indexFiles {
		if file != indexFile {
			newIndexFiles[index] = file
			index++
		}
	}

	// Update the global indexFiles array
	indexFiles = newIndexFiles
}

func (bt *BTree) deleteLeafNodeEntry(node *BTreeNode, index int) string {
	leftIndexFile := node.Children[index].IndexFile
	rightIndexFile := node.Children[index+1].IndexFile

	// Try replacing with the predecessor from left index file
	if leftIndexFile != "" {
		predecessorKey, predecessorOffset, found := getMaxEntryFromIndexFile(leftIndexFile)
		if found {
			node.Entries[index] = IndexEntry{Key: predecessorKey, FileOffset: predecessorOffset}
			bt.deleteFromIndexFile(leftIndexFile, predecessorKey)
			return "Success: Replaced with predecessor and deleted from index file"
		}
	}

	// If left failed, try replacing with the successor from right index file
	if rightIndexFile != "" {
		successorKey, successorOffset, found := getMinEntryFromIndexFile(rightIndexFile)
		if found {
			node.Entries[index] = IndexEntry{Key: successorKey, FileOffset: successorOffset}
			bt.deleteFromIndexFile(rightIndexFile, successorKey)
			return "Success: Replaced with successor and deleted from index file"
		}
	}

	//  If neither index file had entries, check if this is the only entry
	if len(node.Entries) == 1 {
		// remove the entry and both child pointers  and delete the both left and right index file
		node.Entries = make([]IndexEntry, 0)
		node.Children = make([]ChildPointer, 0)
		os.Remove(leftIndexFile)
		DeleteIndexFile(leftIndexFile)
		os.Remove(rightIndexFile)
		DeleteIndexFile(rightIndexFile)
		// If the node also happens to be the root node
		if node == bt.Root {
			bt.currentMemoryUsage = 0
			bt.Root = nil
		}
		return "Success: Root node removed, tree is empty"
	}

	//  If node has more than one entry, remove the entry and delete the left index file
	//estimate memory saved
	entrySize := len(node.Entries[index].Key) + 8 // Key length + int64 offset
	childPointerSize := 1 * 16                    // One child pointer (each ~16 bytes)

	newEntries := make([]IndexEntry, len(node.Entries)-1)
	newChildren := make([]ChildPointer, len(node.Children)-1)
	for i := 0; i < len(node.Entries); i++ {
		if i < index {
			newEntries[i] = node.Entries[i]
		}
		if i > index {
			newEntries[i-1] = node.Entries[i]
		}
	}
	node.Entries = newEntries

	os.Remove(leftIndexFile)
	DeleteIndexFile(leftIndexFile)
	for i := 0; i < len(node.Children); i++ {
		if i < index {
			newChildren[i] = node.Children[i]
		}
		if i > index {
			newChildren[i-1] = node.Children[i]
		}
	}

	node.Children = newChildren
	// update memory usage
	bt.currentMemoryUsage -= (entrySize + childPointerSize)
	return "Success: Entry removed and left index file deleted"
}

func getMaxEntryFromIndexFile(indexFile string) (string, int64, bool) {
	file, err := os.Open(indexFile)
	if err != nil {
		fmt.Println("Error opening index file:", err)
		return "", 0, false
	}
	defer file.Close()

	var maxKey string
	var maxOffset int64
	firstEntry := false // Flag to track first entry

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		key := parts[0]
		offset, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			continue
		}

		// Update maxKey on first valid entry or if key is greater
		if !firstEntry || key > maxKey {
			maxKey = key
			maxOffset = offset
			firstEntry = true
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading index file:", err)
		return "", 0, false
	}

	// If we never updated maxKey, return false
	if !firstEntry {
		return "", 0, false
	}

	return maxKey, maxOffset, true
}

func getMinEntryFromIndexFile(indexFile string) (string, int64, bool) {
	file, err := os.Open(indexFile)
	if err != nil {
		fmt.Println("Error opening index file:", err)
		return "", 0, false
	}
	defer file.Close()

	var minKey string
	var minOffset int64
	firstEntry := false // Flag to track first valid entry

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		key := parts[0]
		offset, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			continue
		}

		// Initialize minKey on first valid entry or update if key is smaller
		if !firstEntry || key < minKey {
			minKey = key
			minOffset = offset
			firstEntry = true
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading index file:", err)
		return "", 0, false
	}

	// If no valid key was found, return false
	if !firstEntry {
		return "", 0, false
	}

	return minKey, minOffset, true
}

func (bt *BTree) deleteInternalNodeEntry(node *BTreeNode, index int, key string) string {
	if node.IsLeaf {
		// Should never reach here because internal nodes are not leaves
		return "Failed: Internal Node Entry deletion failed as node was leaf"
	}

	leftChild := node.Children[index].ChildNode
	rightChild := node.Children[index+1].ChildNode

	// Case 1.1: Left child has extra keys, replace with predecessor
	if len(leftChild.Entries) > (bt.maxEntries / 2) {
		predecessor := bt.getPredecessor(leftChild)
		node.Entries[index] = predecessor
		//update memory usage
		bt.currentMemoryUsage = len(predecessor.Key) - len(node.Entries[index].Key) // -deleted key + predecessory key
		return bt.deleteRecursive(leftChild, predecessor.Key)
	}

	// Case 1.2: Right child has extra keys, replace with successor
	if len(rightChild.Entries) > (bt.maxEntries / 2) {
		successor := bt.getSuccessor(rightChild)
		node.Entries[index] = successor
		//update memory usage
		bt.currentMemoryUsage = len(successor.Key) - len(node.Entries[index].Key) // -deleted key + successor key
		return bt.deleteRecursive(rightChild, successor.Key)
	}

	// Case 1.3: Merge left and right child
	mergedNode := bt.mergeNodes(leftChild, rightChild, node.Entries[index])
	// estimate memory saved
	childPointerSize := 1 * 16 // One child pointer (each ~16 bytes)

	// Replace leftChild with mergedNode in node.Children
	node.Children[index].ChildNode = mergedNode
	newEntries := make([]IndexEntry, len(node.Entries)-1)
	newChildren := make([]ChildPointer, len(node.Children)-1)
	var i int
	for i = 0; i < len(node.Entries); i++ {
		if i < index {
			newEntries[i] = node.Entries[i]
		}
		// skip the key to be deleted entry from node.Entries
		if i > index {
			newEntries[i-1] = node.Entries[i]
		}
	}

	for i = 0; i < len(node.Children); i++ {
		if i <= index {
			newChildren[i] = node.Children[i]
		}
		// skip over original rightChild node which would be at node.Children[index+1], as it has been merged with leftChild node into mergedNode
		if i > index+1 {
			newChildren[i-1] = node.Children[i]
		}
	}

	node.Entries = newEntries
	node.Children = newChildren

	//update memory usage
	bt.currentMemoryUsage -= childPointerSize
	// If root is empty after deletion, update root
	if len(node.Entries) == 0 && node == bt.Root {
		bt.Root = mergedNode
		mergedNode.Parent = nil
	}

	return bt.deleteRecursive(mergedNode, key)
}

func (bt *BTree) deleteFromIndexFile(indexFile string, key string) string {
	if indexFile == "" {
		return fmt.Sprintf("Failed: Key '%s' not found in index files, indexFile missing", key)
	}

	// Open and scan the index file
	file, err := os.Open(indexFile)
	if err != nil {
		return fmt.Sprintf("Failed: Could not open index file %s", indexFile)
	}
	defer file.Close()

	// Read and rewrite the file excluding the key
	tempFile, err := os.Create(indexFile + ".tmp")
	if err != nil {
		return fmt.Sprintf("Failed: Could not create temp index file: %v", err)
	}
	defer tempFile.Close()

	scanner := bufio.NewScanner(file)
	writer := bufio.NewWriter(tempFile)

	keyFound := false
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		if parts[0] == key {
			keyFound = true
			continue // Skip this key
		}

		writer.WriteString(line + "\n")
	}

	writer.Flush()

	// Replace old file with temp file
	if err := os.Rename(indexFile+".tmp", indexFile); err != nil {
		return fmt.Sprintf("Failed: Could not replace index file: %v", err)
	}

	if keyFound {
		return fmt.Sprintf("Success: Key '%s' deleted from index file", key)
	}
	return fmt.Sprintf("Failed: Key '%s' not found", key)
}

func (bt *BTree) mergeNodes(left, right *BTreeNode, midEntry IndexEntry) *BTreeNode {
	right.Parent = nil
	mergedNodeEntries := make([]IndexEntry, len(left.Entries)+1+len(right.Entries))
	mergedNodeChildren := make([]ChildPointer, len(left.Children)+len(right.Children))
	var i, j int

	for i = 0; i < len(left.Entries); i++ {
		mergedNodeEntries[i] = left.Entries[i]
	}
	mergedNodeEntries[i] = midEntry
	for j = 0; j < len(right.Entries); j++ {
		i++
		mergedNodeEntries[i] = right.Entries[j]
	}
	for i = 0; i < len(left.Children); i++ {
		mergedNodeChildren[i] = left.Children[i]
	}
	for j = 0; j < len(right.Children); j++ {
		mergedNodeChildren[i] = right.Children[j]
		i++
	}

	mergedNode := &BTreeNode{
		Entries:  mergedNodeEntries,
		Children: mergedNodeChildren,
		IsLeaf:   left.IsLeaf,
		Parent:   left.Parent,
	}
	// Only update parent pointers for children that are non-nil
	for _, child := range mergedNode.Children {
		if child.ChildNode != nil {
			child.ChildNode.Parent = mergedNode
		}
	}

	return mergedNode
}

func (bt *BTree) getPredecessor(node *BTreeNode) IndexEntry {
	if node.IsLeaf {
		return node.Entries[len(node.Entries)-1]
	}
	// Move to the rightmost child of the left subtree
	curr := node.Children[len(node.Children)-1].ChildNode
	for !curr.IsLeaf {
		curr = curr.Children[len(curr.Children)-1].ChildNode
	}
	// Return the rightmost key in this leaf node
	return curr.Entries[len(curr.Entries)-1]
}

func (bt *BTree) getSuccessor(node *BTreeNode) IndexEntry {
	if node.IsLeaf {
		return node.Entries[0]
	}
	// Move to the leftmost child of the right subtree
	curr := node.Children[0].ChildNode
	for !curr.IsLeaf {
		curr = curr.Children[0].ChildNode
	}
	// Return the leftmost key in this leaf node
	return curr.Entries[0]
}

func (bti *BTreeIndex) DecodeAndReconstructBTreeIndexFromEncodedBTreeIndexFile(filename string) (*BTree, error) {
	// Read the encoded file
	fileContent, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read encoded file: %v", err)
	}

	encodedStr := string(fileContent)
	if len(encodedStr) == 0 {
		return NewBTree() // Return a new empty BTree
	}

	stack := []interface{}{} // Stack to store elements during parsing
	var i int                // Index tracker
	var memoryUsage int
	for i < len(encodedStr) {
		char := encodedStr[i]

		switch char {
		case '{':
			// Push start marker
			stack = append(stack, "{")
			i++

		case '}':
			// Process stack to construct a node
			var entries []IndexEntry
			var children []ChildPointer
			isLeaf := false

			// Pop items from stack until we find "{"
			for len(stack) > 0 {
				top := stack[len(stack)-1]
				stack = stack[:len(stack)-1] // Pop

				if top == "{" {
					break
				}

				switch v := top.(type) {
				case string:
					// If it's an index file content (enclosed in `#...#`), create a new index file
					if strings.HasPrefix(v, "#") && strings.HasSuffix(v, "#") {
						indexFile := createNewIndexFile() // Create a new index file
						isLeaf = true

						// Extract key-value pairs from `#...#`
						indexEntries := v[1 : len(v)-1] // Remove `#` markers
						lines := strings.Split(indexEntries, "\n")

						for _, line := range lines {
							parts := strings.SplitN(line, ":", 2)
							if len(parts) != 2 {
								continue // Ignore malformed entries
							}

							offset, err := strconv.ParseInt(parts[1], 10, 64)
							if err != nil {
								continue // Ignore invalid offsets
							}

							indexEntry := IndexEntry{Key: parts[0], FileOffset: offset}
							appendToIndexFile(indexFile, indexEntry, false) // Store in index file
						}

						// Add the child pointer with the created index file
						children = append([]ChildPointer{{IndexFile: indexFile}}, children...)
						//update memory usage
						childPointerSize := 1 * 16
						memoryUsage += childPointerSize
					} else {
						// Key-Value Entry
						parts := strings.SplitN(v, ":", 2)
						if len(parts) == 2 {
							offset, err := strconv.ParseInt(parts[1], 10, 64)
							if err != nil {
								return nil, fmt.Errorf("invalid offset: %s", parts[1])
							}
							entries = append([]IndexEntry{{Key: parts[0], FileOffset: offset}}, entries...)
							// update memory usage
							entrySize := len(parts[0]) + 8 // 8 bytes for offset
							memoryUsage += entrySize
						}
					}

				case *BTreeNode:
					// If we encounter a child node, add it to children array
					children = append([]ChildPointer{{ChildNode: v}}, children...)
					// update memory usage
					childPointerSize := 1 * 16
					memoryUsage += childPointerSize
				}
			}

			// Create a new BTreeNode
			newNode := &BTreeNode{
				Entries:  entries,
				Children: children,
				IsLeaf:   isLeaf,
			}

			// Push the newly created node back onto the stack
			stack = append(stack, newNode)
			i++

		case '#':
			// Extract index file contents between two `#`
			endIndex := strings.Index(encodedStr[i+1:], "#")
			if endIndex == -1 {
				return nil, fmt.Errorf("malformed encoding, unmatched '#'")
			}
			endIndex += i + 1

			stack = append(stack, encodedStr[i:endIndex+1]) // Include both `#`
			i = endIndex + 1

		default:
			// Extract Key:Value pair
			endIndex := i
			for endIndex < len(encodedStr) && encodedStr[endIndex] != '{' && encodedStr[endIndex] != '}' && encodedStr[endIndex] != '#' {
				endIndex++
			}

			if endIndex > i {
				stack = append(stack, encodedStr[i:endIndex])
				i = endIndex
			}
		}
	}

	// The last remaining element on the stack should be the root
	if len(stack) != 1 {
		return nil, fmt.Errorf("decoding failed, stack is not empty")
	}

	rootNode, ok := stack[0].(*BTreeNode)
	if !ok {
		return nil, fmt.Errorf("decoding failed, root is not a BTreeNode")
	}

	return &BTree{Root: rootNode, maxEntries: 5, currentMemoryUsage: memoryUsage}, nil
}

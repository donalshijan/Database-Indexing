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
			fmt.Printf("Deleted index file: %s\n", indexFile)
		}
	}

	// Clear the index files list
	indexFiles = []string{}

	// Reset the B-tree structure
	bti.btree.Root = nil
	bti.btree.currentMemoryUsage = 0

	fmt.Println("B-tree index cleared and all index files removed successfully.")
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

func appendToIndexFile(indexFile string, indexEntry IndexEntry) {
	file, err := os.OpenFile(indexFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(fmt.Sprintf("Failed to open index file: %v", err))
	}
	defer file.Close()

	// Write the key and offset as "key:offset"
	writer := bufio.NewWriter(file)
	_, err = writer.WriteString(fmt.Sprintf("%s:%d\n", indexEntry.Key, indexEntry.FileOffset))
	if err != nil {
		panic(fmt.Sprintf("Failed to write to index file: %v", err))
	}
	writer.Flush()
}

func (bti *BTreeIndex) Insert(key string, value string) string {
	bti.mu.Lock()
	defer bti.mu.Unlock()

	// Check if the key already exists in the index
	if true {
		return fmt.Sprintf("Failed: Key '%s' already exists", key)
	}

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
	if bt.Root == nil {
		// Tree is empty, create a new root
		bt.Root = &BTreeNode{
			Entries:  []IndexEntry{indexEntry},
			IsLeaf:   true,
			Children: make([]ChildPointer, 2), // Two child pointers for before and after the key
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
	if len(root.Entries) == bt.maxEntries {
		// Root is full, need to split
		newRoot := &BTreeNode{
			IsLeaf:   false,
			Children: make([]ChildPointer, 1),
		}

		// Point first child pointer to previous root
		newRoot.Children[0] = ChildPointer{ChildNode: root}
		bt.Root = newRoot
		success := bt.splitNode(newRoot, 0)
		if !success {
			return "Failed: Split node failed"
		}
	}

	return bt.insertNonFull(bt.Root, indexEntry)
}

// splitNode splits a full node into two nodes and adjusts the parent
func (bt *BTree) splitNode(parent *BTreeNode, index int) bool {
	//estimate memory usage
	childPointerSize := 1 * 16 // One new child pointer ( ~16 bytes)
	if bt.currentMemoryUsage+childPointerSize > MaxMemoryUsage {
		return false
	}
	fullNode := parent.Children[index].ChildNode
	midIndex := len(fullNode.Entries) / 2
	midKey := fullNode.Entries[midIndex].Key
	midFileOffset := fullNode.Entries[midIndex].FileOffset

	midElement := IndexEntry{
		Key:        midKey,
		FileOffset: midFileOffset,
	}

	// Create a new node for the split
	newNode := &BTreeNode{
		Entries:  fullNode.Entries[midIndex+1:],
		Children: fullNode.Children[midIndex+1:],
		IsLeaf:   fullNode.IsLeaf,
		Parent:   parent,
	}

	// Adjust the original node
	fullNode.Entries = fullNode.Entries[:midIndex]
	fullNode.Children = fullNode.Children[:midIndex+1]

	// Convert newNode into a ChildPointer
	newChildPointer := ChildPointer{
		ChildNode: newNode,
	}

	// Update memory usage
	bt.currentMemoryUsage += int(childPointerSize)

	// Update the parent node
	parent.Entries = append(parent.Entries[:index], append([]IndexEntry{midElement}, parent.Entries[index:]...)...)
	parent.Children = append(parent.Children[:index+1], append([]ChildPointer{newChildPointer}, parent.Children[index+1:]...)...)
	return true
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
					// Prepend the entry to the beginning of the entries array
					node.Entries = append([]IndexEntry{indexEntry}, node.Entries...)

					// Create an index file for the range before the first key
					rangeIndexFile := createNewIndexFile()
					node.Children = append([]ChildPointer{{IndexFile: rangeIndexFile}}, node.Children...)
				} else {
					// Append the entry to the end of the entries array
					node.Entries = append(node.Entries, indexEntry)

					// Create an index file for the range after the last key
					rangeIndexFile := createNewIndexFile()
					node.Children = append(node.Children, ChildPointer{IndexFile: rangeIndexFile})
				}
				// Update memory usage
				bt.currentMemoryUsage += int(entrySize + childPointerSize)
				return fmt.Sprintf("Success: Key '%s' inserted", indexEntry.Key)
			}
		}

		// Case 2: Look up the respective index file and append the entry
		indexFile := node.Children[i].IndexFile

		appendToIndexFile(indexFile, indexEntry)
		return fmt.Sprintf("Success: Key '%s' inserted", indexEntry.Key)
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
	if len(child.Entries) == bt.maxEntries { // Assuming max of 5 keys per node before splitting
		success := bt.splitNode(node, i)
		if !success {
			return "Failed: Split node failed"
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
			if indexFile != "" {
				return searchIndexFile(indexFile, key) // Search the index file
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
		return bt.deleteInternalNode(node, i, key)
	}

	// Case 3: Key is in a subtree, recurse into the correct child
	if node.Children[i].ChildNode == nil {
		return fmt.Sprintf("Failed: Key '%s' not found in tree", key)
	}
	return bt.deleteRecursive(node.Children[i].ChildNode, key)
}

func DeleteIndexFile(indexFile string) {
	// Remove the file from the global indexFiles array
	for i, file := range indexFiles {
		if file == indexFile {
			indexFiles = append(indexFiles[:i], indexFiles[i+1:]...) // Remove from slice
			break
		}
	}
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

	//  If neither index file had entries, check if this is the only entry in root node
	if len(node.Entries) == 1 && node == bt.Root {
		os.Remove(leftIndexFile)
		DeleteIndexFile(leftIndexFile)
		bt.currentMemoryUsage = 0
		os.Remove(rightIndexFile)
		DeleteIndexFile(rightIndexFile)
		bt.Root = nil
		return "Success: Root node removed, tree is empty"
	}

	//  If not root, remove the entry and delete the left index file
	//estimate memory saved
	entrySize := len(node.Entries[index].Key) + 8 // Key length + int64 offset
	childPointerSize := 1 * 16                    // One child pointer (each ~16 bytes)
	node.Entries = append(node.Entries[:index], node.Entries[index+1:]...)
	os.Remove(leftIndexFile)
	DeleteIndexFile(leftIndexFile)
	node.Children = append(node.Children[:index], node.Children[index+1:]...)
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
	if firstEntry {
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
	if firstEntry {
		return "", 0, false
	}

	return minKey, minOffset, true
}

func (bt *BTree) deleteInternalNode(node *BTreeNode, index int, key string) string {
	if node.IsLeaf {
		// Should never reach here because internal nodes are not leaves
		return "Error: Internal node marked as leaf"
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
	// Remove rightChild from node.Children
	node.Children = append(node.Children[:index], node.Children[index+1:]...)
	// Remove the key to be deleted entry from node.Entries
	node.Entries = append(node.Entries[:index], node.Entries[index+1:]...)

	//update memory usage
	bt.currentMemoryUsage -= childPointerSize
	// If root is empty after deletion, update root
	if len(node.Entries) == 0 && node == bt.Root {
		bt.Root = mergedNode
	}

	return bt.deleteRecursive(mergedNode, key)
}

func (bt *BTree) deleteFromIndexFile(indexFile string, key string) string {
	if indexFile == "" {
		return fmt.Sprintf("Failed: Key '%s' not found in index files", key)
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
	mergedNode := &BTreeNode{
		Entries:  append(left.Entries, append([]IndexEntry{midEntry}, right.Entries...)...),
		Children: append(left.Children, right.Children...),
		IsLeaf:   left.IsLeaf,
	}

	// Update parent pointers
	for _, child := range mergedNode.Children {
		child.ChildNode.Parent = mergedNode
	}

	return mergedNode
}

func (bt *BTree) getPredecessor(node *BTreeNode) IndexEntry {
	// Move to the rightmost child of the left subtree
	curr := node.Children[len(node.Children)-1].ChildNode
	for !curr.IsLeaf {
		curr = curr.Children[len(curr.Children)-1].ChildNode
	}
	// Return the rightmost key in this leaf node
	return curr.Entries[len(curr.Entries)-1]
}

func (bt *BTree) getSuccessor(node *BTreeNode) IndexEntry {
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
							appendToIndexFile(indexFile, indexEntry) // Store in index file
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

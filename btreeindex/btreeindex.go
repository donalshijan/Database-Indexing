package btreeindex

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Global atomic counter
var indexFileCounter uint64

type IndexEntry struct {
	Key        string
	FileOffset int64 // Offset in the file for this node
}

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
	Root                                *BTreeNode
	currentMemoryUsage                  int
	maxEntries                          int
	minEntries                          int
	indexFileToStoreInternalNodeEntries string
}

type BTreeIndex struct {
	dataFile              string
	encodedBTreeIndexFile string
	btree                 *BTree
	mu                    sync.RWMutex
}

var indexFiles []string

const MaxMemoryUsage = 102400 // bytes (102400 = 100KB ~= 0.1MB)

func (bti *BTreeIndex) GetMaxMemoryUsage() int64 {
	return MaxMemoryUsage
}

func (bti *BTreeIndex) GetCurrentMemoryUsage() int64 {
	return int64(bti.btree.currentMemoryUsage)
}

type MinHeap []IndexEntry

func (h MinHeap) Len() int           { return len(h) }
func (h MinHeap) Less(i, j int) bool { return h[i].Key < h[j].Key }
func (h MinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(IndexEntry))
}
func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type MaxHeap []IndexEntry

func (h MaxHeap) Len() int           { return len(h) }
func (h MaxHeap) Less(i, j int) bool { return h[i].Key > h[j].Key } // Reverse order for max heap
func (h MaxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *MaxHeap) Push(x interface{}) {
	*h = append(*h, x.(IndexEntry))
}
func (h *MaxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func NewBTreeIndex(dataFilename, encodedBTreeIndexFileFilename string) *BTreeIndex {

	btreeIndex := &BTreeIndex{
		dataFile:              dataFilename,
		encodedBTreeIndexFile: encodedBTreeIndexFileFilename,
		btree:                 nil, // Assign the BTree to the index field
	}
	btree, err := NewBTree() // Call NewBTree to get the BTree
	if err != nil {
		panic(fmt.Sprintf("Failed to create BTree: %v", err)) // Handle error (or return it)
	}
	btreeIndex.btree = btree
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
	branching_factor := 8 // This has to be greater than 2
	return &BTree{
		Root:                                nil, // The root is initially set to nil
		currentMemoryUsage:                  0,
		maxEntries:                          2*branching_factor - 1,
		minEntries:                          branching_factor - 1,
		indexFileToStoreInternalNodeEntries: createNewIndexFile(),
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
		}
	}

	// Clear the index files list
	indexFiles = []string{}

	// Reset the B-tree structure
	bti.btree.Root = nil
	bti.btree.currentMemoryUsage = 0

	fmt.Println("\nB-tree Index cleared.")
}

func (bti *BTreeIndex) StopIndexing() {
	bti.mu.Lock()

	// Persist the index to disk (if needed)
	err := bti.EncodeBTreeIndexToFile()
	bti.mu.Unlock()
	if err != nil {
		fmt.Println("Warning: Failed to save index:", err)
	} else {
		fmt.Println("BTree Index has been encoded to string and saved to file on disk successfully.")
	}
	// Clear the B-tree index from memory
	bti.ClearIndex()
}

// Checks if all entries in the index file are smaller than the given key.
func allEntriesAreSmallerThan(indexFile string, key string) bool {
	file, err := os.Open(indexFile)
	if err != nil {
		fmt.Printf("Warning: Could not open index file [%s]: %v\n", indexFile, err)
		return false
	}
	defer file.Close()
	var entry_no int
	entry_no = 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		entryKey := parts[0]

		//  If any entry is greater than or equal to the given key, return false.
		if entryKey > key {
			fmt.Printf(" Error: Entry No: [%d], Entry with key: [%s] in index file [%s] is not smaller than [%s]\n", entry_no, entryKey, indexFile, key)
			return false
		}
		if entryKey == key {
			fmt.Printf(" Error: Entry no:[%d] ,Entry with Key [%s] in index file [%s] is not equal to [%s]\n", entry_no, entryKey, indexFile, key)
			return false
		}
	}
	entry_no++
	if err := scanner.Err(); err != nil {
		fmt.Printf(" Warning: Error reading index file [%s]: %v\n", indexFile, err)
		return false
	}

	return true //  All entries are smaller
}

// Checks if all entries in the index file are greater than the given key.
func allEntriesAreGreaterThan(indexFile string, key string) bool {
	file, err := os.Open(indexFile)
	if err != nil {
		fmt.Printf(" Warning: Could not open index file [%s]: %v\n", indexFile, err)
		return false
	}
	defer file.Close()
	var entry_no int
	entry_no = 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		entryKey := parts[0]

		// If any entry is smaller than or equal to the given key, return false.
		if entryKey < key {
			fmt.Printf(" Error: Entry no:[%d] ,Entry with Key [%s] in index file [%s] is not greater than [%s]\n", entry_no, entryKey, indexFile, key)
			return false
		}
		if entryKey == key {
			fmt.Printf(" Error: Entry no:[%d] ,Entry with Key [%s] in index file [%s] is not equal to [%s]\n", entry_no, entryKey, indexFile, key)
			return false
		}
	}
	entry_no++
	if err := scanner.Err(); err != nil {
		fmt.Printf("Warning: Error reading index file [%s]: %v\n", indexFile, err)
		return false
	}

	return true // All entries are greater
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
			fmt.Printf(" Error: Entries are not sorted at node level! Found [%s] > [%s]\n",
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
					fmt.Printf(" Error: Left child key [%s] > Parent key [%s]\n", entry.Key, node.Entries[i].Key)
					return true
				}
			}
		}
		if rightChildNode != nil {
			for _, entry := range rightChildNode.Entries {
				if entry.Key < node.Entries[i].Key {
					fmt.Printf(" Error: Right child key [%s] < Parent key [%s]\n", entry.Key, node.Entries[i].Key)
					return true
				}
			}
		}
		if node.IsLeaf {
			leftIndexFile := node.Children[i].IndexFile
			rightIndexFile := node.Children[i+1].IndexFile
			// Read first key from leftIndexFile
			if leftIndexFile != "" {
				if !allEntriesAreSmallerThan(leftIndexFile, node.Entries[i].Key) {
					fmt.Printf(" Error:  key in left index file > Parent key [%s]\n", node.Entries[i].Key)
					return true
				}
			}
			// Read first key from rightIndexFile
			if rightIndexFile != "" {
				if !allEntriesAreGreaterThan(rightIndexFile, node.Entries[i].Key) {
					fmt.Printf(" Error:  key in right index file < Parent key [%s]\n", node.Entries[i].Key)
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
	// Atomically increment counter
	counter := atomic.AddUint64(&indexFileCounter, 1)

	// Generate a unique filename using both timestamp and counter
	// indexFile := fmt.Sprintf("index_%d_%d.db", time.Now().UnixNano(), counter)

	// Generate a unique filename using counter
	indexFile := fmt.Sprintf("index_file_%d.db", counter)

	// Create the file
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
	file.Seek(0, io.SeekEnd) // Move file pointer to the end before writing
	// Append the key and offset to the file
	_, err = file.WriteString(fmt.Sprintf("%s:%d\n", indexEntry.Key, indexEntry.FileOffset))
	if err != nil {
		return fmt.Sprintf("Failed: Could not write to index file: %v", err)
	}

	return fmt.Sprintf("Success: Key '%s' added to index file", indexEntry.Key)
}

func writeEntriesToFile(filename string, h heap.Interface) bool {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	defer file.Close()
	if err != nil {
		return false
	}

	for h.Len() > 0 {
		entry := heap.Pop(h).(IndexEntry)
		_, err := file.WriteString(fmt.Sprintf("%s:%d\n", entry.Key, entry.FileOffset))
		if err != nil {
			return false
		}
	}

	file.Sync()

	return true
}

func (bt *BTree) appendToIndexFileAndHandleIndexFileOverflow(indexFile string, indexEntry IndexEntry, leafNode *BTreeNode, indexOfChildPointerContainingIndexFile int) string {
	// Open the index file in read+write mode (to check for duplicate keys)
	file, err := os.OpenFile(indexFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Sprintf("Failed: Could not open index file: %v", err)
	}
	defer file.Close()

	maxHeap := &MaxHeap{}
	minHeap := &MinHeap{}
	*maxHeap = make(MaxHeap, 0)
	*minHeap = make(MinHeap, 0)
	heap.Init(maxHeap)
	heap.Init(minHeap)

	// Reset cursor to the beginning before scanning
	file.Seek(0, 0)
	number_of_entries := 0
	// Check if the key already exists in the file
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue // Skip malformed lines
		}
		number_of_entries++
		if parts[0] == indexEntry.Key {
			*maxHeap = (*maxHeap)[:0] // Clear maxHeap
			*minHeap = (*minHeap)[:0] // Clear minHeap
			return fmt.Sprintf("Failed: Key '%s' already exists in index file", indexEntry.Key)
		}

		offset, _ := strconv.ParseInt(parts[1], 10, 64)
		entry := IndexEntry{Key: parts[0], FileOffset: offset}

		heap.Push(maxHeap, entry)
		if minHeap.Len() > 0 && (*maxHeap)[0].Key > (*minHeap)[0].Key {
			heap.Push(minHeap, heap.Pop(maxHeap))
		}

		if maxHeap.Len() > minHeap.Len()+1 {
			heap.Push(minHeap, heap.Pop(maxHeap))
		}
		if minHeap.Len() > maxHeap.Len()+1 {
			heap.Push(maxHeap, heap.Pop(minHeap))
		}
	}
	if err := scanner.Err(); err != nil {
		*maxHeap = (*maxHeap)[:0] // Clear maxHeap
		*minHeap = (*minHeap)[:0] // Clear minHeap
		return fmt.Sprintf("Failed: Error reading index file: %v", err)
	}

	file.Seek(0, io.SeekEnd) // Move file pointer to the end before writing
	// Append the key and offset to the file
	_, err = file.WriteString(fmt.Sprintf("%s:%d\n", indexEntry.Key, indexEntry.FileOffset))
	if err != nil {
		*maxHeap = (*maxHeap)[:0] // Clear maxHeap
		*minHeap = (*minHeap)[:0] // Clear minHeap
		return fmt.Sprintf("Failed: Could not write to index file: %v", err)
	}

	heap.Push(maxHeap, indexEntry)
	if minHeap.Len() > 0 && (*maxHeap)[0].Key > (*minHeap)[0].Key {
		heap.Push(minHeap, heap.Pop(maxHeap))
	}

	if maxHeap.Len() > minHeap.Len()+1 {
		heap.Push(minHeap, heap.Pop(maxHeap))
	}
	if minHeap.Len() > maxHeap.Len()+1 {
		heap.Push(maxHeap, heap.Pop(minHeap))
	}

	// If index file had max entries before adding this entry, it should overflow by now since we added one more, so we split this overflowing indexfile
	if number_of_entries >= 10*bt.maxEntries {
		var medianEntry IndexEntry
		if number_of_entries%2 != 0 {
			if maxHeap.Len() > minHeap.Len() {
				medianEntry = heap.Pop(maxHeap).(IndexEntry)
			} else {
				medianEntry = heap.Pop(minHeap).(IndexEntry)
			}
		} else {
			medianEntry = heap.Pop(maxHeap).(IndexEntry)
		}

		// Update memory usage
		entrySize := len(medianEntry.Key) + 8 + 16 // Key length + int64 offset+ string overhead
		childPointerSize := 1 * 16                 // one new child pointers (each ~16 bytes)

		indexFileForKeyEntriesSmallerThanMedian := createNewIndexFile()
		indexFileForKeyEntriesGreaterThanMedian := createNewIndexFile()

		successfullyCreatedIndexFileEntriesForSmallerHalf := writeEntriesToFile(indexFileForKeyEntriesSmallerThanMedian, maxHeap)
		successfullyCreatedIndexFileEntriesForGreaterHalf := writeEntriesToFile(indexFileForKeyEntriesGreaterThanMedian, minHeap)

		if !successfullyCreatedIndexFileEntriesForSmallerHalf || !successfullyCreatedIndexFileEntriesForGreaterHalf {
			*maxHeap = (*maxHeap)[:0] // Clear maxHeap
			*minHeap = (*minHeap)[:0] // Clear minHeap
			return fmt.Sprintf("Failed: Error occured while splitting index file ")
		}

		// insert median into leaf node and update child pointers to point to these new index files
		newLeafNodeEntries := make([]IndexEntry, len(leafNode.Entries)+1)

		copy(newLeafNodeEntries[:indexOfChildPointerContainingIndexFile], leafNode.Entries[:indexOfChildPointerContainingIndexFile])
		newLeafNodeEntries[indexOfChildPointerContainingIndexFile] = medianEntry
		copy(newLeafNodeEntries[indexOfChildPointerContainingIndexFile+1:], leafNode.Entries[indexOfChildPointerContainingIndexFile:])
		leafNode.Entries = newLeafNodeEntries

		newLeafNodeChildrenEntries := make([]ChildPointer, len(leafNode.Children)+1)
		copy(newLeafNodeChildrenEntries[:indexOfChildPointerContainingIndexFile], leafNode.Children[:indexOfChildPointerContainingIndexFile])
		newLeafNodeChildrenEntries[indexOfChildPointerContainingIndexFile] = ChildPointer{ChildNode: nil, IndexFile: indexFileForKeyEntriesSmallerThanMedian}
		newLeafNodeChildrenEntries[indexOfChildPointerContainingIndexFile+1] = ChildPointer{ChildNode: nil, IndexFile: indexFileForKeyEntriesGreaterThanMedian}
		copy(newLeafNodeChildrenEntries[indexOfChildPointerContainingIndexFile+2:], leafNode.Children[indexOfChildPointerContainingIndexFile+1:])
		leafNode.Children = newLeafNodeChildrenEntries

		appendToIndexFile(bt.indexFileToStoreInternalNodeEntries, medianEntry, false)
		//update memory usage
		bt.currentMemoryUsage += int(entrySize + childPointerSize)

		os.Remove(indexFile)
		DeleteIndexFile(indexFile)
	}

	*maxHeap = (*maxHeap)[:0] // Clear maxHeap
	*minHeap = (*minHeap)[:0] // Clear minHeap
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
	if bt.Root == nil {
		// Tree is empty, create a new root
		bt.Root = &BTreeNode{
			Entries:  []IndexEntry{indexEntry},
			IsLeaf:   true,
			Children: make([]ChildPointer, 2), // Two child pointers for before and after the key
			Parent:   nil,
		}

		appendToIndexFile(bt.indexFileToStoreInternalNodeEntries, indexEntry, false)
		// Create two new index files for before and after the key
		beforeIndexFile := createNewIndexFile()
		afterIndexFile := createNewIndexFile()

		// Assign child pointers
		bt.Root.Children[0] = ChildPointer{IndexFile: beforeIndexFile}
		bt.Root.Children[1] = ChildPointer{IndexFile: afterIndexFile}
		// Update memory usage
		entrySize := len(indexEntry.Key) + 8 + 16 // Key length + int64 offset+ string overhead
		childPointerSize := 2 * 16                // Two child pointers (each ~16 bytes)
		bt.currentMemoryUsage += int(entrySize + childPointerSize)
		return fmt.Sprintf("Success: Key '%s' inserted", indexEntry.Key)
	}

	// Insert key into the appropriate node
	root := bt.Root
	if len(root.Entries) > bt.maxEntries {
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
		success, message, _ := bt.splitNode(newRoot, 0)
		if !success {
			return message
		}
	}

	return bt.insertNonFull(bt.Root, indexEntry)
}

// splitNode splits a full node into two nodes and moves the mid element from full node to the parent
func (bt *BTree) splitNode(parent *BTreeNode, index int) (bool, string, *BTreeNode) {
	//estimate memory usage
	childPointerSize := 1 * 16 // One new child pointer ( ~16 bytes)
	if bt.currentMemoryUsage+childPointerSize > MaxMemoryUsage {
		return false, "Failed: Memory limit exceeded", parent
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

	/*
		For propagating spliting operation upto parent if parent overflows which we are not doing as of now, unlike deletion, insertion can be forgiving of not maintaining tree balance
		at all times.
	*/

	if len(parent.Entries) > bt.maxEntries {
		if parent.Parent != nil {
			grandParent := parent.Parent
			for i, child := range grandParent.Children {
				if child.ChildNode == parent {
					return bt.splitNode(grandParent, i)
				}
			}
		} else {
			// Root is full, need to split
			newRoot := &BTreeNode{
				IsLeaf:   false,
				Children: make([]ChildPointer, 1),
				Parent:   nil,
			}

			// Point first child pointer to previous root
			newRoot.Children[0] = ChildPointer{ChildNode: bt.Root}
			bt.Root.Parent = newRoot
			bt.Root = newRoot
			return bt.splitNode(newRoot, 0)
		}
	}
	return true, "", parent
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
			if len(node.Entries) <= bt.maxEntries { // Assuming max of maxEntries keys per node before splitting
				//estimate memory usage
				entrySize := len(indexEntry.Key) + 8 + 16 // Key length + int64 offset + string overhead 16 bytes
				childPointerSize := 1 * 16                // One child pointer (each ~16 bytes)
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
							// return appendToIndexFile(indexFile, indexEntry, true)
							return bt.appendToIndexFileAndHandleIndexFileOverflow(indexFile, indexEntry, node, i)
						}
					}
					// Manually inserting `indexEntry` at the beginning of `node.Entries`
					newEntries := make([]IndexEntry, len(node.Entries)+1)
					newChildren := make([]ChildPointer, len(node.Children)+1)

					// Insert the new entry at the beginning
					newEntries[0] = indexEntry
					appendToIndexFile(bt.indexFileToStoreInternalNodeEntries, indexEntry, false)

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
							// return appendToIndexFile(indexFile, indexEntry, true)
							return bt.appendToIndexFileAndHandleIndexFileOverflow(indexFile, indexEntry, node, i)
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
					appendToIndexFile(bt.indexFileToStoreInternalNodeEntries, indexEntry, false)

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

		// return appendToIndexFile(indexFile, indexEntry, true)
		return bt.appendToIndexFileAndHandleIndexFileOverflow(indexFile, indexEntry, node, i)
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
	if len(child.Entries) > bt.maxEntries {
		success, message, nodeToRestartInsertProcedureFromAfterPropagatingSplitUpTheTree := bt.splitNode(node, i)
		if !success {
			return message
		}
		return bt.insertNonFull(nodeToRestartInsertProcedureFromAfterPropagatingSplitUpTheTree, indexEntry)
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

	msg := bti.Delete(key)
	if strings.HasPrefix(msg, "Failed") {
		return fmt.Sprintf("Failed: Delete failed with message:%s", msg)
	}
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
		return deleteFromIndexFile(node.Children[i].IndexFile, key)
	}

	// Case 2: If the node is an INTERNAL NODE
	if i < len(node.Entries) && node.Entries[i].Key == key {
		return bt.deleteInternalNodeEntry(node, i)
	}

	// Case 3: Key is in a subtree, recurse into the correct child
	if node.Children[i].ChildNode == nil {
		return fmt.Sprintf("Failed: Key '%s' not found in tree", key)
	}
	child := node.Children[i].ChildNode
	if len(child.Entries) < bt.minEntries {
		bt.fillUnderflow(node, i)
		return bt.deleteRecursive(bt.Root, key)
	}
	return bt.deleteRecursive(node.Children[i].ChildNode, key)
}

// Increases the number of entries in a node with less than min entries by 1
func (bt *BTree) fillUnderflow(parent *BTreeNode, index int) {
	underflowNode := parent.Children[index].ChildNode

	// Try borrowing from left sibling
	if index > 0 && len(parent.Children[index-1].ChildNode.Entries) > bt.minEntries {
		leftSibling := parent.Children[index-1].ChildNode

		// Move last key from leftSibling to parent
		newEntries := make([]IndexEntry, len(underflowNode.Entries)+1)
		newEntries[0] = parent.Entries[index-1]
		copy(newEntries[1:], underflowNode.Entries) // Shift child entries to the right
		underflowNode.Entries = newEntries

		// Move key from leftSibling to parent
		parent.Entries[index-1] = leftSibling.Entries[len(leftSibling.Entries)-1]
		newSiblingEntries := make([]IndexEntry, len(leftSibling.Entries)-1)
		copy(newSiblingEntries, leftSibling.Entries[:len(leftSibling.Entries)-1])
		leftSibling.Entries = newSiblingEntries

		// Move last child pointer from leftSibling to child
		newChildren := make([]ChildPointer, len(underflowNode.Children)+1)
		// Update child pointers
		if leftSibling.Children[len(leftSibling.Children)-1].ChildNode != nil {
			for _, child_pointer := range leftSibling.Children[len(leftSibling.Children)-1].ChildNode.Children {
				if child_pointer.ChildNode != nil {
					child_pointer.ChildNode.Parent = underflowNode
				}
			}
		}

		newChildren[0] = leftSibling.Children[len(leftSibling.Children)-1] // Move the last child
		copy(newChildren[1:], underflowNode.Children)                      // Shift existing child pointers to the right
		underflowNode.Children = newChildren

		// Remove the moved child pointer from leftSibling
		newSiblingChildren := make([]ChildPointer, len(leftSibling.Children)-1)
		copy(newSiblingChildren, leftSibling.Children[:len(leftSibling.Children)-1])
		leftSibling.Children = newSiblingChildren

		return
	}

	// Try borrowing from right sibling
	if index < len(parent.Children)-1 && len(parent.Children[index+1].ChildNode.Entries) > bt.minEntries {
		rightSibling := parent.Children[index+1].ChildNode

		// Move first key from rightSibling to parent
		newEntries := make([]IndexEntry, len(underflowNode.Entries)+1)
		copy(newEntries, underflowNode.Entries)                        // Copy existing entries
		newEntries[len(underflowNode.Entries)] = parent.Entries[index] // Append parent's entry
		underflowNode.Entries = newEntries

		// Move key from rightSibling to parent
		parent.Entries[index] = rightSibling.Entries[0]
		newSiblingEntries := make([]IndexEntry, len(rightSibling.Entries)-1)
		copy(newSiblingEntries, rightSibling.Entries[1:])
		rightSibling.Entries = newSiblingEntries

		// Move first child pointer from rightSibling to child
		newChildren := make([]ChildPointer, len(underflowNode.Children)+1)
		copy(newChildren, underflowNode.Children) // Copy existing child pointers
		// Update child pointers
		if rightSibling.Children[0].ChildNode != nil {
			for _, child_pointer := range rightSibling.Children[0].ChildNode.Children {
				if child_pointer.ChildNode != nil {
					child_pointer.ChildNode.Parent = underflowNode
				}
			}
		}
		newChildren[len(underflowNode.Children)] = rightSibling.Children[0] // Append right sibling's first child pointer
		underflowNode.Children = newChildren

		// Remove the moved child pointer from rightSibling
		newSiblingChildren := make([]ChildPointer, len(rightSibling.Children)-1)
		copy(newSiblingChildren, rightSibling.Children[1:])
		rightSibling.Children = newSiblingChildren

		return
	}

	// If neither sibling has extra keys, merge
	if index > 0 {
		bt.mergeNodes(parent, index-1)
	} else {
		bt.mergeNodes(parent, index)
	}
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
	originalEntry := node.Entries[index]
	// Try replacing with the predecessor from left index file
	if leftIndexFile != "" {
		predecessorKey, predecessorOffset, found := getMaxEntryFromIndexFile(leftIndexFile)
		if found {
			node.Entries[index] = IndexEntry{Key: predecessorKey, FileOffset: predecessorOffset}
			//update memory usage
			bt.currentMemoryUsage -= (len(originalEntry.Key) - len(predecessorKey))
			deleteFromIndexFile(leftIndexFile, predecessorKey)
			deleteFromIndexFile(bt.indexFileToStoreInternalNodeEntries, originalEntry.Key)
			return "Success: Replaced with predecessor and deleted from index file"
		}
	}

	// If left failed, try replacing with the successor from right index file
	if rightIndexFile != "" {
		successorKey, successorOffset, found := getMinEntryFromIndexFile(rightIndexFile)
		if found {
			node.Entries[index] = IndexEntry{Key: successorKey, FileOffset: successorOffset}
			//update memory usage
			bt.currentMemoryUsage -= (len(originalEntry.Key) - len(successorKey))
			deleteFromIndexFile(rightIndexFile, successorKey)
			deleteFromIndexFile(bt.indexFileToStoreInternalNodeEntries, originalEntry.Key)
			return "Success: Replaced with successor and deleted from index file"
		}
	}

	//  If neither index file had entries, check if this is the last entry in the node , which happens in the case of last remaining entry in root node
	if len(node.Entries) == 1 {
		node.Entries = nil
		node.Children = nil

		os.Remove(leftIndexFile)
		DeleteIndexFile(leftIndexFile)
		os.Remove(rightIndexFile)
		DeleteIndexFile(rightIndexFile)

		if node == bt.Root {
			bt.currentMemoryUsage = 0
			bt.Root = nil
		}
		deleteFromIndexFile(bt.indexFileToStoreInternalNodeEntries, originalEntry.Key)
		return "Success: Last Entry deleted, tree is empty"
	}

	//  If node has more than one entry, remove the entry and  the left Child Pointer and delete the index file in it

	//estimate memory saved
	entrySize := len(node.Entries[index].Key) + 8 + 16 // Key length + int64 offset + string overhead 16 bytes
	childPointerSize := 1 * 16                         // One child pointer (each ~16 bytes)

	newEntries := make([]IndexEntry, len(node.Entries)-1)
	copy(newEntries, node.Entries[:index])           // Copy elements before index
	copy(newEntries[index:], node.Entries[index+1:]) // Copy elements after index
	node.Entries = newEntries

	newChildren := make([]ChildPointer, len(node.Children)-1)
	copy(newChildren, node.Children[:index])           // Copy elements before index
	copy(newChildren[index:], node.Children[index+1:]) // Copy elements after index
	node.Children = newChildren

	os.Remove(leftIndexFile)
	DeleteIndexFile(leftIndexFile)
	// update memory usage
	bt.currentMemoryUsage -= (entrySize + childPointerSize)

	//  Check for underflow and handle it
	if len(node.Entries) < bt.minEntries && node != bt.Root {
		parent := node.Parent
		for i, child := range parent.Children {
			if child.ChildNode == node {
				bt.fillUnderflow(parent, i)
				break
			}
		}
	}

	deleteFromIndexFile(bt.indexFileToStoreInternalNodeEntries, originalEntry.Key)
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

func (bt *BTree) deleteInternalNodeEntry(node *BTreeNode, index int) string {
	if node.IsLeaf {
		// Should never reach here because internal nodes are not leaves
		return "Failed: Internal Node Entry deletion failed as node was leaf"
	}

	leftChild := node.Children[index].ChildNode
	rightChild := node.Children[index+1].ChildNode
	originalEntry := node.Entries[index]
	// Case 1.1: Left child has extra keys, replace with predecessor
	if len(leftChild.Entries) >= bt.minEntries {
		predecessor := bt.getPredecessor(leftChild)
		node.Entries[index] = predecessor
		//update memory usage
		bt.currentMemoryUsage += len(predecessor.Key) - len(node.Entries[index].Key) // -deleted key + predecessory key
		deleteFromIndexFile(bt.indexFileToStoreInternalNodeEntries, originalEntry.Key)
		return bt.deleteRecursive(leftChild, predecessor.Key)
	}

	// Case 1.2: Right child has extra keys, replace with successor
	if len(rightChild.Entries) >= bt.minEntries {
		successor := bt.getSuccessor(rightChild)
		node.Entries[index] = successor
		//update memory usage
		bt.currentMemoryUsage += len(successor.Key) - len(node.Entries[index].Key) // -deleted key + successor key
		deleteFromIndexFile(bt.indexFileToStoreInternalNodeEntries, originalEntry.Key)
		return bt.deleteRecursive(rightChild, successor.Key)
	}

	// Case 1.3: Merge left and right child
	key := node.Entries[index].Key
	mergedNode := bt.mergeNodes(node, index)
	return bt.deleteRecursive(mergedNode, key)
}

func deleteFromIndexFile(indexFile string, key string) string {
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

		_, err := writer.WriteString(line + "\n")
		if err != nil {
			tempFile.Close()
			os.Remove(indexFile + ".tmp")
			return fmt.Sprintf("Failed: Error writing to temp file: %v", err)
		}
		writer.Flush()
	}

	writer.Flush()
	tempFile.Close()
	file.Close()

	// Replace old file with temp file
	if err := os.Rename(indexFile+".tmp", indexFile); err != nil {
		return fmt.Sprintf("Failed: Could not replace index file: %v", err)
	}

	if keyFound {
		return fmt.Sprintf("Success: Key '%s' deleted from index file", key)
	}
	return fmt.Sprintf("Failed: Key '%s' not found in the index file", key)
}

// Merge a parent node's two children and move a parent's entry down to merged node
func (bt *BTree) mergeNodes(parent *BTreeNode, index int) *BTreeNode {
	left := parent.Children[index].ChildNode
	right := parent.Children[index+1].ChildNode

	//estimate memory saved
	childPointerSize := 1 * 16 // One child pointer (each ~16 bytes)
	// Merge parent entry into left
	mergedEntries := make([]IndexEntry, len(left.Entries)+1+len(right.Entries))
	copy(mergedEntries, left.Entries)
	mergedEntries[len(left.Entries)] = parent.Entries[index]
	copy(mergedEntries[len(left.Entries)+1:], right.Entries)

	// Merge children
	mergedChildren := make([]ChildPointer, len(left.Children)+len(right.Children))
	copy(mergedChildren, left.Children)
	copy(mergedChildren[len(left.Children):], right.Children)

	mergedNode := &BTreeNode{
		Entries:  mergedEntries,
		Children: mergedChildren,
		IsLeaf:   left.IsLeaf,
		Parent:   parent,
	}

	// Update child pointers
	for _, child := range mergedNode.Children {
		if child.ChildNode != nil {
			child.ChildNode.Parent = mergedNode
		}
	}

	// Replace leftChild with mergedNode in parent.Children
	parent.Children[index].ChildNode = mergedNode

	// Update parent by removing rightChild and its entry
	newEntries := make([]IndexEntry, len(parent.Entries)-1)
	copy(newEntries[:index], parent.Entries[:index])
	copy(newEntries[index:], parent.Entries[index+1:])

	newChildren := make([]ChildPointer, len(parent.Children)-1)
	copy(newChildren[:index+1], parent.Children[:index+1])
	copy(newChildren[index+1:], parent.Children[index+2:])

	parent.Entries = newEntries
	parent.Children = newChildren

	// update memory usage
	bt.currentMemoryUsage -= childPointerSize
	// If parent becomes empty, update root
	if parent == bt.Root && len(parent.Entries) == 0 {
		bt.Root = mergedNode
		bt.Root.Parent = nil
	}

	// If merged node's parent is now underflowing, and mergeNode is not root (if it was, mergeNode.parent would have been nil) fix underflow
	if mergedNode.Parent != nil && len(mergedNode.Parent.Entries) < bt.minEntries {
		grandParent := mergedNode.Parent
		for i, child := range grandParent.Children {
			if child.ChildNode == parent {
				bt.fillUnderflow(grandParent, i)
				break
			}
		}
	}

	return mergedNode

}

func (bt *BTree) getPredecessor(node *BTreeNode) IndexEntry {
	// Move to the rightmost child of the left subtree
	curr := node
	for !curr.IsLeaf {
		curr = curr.Children[len(curr.Children)-1].ChildNode
	}
	// look for max entry in right most index file
	predecessorKey, predecessorOffset, found := getMaxEntryFromIndexFile(curr.Children[len(curr.Children)-1].IndexFile)
	if found {
		return IndexEntry{Key: predecessorKey, FileOffset: predecessorOffset}
	}
	// Return the rightmost key in this leaf node
	return curr.Entries[len(curr.Entries)-1]
}

func (bt *BTree) getSuccessor(node *BTreeNode) IndexEntry {
	// Move to the leftmost child of the right subtree
	curr := node
	for !curr.IsLeaf {
		curr = curr.Children[0].ChildNode
	}
	// look for min entry in left most index file
	successorKey, successorOffset, found := getMinEntryFromIndexFile(curr.Children[0].IndexFile)
	if found {
		return IndexEntry{Key: successorKey, FileOffset: successorOffset}
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
							entrySize := len(parts[0]) + 8 + 16 // Key length + int64 offset + string overhead 16 bytes
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

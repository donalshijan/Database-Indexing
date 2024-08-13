package btree

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Define the B-tree structure
type Node struct {
	Entries    []IndexEntry
	Children   []*Node
	IsLeaf     bool
	Parent     *Node
	FileOffset int // Offset in the file for this node
}

// Define the B-tree structure
type BTree struct {
	Root *Node
}
type IndexEntry struct {
	Key   string
	Value int
}

// NewBTree creates a new B-tree with an empty root
func NewBTree() (*BTree, error) {
	return &BTree{
		Root: nil, // The root is initially set to nil
	}, nil
}

// Insert adds a new key to the B-tree
func (bt *BTree) Insert(indexEntry IndexEntry) {
	if bt.Root == nil {
		// Tree is empty, create a new root
		bt.Root = &Node{
			Entries:  []IndexEntry{indexEntry},
			IsLeaf:   true,
			Children: nil,
		}
		return
	}

	// Insert key into the appropriate node
	root := bt.Root
	if len(root.Entries) == 4 {
		// Root is full, need to split
		newRoot := &Node{
			IsLeaf:   false,
			Children: []*Node{root},
		}
		bt.Root = newRoot
		bt.splitNode(newRoot, 0)
	}

	bt.insertNonFull(bt.Root, indexEntry)
}

// splitNode splits a full node into two nodes and adjusts the parent
func (bt *BTree) splitNode(parent *Node, index int) {
	fullNode := parent.Children[index]
	midIndex := len(fullNode.Entries) / 2
	midKey := fullNode.Entries[midIndex].Key
	midValue := fullNode.Entries[midIndex].Value

	midElement := IndexEntry{
		Key:   midKey,
		Value: midValue,
	}

	// Create a new node for the split
	newNode := &Node{
		Entries:  fullNode.Entries[midIndex+1:],
		Children: fullNode.Children[midIndex+1:],
		IsLeaf:   fullNode.IsLeaf,
		Parent:   parent,
	}

	// Adjust the original node
	fullNode.Entries = fullNode.Entries[:midIndex]
	fullNode.Children = fullNode.Children[:midIndex+1]

	// Insert the new node into the parent
	parent.Entries = append(parent.Entries[:index], append([]IndexEntry{midElement}, parent.Entries[index:]...)...)
	parent.Children = append(parent.Children[:index+1], append([]*Node{newNode}, parent.Children[index+1:]...)...)
}

// insertNonFull inserts a key into a node that is not full
func (bt *BTree) insertNonFull(node *Node, indexEntry IndexEntry) {
	if node.IsLeaf {
		// Insert into a leaf node
		i := 0
		for i < len(node.Entries) && node.Entries[i].Key < indexEntry.Key {
			i++
		}
		node.Entries = append(node.Entries[:i+1], node.Entries[i:]...)
		node.Entries[i] = indexEntry

	} else {
		// Find the child node where the key should go
		i := 0
		for i < len(node.Entries) && node.Entries[i].Key < indexEntry.Key {
			i++
		}
		child := node.Children[i]
		if len(child.Entries) == 4 {
			// Child is full, need to split
			bt.splitNode(node, i)
			if indexEntry.Key > node.Entries[i].Key {
				child = node.Children[i+1]
			}
		}
		bt.insertNonFull(child, indexEntry)
	}
}

// traverse recursively visits each node and prints keys
func (bt *BTree) Traverse() {
	if bt.Root != nil {
		bt.traverseNode(bt.Root)
	}
}

func (bt *BTree) traverseNode(node *Node) {
	// Traverse the subtree rooted at this node
	for i := 0; i < len(node.Entries); i++ {
		if !node.IsLeaf {
			bt.traverseNode(node.Children[i])
		}
		fmt.Printf("%s,%d", node.Entries[i].Key, node.Entries[i].Value)
	}
	if !node.IsLeaf {
		bt.traverseNode(node.Children[len(node.Entries)])
	}
}

// search recursively searches for a key in the B-tree
func (bt *BTree) Search(key string) int {
	return bt.searchNode(bt.Root, key)
}

func (bt *BTree) searchNode(node *Node, key string) int {
	i := 0
	for i < len(node.Entries) && key > node.Entries[i].Key {
		i++
	}
	if i < len(node.Entries) && node.Entries[i].Key == key {
		return node.Entries[i].Value
	}
	if node.IsLeaf {
		return -1
	}
	return bt.searchNode(node.Children[i], key)
}

func LoadTree(filename string, bt *BTree) (*BTree, error) {
	file, err := os.Open(filename)
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
	valueInt, err := strconv.Atoi(value)
	if err != nil {
		return bt, err
	}
	node := IndexEntry{
		Key:   key,
		Value: valueInt,
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
	iCurly := strings.Index(s, "{")
	iClose := strings.Index(s, "}")

	// Determine the smallest index (the first occurrence)
	if iCurly == -1 && iClose == -1 {
		return s, "" // No delimiters found
	} else if iCurly == -1 {
		return s[:iClose], s[iClose:]
	} else if iClose == -1 {
		return s[:iCurly], s[iCurly:]
	} else if iCurly < iClose {
		return s[:iCurly], s[iCurly:]
	} else {
		return s[:iClose], s[iClose:]
	}
}

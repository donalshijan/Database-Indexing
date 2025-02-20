# Project Overview

Project implements and compares multiple database indexing techniques, and attempts to make improvements along further iterations for better performance and indexing capacity.
As of now it implements a Hash Indexed and BTree Indexed database, and we have tests to measure performance of various operations and indexing capacity.

## Hash Index

Every data we wish to store in the DB will have a unique key, and the data associated with that key gets stored into datafile, appended at the end of the file as the new entry.
When data gets appended to data file , the offset at which it got appended is noted, with that we have the both the key associated with the data and the offset in the datafile at which the data is actually located.
Hash map is used to map every data's Key to it's offset in datafile. This `HashMap<key=>Offset>` obviously resides in memory while the data itself is on file stored on disk.
The most important feature of a database, is persistance, that is what makes a db a db. So to guarantee persistence, we need to introduce index file, the whole purpose of index file is to maintain a copy of the hashmap in a file on disk, to make sure even if the database program crashes, the Index still persists on disk albeit as a file, but it can easily be used to reconstruct the Hash Index on restart.
For every insert operation, after the data gets appended to the data file, it's offset is entered into the HashMap at it's key. Immediately afterwards the index file also appends a new entry as `Key:Offset` for this insertion in the index file.
For search operation, we lookup the offset for a key in the hashmap and retrieve the data from the datafile at that offset. For deletion we will move all the data to a temporary datafile skipping over the data to be removed, and then rename the temporary data file as the new datafile, then remove that key from hashmap and also remove that key and offset entry from the index file the same way as we did for datafile by creating a new temporary index file and copying all but the entry with that key, and make this temporary index file the new index file. This is not the most efficient way of doing this, as there are many strategies to improve upon this , which will be explored later. Updation is done by deletion followed by insertion of new value.

## BTree Index

Data is stored in datafile on disk, and it's key and offset in datafile together form `Key:Offset` an entry in the btree. Child Pointers of internal nodes in the tree point to other nodes, whereas the child pointer's of leaf nodes point to index files which also stores multiple `Key:Offset` entries in it. Tree grows as more entries are made and shrinks as they get removed but most of the entries are still stored in the index files pointed by leaf nodes, which is why it is able to Index more entries than Hash Index.
All entries in a node are sorted by key and every entry in the left subtree of an entry is smaller than the entry and every entry in the right subtree of an entry is larger than the entry in that node.
When searching for  Key in the tree, we follow same search logic as we would on binary search tree as all entries in a node are sorted, if we find the key we were looking for in the current node we will retrive data from the datafile at the offset stored along with that key in the entry, if not we will recurse into the right sub tree and continue our search again, when we reach leaf node, we can either have the key as an entry in the node itself if not then it has to be in the index file pointed to by the child pointer for the appropriate range where the key should fall under, if we don't find the key entry in that index file, we return saying key does not exist.
Updation is done by deletion followed by insertion with new value.

## How to Run

To run the program install go and run 

`go mod tidy`
`go mod verify`
using environment.env file as example create a new .env file and populate it accordingly, for hash index and btree index we need to have two files a data file and and index file, create these files, you can name it as you want. 
When finally running the executable we need to pass an argument, either `hash` or `btree` , depending on which type of index you want use.
`go run main.go hash` for running hash indexed DB.
`go run main.go btree` for running btree indexed DB.
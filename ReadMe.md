# Project Overview

Project implements and compares multiple database indexing techniques, and attempts to make improvements for better performance and indexing capacity.
As of now it implements a Hash Indexed and BTree Indexed database, and we have tests to measure performance of various operations and indexing capacity.

## Hash Index

Every data we wish to store in the DB will have a unique key, and the data associated with that key gets stored into data file, appended at the end of the file as a new entry.
When data gets appended to data file , the offset at which it got appended is noted, with that we have both the key associated with the data and the offset in the data file at which the data is actually located.
Hash map is used to map every data's Key to it's offset in data file. This `HashMap<key=>Offset>` obviously resides in memory while the data itself is on file stored on disk.
The most important feature of a database, is persistance, that is what makes a db a db. So to guarantee persistence, we need to introduce index file, the whole purpose of index file in the case of hash index is to maintain a copy of the hashmap in a file on disk, to make sure even if the database program crashes, the Index still persists on disk albeit as a file, but it can easily be used to reconstruct the Hash Index on restart, by inserting every entry in the file to the a new HashIndex. Write Ahead logs can be used to avoid maintaining copy of HashMap entries of HashIndexed DB on disk as a file altogether and achieve persistence in a much more elegant way, but we will leave that for later iterations.
For every insert operation, after the data gets appended to the data file, it's offset is entered into the HashMap at it's key. Immediately afterwards the index file also appends a new entry as `Key:Offset` for this insertion in the index file.
For search operation, we lookup the offset for a key in the hashmap and retrieve the data from the data file at that offset. For deletion, we remove that key from hashmap and also remove that key and offset entry from the index file by creating a new temporary index file and copying all but the entry with that key, and make this temporary index file the new index file. This is not the most efficient way of doing this, as there are many strategies to improve upon this , which will be explored later. Updation is done by deletion followed by insertion of new value.

## BTree Index

Data is stored in data file on disk, and it's key and offset in data file together form `Key:Offset` an entry in the btree. Child Pointers of internal nodes in the tree point to other nodes, whereas the child pointer's of leaf nodes point to index files which also stores multiple `Key:Offset` entries in it. Tree grows as more entries are made and shrinks as they get removed but most of the entries are still stored on disk in the index files pointed by leaf nodes, which is why it is able to Index more entries than Hash Index.
All entries in a node are sorted by key and every entry in the left subtree of an entry in a node is smaller than the entry in the node and every entry in the right subtree of an entry in a node is larger than the entry in the node. 
When searching for a key in the tree, we follow same search logic as we would on binary search tree as all entries in a node are sorted, if we find the key we were looking for in the current node, we will retrieve data from the data file at the offset stored along with that key in the entry, if not we will recurse into the appropriate sub tree and continue our search again, when we reach leaf node, we can either have the key as an entry in the node itself, if not, then it has to be in the index file pointed by the child pointer for the appropriate range where the key should fall under, if we don't find the key's entry in that index file either, we return saying key does not exist or not found.
Updation is done by deletion followed by insertion with new value.

This implementation of BTree index is a non preemptive implementation or also known as lazy implementation, because in this implementation, merge and split operations which rebalances the tree is done respectively only when overflow and underflow condition is detected while traversing down the tree in attempt to insert a new entry or delete an entry.
In preemptive implementation also knows as eager implementation, this is done as soon as an overflow or underflow occurs immediately after insertion or deletion respectively, which ensures every operation when completed leaves the tree in a balanced state, whereas the non preemptive implementation only balances after constraint violation or imbalances get detected when traversing down the tree for the next operation. Essentially what this means is that, we could leave the tree in an imbalanced or constraint violated state when we finish an operation, and it will only get picked up for correction in the next operation down that same path, as in the one which traverses down that same path where violation or imbalance occured previously, hence it is called lazy implementation as it often leaves the tree imbalanced and balances less frequently and partially as compared to eager implementation, which always leaves the whole tree balanced after every operation.
This BTree implementation works only for branching factors greater than 2.

Since, data is stored first in the data file, before inserting the `Key:Offset` entry for that data into the BTreeIndex, data always persists on disk by default.
All entries stored in index files pointed by leaf node's child pointers, will also persist on disk even when program crashes, for internal nodes, as a new entry gets added or removed to any internal node, that entry is also made to a separate index file which contains all entries of all internal nodes. In the event of a crash, you can reconstruct the index by inserting each entry from all these index files into the new BtreeIndex one by one.
If program doesn't crash, and exits gracefully, when indexing is stopped, it encodes the entire Btree index and all entries in it's internal nodes,leaf nodes and index files using a custom encoding scheme and stores it all into a single file (`encodedBTreeIndexFile`), later when the program restarts you can pass this file as the index file parameter, and the program will parse and decode this file using custom decoding scheme (inverse of encoding scheme) and reconstruct the original BTreeIndex as before, using this file and ofcourse the data file.

## How to Start

After cloning the repo

To run the program install go and run 

    go mod tidy

    go mod verify

using environment.env file as example create a new .env file and populate it accordingly, for hash index and btree index, each of them requires to have two files a data file and an index file, create these files ( you can name it as you want, ideally with .db extension), when running the program to set up a db for the first time.

Every other time when you restart after exiting gracefully, you can pass in the same index and data file that was used when running the program previously, as they would have been populated to reflect the data storage and index by now, and the program will reconstruct the DB Index in the state it was before exiting.

Everytime we restart the program after gracefully terminating in previous run, if we intend to rebuild and continue with the index where we left of, we pass in the same data file and index file that we did before, obviously they would have been populated to reflect all data storage that happened until before exiting and the state of the Index, In which case,   Data file in the case of both Hash and BTree Index holds the data, index file in the case of hash index will maintain a copy of the hashmap as it was before exiting, whereas the index file in the case of BTree index will contain the entire BTreeIndex including entries form internal nodes, leaf nodes and index files pointed to by leaf nodes, encoded and stored into one single file.

If we wish to build a new index instead of reconstructing and continuing with a previously built and used index, create a new data file and index file and pass them as the parameter.

We don't pass anything directly, all files which we need, their path is supposed to be exposed in the .env file, the program will fetch the file at the file path associated with that environment variable.

Finally when running the program we need to pass an argument, either `hash` or `btree` , depending on which type of index you want to use.

    `go run main.go hash` for running hash indexed DB.
    `go run main.go btree` for running btree indexed DB.

The CLI will guide you with the command usage for interaction.

When running the program in btree index mode, if the program crashes while running tests, then to clean up all those index files generated by test manually, would be a pain, so to help with that, a script thas been provided to delete all those index files. (Hopefully, you will never need this)

To delete index files generated when running test using script 

    chmod +x delete_index_files.sh

    ./delete_index_files.sh


## 💖 Support This Project
If you like this project, you can support me:

- 💸 [PayPal](https://paypal.me/donalshijan)
- ☕ [Buy Me a Coffee](https://buymeacoffee.com/donalshijan)


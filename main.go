package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"database_indexing/btreeindex"
	"database_indexing/database"
	"database_indexing/hashindex"
	"database_indexing/wal"

	"github.com/briandowns/spinner"
	"github.com/joho/godotenv"
	"github.com/schollz/progressbar/v3"
)

// Load environment variables from .env file
func loadEnv(dbType string) (string, string, string) {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	var dataFile string

	var indexFile string
	if dbType == "btree" {
		dataFile = os.Getenv("BTREE_INDEXED_DB_DATA_FILE")
		indexFile = os.Getenv("BTREE_INDEXED_DB_ENCODED_BTREE_INDEX_FILE")
	} else if dbType == "hash" {
		dataFile = os.Getenv("HASH_INDEXED_DB_DATA_FILE")
		indexFile = os.Getenv("HASH_INDEXED_DB_INDEX_FILE")
	} else {
		log.Fatal("Invalid database type. Use 'hash' or 'btree'")
	}
	wal_dir := os.Getenv("WAL_DIR_PATH")
	if dataFile == "" || indexFile == "" || wal_dir == "" {
		log.Fatal("Required environment variables are missing in .env file")
	}

	return dataFile, indexFile, wal_dir
}

// Generate a random 8-letter word
func randomWord() string {
	letters := []rune("abcdefghijklmnopqrstuvwxyz")
	word := make([]rune, 8)
	for i := range word {
		word[i] = letters[rand.Intn(len(letters))]
	}
	return string(word)
}

func clearDbFiles(dataFile, indexFile string) {
	// Clear the data file
	err := os.Truncate(dataFile, 0)
	if err != nil {
		log.Fatalf("Failed to clear data file: %v", err)
	}

	// Clear the index file
	err = os.Truncate(indexFile, 0)
	if err != nil {
		log.Fatalf("Failed to clear index file: %v", err)
	}

	// log.Println("Data and index files cleared.")
}

// Utility function to create empty files
func createEmptyFile(filename string) {
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Failed to create file %s: %v", filename, err)
	}
	file.Close()
}

// Performance test for insert, search, update, delete
func performanceTest(indexType string) {
	// Define temporary file names
	tempDataFile := "temp_data.db"
	tempIndexFile := "temp_index.db"

	// Remove any existing test files before starting
	os.Remove(tempDataFile)
	os.Remove(tempIndexFile)

	// Ensure the temp files are created before usage
	createEmptyFile(tempDataFile)
	createEmptyFile(tempIndexFile)

	var db database.Database

	// Initialize the appropriate index type
	switch indexType {
	case "hash":
		db = hashindex.NewHashIndex(tempDataFile, tempIndexFile)
		fmt.Println("Using Hash Index Database")

	case "btree":
		db = btreeindex.NewBTreeIndex(tempDataFile, tempIndexFile)
		fmt.Println("Using BTree Index Database")

	default:
		fmt.Println("Invalid index type. Use 'hash' or 'btree'")
		return
	}
	logFile, err := os.OpenFile("test_logs.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("Error opening log file")
	}
	defer logFile.Close()
	// Clear the contents of the log file before writing new test result logs
	err = os.Truncate("test_logs.log", 0)
	if err != nil {
		log.Fatalf("Failed to clear log file: %v", err)
	}
	logger := log.New(logFile, "", log.LstdFlags)

	logger.Println("=======================================")
	logger.Printf("     TEST RESULTS FOR %s INDEXING      ", strings.ToUpper(indexType))
	logger.Println("=======================================")

	numInserts := 2000
	numOps := 2000
	var insertTimes, searchTimes, updateTimes, deleteTimes []time.Duration
	keys := make([]string, numInserts)
	keyValueMap := make(map[string]string) // Store key-value pairs for validation

	// INSERT TEST
	fmt.Printf("\nRunning Insert Test\n")
	insertBar := progressbar.NewOptions(numInserts, progressbar.OptionSetWriter(os.Stderr), progressbar.OptionSetDescription("Inserting"))
	for i := 0; i < numInserts; i++ {
		key := randomWord() // Generate random key
		value := randomWord()
		start := time.Now()
		msg := db.Insert(key, value)
		insertTimes = append(insertTimes, time.Since(start))
		keys[i] = key
		keyValueMap[key] = value // Store key-value pair

		if strings.HasPrefix(msg, "Failed") {
			// Clean up: Remove the temporary database files before panicking
			db.ClearIndex()
			os.Remove(tempDataFile)
			os.Remove(tempIndexFile)
			fmt.Printf("Insert failed: %s\n", msg)
			panic(msg)
		}

		insertBar.Add(1)
	}
	avgInsertTime := averageTime(insertTimes)
	fmt.Printf("Average Insert Time: %v\n", avgInsertTime)
	logger.Printf("Average Insert Time: %v\n", avgInsertTime)

	// SEARCH TEST
	fmt.Printf("\nRunning Search Test\n")
	searchBar := progressbar.NewOptions(numOps, progressbar.OptionSetWriter(os.Stderr), progressbar.OptionSetDescription("Searching"))
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	for i := 0; i < numOps; i++ {
		start := time.Now()
		value, found := db.Search(keys[i])
		searchTimes = append(searchTimes, time.Since(start))

		// Validate search result
		expectedValue, exists := keyValueMap[keys[i]]
		if !found || !exists || value != expectedValue {
			fmt.Printf("Search failed: key=%s, expected=%s, got=%s\n", keys[i], expectedValue, value)
			continue
		}

		searchBar.Add(1)
	}
	avgSearchTime := averageTime(searchTimes)
	fmt.Printf("Average Search Time: %v\n", avgSearchTime)
	logger.Printf("Average Search Time: %v\n", avgSearchTime)

	// UPDATE TEST
	fmt.Printf("\nRunning Update Test\n")
	updateBar := progressbar.NewOptions(numOps, progressbar.OptionSetWriter(os.Stderr), progressbar.OptionSetDescription("Updating"))
	for i := 0; i < numOps; i++ {
		start := time.Now()
		msg := db.Update(keys[i], randomWord())
		updateTimes = append(updateTimes, time.Since(start))
		if strings.HasPrefix(msg, "Failed") {
			// Clean up: Remove the temporary database files before panicking
			db.ClearIndex()
			os.Remove(tempDataFile)
			os.Remove(tempIndexFile)
			fmt.Printf("Update failed: %s\n", msg)
			panic(msg)
		}
		updateBar.Add(1)
	}
	avgUpdateTime := averageTime(updateTimes)
	fmt.Printf("Average Update Time: %v\n", avgUpdateTime)
	logger.Printf("Average Update Time: %v\n", avgUpdateTime)

	// DELETE TEST
	fmt.Printf("\nRunning Delete Test\n")
	deleteBar := progressbar.NewOptions(numOps, progressbar.OptionSetWriter(os.Stderr), progressbar.OptionSetDescription("Deleting"))
	for i := 0; i < numOps; i++ {
		start := time.Now()
		msg := db.Delete(keys[i])
		deleteTimes = append(deleteTimes, time.Since(start))
		if strings.HasPrefix(msg, "Failed") {
			// Clean up: Remove the temporary database files before panicking
			db.ClearIndex()
			os.Remove(tempDataFile)
			os.Remove(tempIndexFile)
			fmt.Printf("Delete failed: %s\n", msg)
			panic(msg)
		}
		deleteBar.Add(1)
	}
	avgDeleteTime := averageTime(deleteTimes)
	fmt.Printf("Average Delete Time: %v\n", avgDeleteTime)
	logger.Printf("Average Delete Time: %v\n", avgDeleteTime)

	// INDEXING CAPACITY TEST
	fmt.Printf("\nRunning Indexing Capacity Test\n")
	fmt.Print("Clearing test DB Index and reinitializing to run indexing capacity test.")
	db.ClearIndex()
	clearDbFiles(tempDataFile, tempIndexFile)
	fmt.Println("Cleared test DB Index to run indexing capacity test.")
	count := 0
	// Initialize the spinner with a rotating effect
	s := spinner.New(spinner.CharSets[14], 100*time.Millisecond) // Use character set 14 with 100ms update speed
	s.Suffix = fmt.Sprintf("  Entries Indexed: %d", count)
	s.Start()
	for {
		key := randomWord()
		value := randomWord()

		// Try inserting
		result := db.Insert(key, value)

		// Stop if memory limit is hit
		if strings.Contains(result, "Failed: Memory limit exceeded") {
			s.Stop()
			fmt.Print("\r✔ ") // Overwrite spinner with a final completion checkmark
			fmt.Printf("Indexing capacity: %d entries indexed in %.2f MB memory\n", count, float64(db.GetMaxMemoryUsage())/(1024*1024))
			logger.Printf("Indexing capacity : %d entries indexed in %.2f MB memory \n", count, float64(db.GetMaxMemoryUsage())/(1024*1024))
			break
		}
		if strings.HasPrefix(result, "Failed") && !strings.Contains(result, "Failed: Memory limit exceeded") {
			continue
		}
		count++
		s.Suffix = fmt.Sprintf("  Entries Indexed: %d", count)
	}
	// Clean up: Remove the temporary database files
	fmt.Print("\nClearing test DB Index.")
	db.ClearIndex()
	os.Remove(tempDataFile)
	os.Remove(tempIndexFile)
	time.Sleep(1 * time.Second)
	fmt.Println("\nPerformance test completed. Results saved to test_logs.log")

}

// Helper function to calculate average time
func averageTime(times []time.Duration) time.Duration {
	var total time.Duration
	for _, t := range times {
		total += t
	}
	return total / time.Duration(len(times))
}

func replayWALFileEntriesAndReconstructIndex(db database.Database, wal *wal.WAL) {

	// Read all WAL entries and replay them into the database
	entries, err := wal.ReadAll()
	if err != nil {
		fmt.Println("Error reading WAL entries:", err)
		return
	}

	for _, entry := range entries {

		// Assume entries follow format: "operation key value"
		parts := strings.SplitN(string(entry), "|", 3)
		if len(parts) < 2 {
			fmt.Println("Invalid entry, skipping:", string(entry))
			continue
		}

		command, key := parts[0], parts[1]
		switch command {
		case "insert":
			if len(parts) < 3 {
				fmt.Println("Invalid insert entry, skipping:", string(entry))
				continue
			}
			value := parts[2]
			db.Insert(key, value)
		case "delete":
			db.Delete(key)
		case "update":
			if len(parts) < 3 {
				fmt.Println("Invalid update entry, skipping:", string(entry))
				continue
			}
			value := parts[2]
			db.Update(key, value)
		default:
			fmt.Println("Unknown WAL operation, skipping:", string(entry))
		}
	}

	fmt.Println("WAL replay completed. Index reconstructed successfully.")
}

func setupWAL(walDir string) (*wal.WAL, error) {
	// Check if metadata file exists
	metadataPath := walDir + "/wal_metadata.json"
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		fmt.Println("No WAL metadata file found, starting new WAL")
		return wal.NewWAL(walDir)
	}

	fmt.Println("WAL metadata found, loading previous WAL")
	return wal.LoadPrevWAL(walDir)
}

func main() {

	// Check if index type is provided as an argument
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <index_type>")
		fmt.Println("index_type: 'hash' or 'btree'")
		return
	}

	indexType := strings.ToLower(os.Args[1])

	var db database.Database
	var WAL_DIR_PATH string
	// Initialize the appropriate index type
	switch indexType {
	case "hash":
		// Load file paths from .env
		dataFile, indexFile, walDir := loadEnv(indexType)
		db = hashindex.NewHashIndex(dataFile, indexFile)
		WAL_DIR_PATH = walDir
		fmt.Println("Using Hash Indexed Database")

	case "btree":
		// Load file paths from .env
		dataFile, encodedBTreeIndexFile, walDir := loadEnv(indexType)
		db = btreeindex.NewBTreeIndex(dataFile, encodedBTreeIndexFile)
		WAL_DIR_PATH = walDir
		fmt.Println("Using BTree Indexed Database")

	default:
		fmt.Println("Invalid index type. Use 'hash' or 'btree'")
		return
	}

	// create wal directory if not exists
	if _, err := os.Stat(WAL_DIR_PATH); os.IsNotExist(err) {
		if err := os.MkdirAll(WAL_DIR_PATH, os.ModePerm); err != nil {
			log.Fatalf("Failed to create directory %s: %v", WAL_DIR_PATH, err)
			return
		} else {
			log.Printf("Created directory: %s", WAL_DIR_PATH)
		}
	}

	// **Determine if this is a new WAL or a restart**
	wal, err := setupWAL(WAL_DIR_PATH)
	if err != nil {
		log.Fatalf("Failed to initialize WAL: %v", err)
	}
	fmt.Println("WAL initialized successfully")

	fmt.Println("Commands: insert <key> <value> | search <key> | delete <key> | update <key> <value> | run_performance_test | replay_wal_entries_to_reconstruct_index | exit")

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("Enter command: ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if input == "exit" {
			db.StopIndexing()
			fmt.Println("Exiting database...")
			break
		}

		args := strings.SplitN(input, " ", 3)
		if len(args) < 2 && (args[0] != "run_performance_test" && args[0] != "replay_wal_entries_to_reconstruct_index") {
			fmt.Println("Invalid command format. Try again.")
			continue
		}

		command, key := args[0], ""
		if len(args) > 1 {
			key = args[1]
		}

		switch command {
		case "insert":
			if len(args) < 3 {
				fmt.Println("Usage: insert <key> <value>")
				continue
			}
			value := args[2]

			// writing to wal
			entry := fmt.Sprintf("insert|%s|%s", key, value) // Create WAL entry
			if err := wal.Write([]byte(entry)); err != nil { // Log the operation in WAL
				fmt.Println("WAL write failed:", err)
				continue
			}

			result := db.Insert(key, value)

			// Check if the result starts with "Success"
			if strings.HasPrefix(result, "Success") {
				fmt.Printf("Inserted: %s -> %s\n", key, value)
			} else {
				fmt.Println(result) // Print the failure message returned by the Insert method
			}

		case "search":
			value, found := db.Search(key)
			if found {
				fmt.Printf("Found: %s -> %s\n", key, value)
			} else {
				fmt.Println("Key not found")
			}

		case "delete":

			// writing to wal
			entry := fmt.Sprintf("delete|%s", key)           // WAL entry for delete
			if err := wal.Write([]byte(entry)); err != nil { // Log deletion
				fmt.Println("WAL write failed:", err)
				continue
			}

			result := db.Delete(key)
			if strings.HasPrefix(result, "Success") {
				fmt.Printf("Deleted key: %s\n", key)
			} else {
				fmt.Println(result) // Print the failure message returned by the Insert method
			}

		case "update":
			if len(args) < 3 {
				fmt.Println("Usage: update <key> <value>")
				continue
			}
			value := args[2]

			// writing to wal
			entry := fmt.Sprintf("update|%s|%s", key, value) // WAL entry for update
			if err := wal.Write([]byte(entry)); err != nil { // Log update operation
				fmt.Println("WAL write failed:", err)
				continue
			}

			result := db.Update(key, value)
			if strings.HasPrefix(result, "Success") {
				fmt.Printf("Updated: %s -> %s\n", key, value)
			} else {
				fmt.Println(result) // Print the failure message returned by the Insert method
			}

		case "run_performance_test":
			fmt.Println("Running performance test...")
			performanceTest(indexType)

		case "replay_wal_entries_to_reconstruct_index":
			fmt.Printf("Replaying WAL entries from and reconstructing index.")
			replayWALFileEntriesAndReconstructIndex(db, wal)

		default:
			fmt.Println("Unknown command. Use insert, search, delete, update, run_performance_test, or exit.")
		}
	}
}

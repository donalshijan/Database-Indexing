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
	"database_indexing/hashindex" // Import the HashIndex package

	"github.com/briandowns/spinner"
	"github.com/joho/godotenv"
	"github.com/schollz/progressbar/v3"
)

// Load environment variables from .env file
func loadEnv(dbType string) (string, string) {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	var dataFile string

	var indexFile string
	if dbType == "btree" {
		dataFile = os.Getenv("BTREE_DATA_FILE")
		indexFile = os.Getenv("ENCODED_BTREE_INDEX_FILE")
	} else if dbType == "hash" {
		dataFile = os.Getenv("DATA_FILE")
		indexFile = os.Getenv("INDEX_FILE")
	} else {
		log.Fatal("Invalid database type. Use 'hash' or 'btree'")
	}

	if dataFile == "" || indexFile == "" {
		log.Fatal("Required environment variables are missing in .env file")
	}

	return dataFile, indexFile
}

// Generate a random 6-letter word
func randomWord() string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	word := make([]rune, 6)
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

	numInserts := 4000
	numOps := 500
	var insertTimes, searchTimes, updateTimes, deleteTimes []time.Duration

	keys := make([]string, numInserts)
	fmt.Printf("\nRunning Insert Test\n")

	// INSERT TEST
	insertBar := progressbar.NewOptions(numInserts, progressbar.OptionSetWriter(os.Stderr), progressbar.OptionSetDescription("Inserting"))
	for i := 0; i < numInserts; i++ {
		key := fmt.Sprintf("key%d", i+1)
		value := randomWord()
		start := time.Now()
		msg := db.Insert(key, value)
		insertTimes = append(insertTimes, time.Since(start))
		keys[i] = key

		// Print status for insertion
		if strings.HasPrefix(msg, "Failed") {
			fmt.Printf("Insert failed: %s\n", msg)
		}

		insertBar.Add(1)
	}
	avgInsertTime := averageTime(insertTimes)
	fmt.Printf("Average Insert Time: %v\n", avgInsertTime)
	logger.Printf("Average Insert Time: %v\n", avgInsertTime)

	fmt.Printf("\nRunning Search Test\n")
	// SEARCH TEST
	searchBar := progressbar.NewOptions(numOps, progressbar.OptionSetWriter(os.Stderr), progressbar.OptionSetDescription("Searching"))
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	for i := 0; i < numOps; i++ {
		start := time.Now()
		db.Search(keys[i])
		searchTimes = append(searchTimes, time.Since(start))
		searchBar.Add(1)
	}
	avgSearchTime := averageTime(searchTimes)
	fmt.Printf("Average Search Time: %v\n", avgSearchTime)
	logger.Printf("Average Search Time: %v\n", avgSearchTime)

	fmt.Printf("\nRunning Update Test\n")
	// UPDATE TEST
	updateBar := progressbar.NewOptions(numOps, progressbar.OptionSetWriter(os.Stderr), progressbar.OptionSetDescription("Updating"))
	for i := 0; i < numOps; i++ {
		start := time.Now()
		db.Update(keys[i], randomWord())
		updateTimes = append(updateTimes, time.Since(start))
		updateBar.Add(1)
	}
	avgUpdateTime := averageTime(updateTimes)
	fmt.Printf("Average Update Time: %v\n", avgUpdateTime)
	logger.Printf("Average Update Time: %v\n", avgUpdateTime)

	fmt.Printf("\nRunning Delete Test\n")
	// DELETE TEST
	deleteBar := progressbar.NewOptions(numOps, progressbar.OptionSetWriter(os.Stderr), progressbar.OptionSetDescription("Deleting"))
	for i := 0; i < numOps; i++ {
		start := time.Now()
		db.Delete(keys[i])
		deleteTimes = append(deleteTimes, time.Since(start))
		deleteBar.Add(1)
	}
	avgDeleteTime := averageTime(deleteTimes)
	fmt.Printf("Average Delete Time: %v\n", avgDeleteTime)
	logger.Printf("Average Delete Time: %v\n", avgDeleteTime)

	fmt.Printf("\nRunning Indexing Capacity Test\n")
	// INDEXING CAPACITY TEST
	db.ClearIndex()
	clearDbFiles(tempDataFile, tempIndexFile)
	count := 0
	// Initialize the spinner with a rotating effect
	s := spinner.New(spinner.CharSets[14], 100*time.Millisecond) // Use character set 14 with 100ms update speed
	s.Suffix = fmt.Sprintf("  Entries Indexed: %d", count)
	s.Start()
	for {
		key := fmt.Sprintf("key%d", count+1)
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

		count++
		s.Suffix = fmt.Sprintf("  Entries Indexed: %d", count)
	}
	// Clean up: Remove the temporary database files
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

func main() {

	// Check if index type is provided as an argument
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <index_type>")
		fmt.Println("index_type: 'hash' or 'btree'")
		return
	}

	indexType := strings.ToLower(os.Args[1])

	var db database.Database

	// Initialize the appropriate index type
	switch indexType {
	case "hash":
		// Load file paths from .env
		dataFile, indexFile := loadEnv(indexType)
		db = hashindex.NewHashIndex(dataFile, indexFile)
		fmt.Println("Using Hash Index Database")

	case "btree":
		// Load file paths from .env
		dataFile, encodedBTreeIndexFile := loadEnv(indexType)
		db = btreeindex.NewBTreeIndex(dataFile, encodedBTreeIndexFile)
		fmt.Println("Using BTree Index Database")

	default:
		fmt.Println("Invalid index type. Use 'hash' or 'btree'")
		return
	}

	fmt.Println("Simple Database CLI")
	fmt.Println("Commands: insert <key> <value> | search <key> | delete <key> | update <key> <value> | run_performance_test | exit")

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
		if len(args) < 2 && args[0] != "run_performance_test" {
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
			result := db.Update(key, value)
			if strings.HasPrefix(result, "Success") {
				fmt.Printf("Updated: %s -> %s\n", key, value)
			} else {
				fmt.Println(result) // Print the failure message returned by the Insert method
			}

		case "run_performance_test":
			fmt.Println("Running performance test...")
			performanceTest(indexType)

		default:
			fmt.Println("Unknown command. Use insert, search, delete, update, run_performance_test, or exit.")
		}
	}
}

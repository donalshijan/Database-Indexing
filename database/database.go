package database

type Database interface {
	Insert(key, value string) string
	Search(key string) (string, bool)
	Delete(key string) string
	Update(key, value string) string
	StopIndexing()
	ClearIndex()
	GetMaxMemoryUsage() int64
}

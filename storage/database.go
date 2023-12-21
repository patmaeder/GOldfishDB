package storage

import (
	_ "github.com/joho/godotenv/autoload"
	"os"
	"strings"
)

var dataDir = os.Getenv("DATA_DIR")

type Database struct {
	Name string
}

func (d *Database) GetPath() string {
	return strings.TrimLeft(dataDir, "/") + "/" + d.Name
}

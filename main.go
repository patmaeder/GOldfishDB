package main

import (
	"DBMS/SQL/Parser"
	"os"
	"strings"

	_ "github.com/joho/godotenv/autoload"
)

func main() {

	// Sanitize DATA_DIR
	dataDir := os.Getenv("DATA_DIR")
	err := os.Setenv("DATA_DIR", strings.TrimRight(dataDir, "/"))
	if err != nil {
		panic(err)
	}

	// Create data directory
	err = os.MkdirAll(os.Getenv("DATA_DIR"), 0777)
	if err != nil {
		panic(err)
	}

	/*Server.Handle(Compose)
	Server.Start()*/
}

// TODO: Rename compose function
func Compose(buffer []byte) []byte {

	parser := Parser.New(string(buffer))
	_, err := parser.Parse()
	if err != nil {
		return []byte(err.Error())
	}
	return []byte("Received and parsed")
}

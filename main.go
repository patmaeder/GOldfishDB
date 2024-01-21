package main

import (
	"DBMS/SQL/Parser"
	"DBMS/TCP/Server"
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

	Server.Handle(Execute)
	Server.Start()
}

func Execute(buffer []byte) []byte {

	parser := Parser.New(string(buffer))
	command, err := parser.Parse()
	if err != nil {
		return []byte(err.Error())
	}
	response := command.Execute()
	if _, isError := response.(error); isError {
		return []byte(response.(error).Error())
	}
	return []byte(response.(string))
}

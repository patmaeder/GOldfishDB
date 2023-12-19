package main

import (
	"DBMS/SQL/Parser"
	"DBMS/storage"
	"fmt"
	"os"

	_ "github.com/joho/godotenv/autoload"
)

func main() {

	// Create data directory
	err := os.MkdirAll(os.Getenv("DATA_DIR"), 0777)
	if err != nil {
		panic(err)
	}

	/*Server.Handle(Compose)
	Server.Start()*/
	err = storage.CreateDatabase("test")
	if err != nil {
		fmt.Println(err.Error())
	}

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

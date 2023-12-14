package main

import (
	"DBMS/SQL/Parser"
	"DBMS/TCP/Server"
	"fmt"
)

func main() {
	Server.Handle(Compose)
	Server.Start()
}

func Compose(buffer []byte) []byte {

	fmt.Printf("Received: %s\n", string(buffer))
	parser := Parser.New(string(buffer))
	err := parser.Parse()
	if err != nil {
		return []byte(err.Error())
	}
	return []byte("Received and parsed")
}

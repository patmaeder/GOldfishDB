package main

import (
	"DBMS/SQL/Parser"
	"DBMS/TCP/Server"
)

func main() {

	parser := Parser.New("SELECT username, password FROM another_mother;")
	err := parser.Parse()

	if err != nil {
		print(err.Error())
	}

	Server.Start()
}

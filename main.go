package main

import (
	"DBMS/SQL/Parser"
)

func main() {

	parser := Parser.New("SELECT username, password FROM another_mother;")
	err := parser.Parse()

	if err != nil {
		print(err.Error())
	}
}

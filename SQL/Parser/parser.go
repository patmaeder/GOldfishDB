package Parser

import (
	"DBMS/SQL"
	"DBMS/SQL/Lexer"
	"DBMS/SQL/Token"
)

type Parser struct {
	SQL     string
	Tokens  []Token.Token
	Pointer int
	Query   SQL.Query
}

func New(sql string) *Parser {
	parser := new(Parser)
	parser.SQL = sql
	parser.Pointer = 0
	return parser
}

func (p *Parser) Parse() error {
	lexer := Lexer.New(p.SQL)
	normalizedSQLStatement, err := lexer.Lex()
	if err != nil {

		return err
	}
	p.Tokens = normalizedSQLStatement

	return nil
}

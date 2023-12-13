package Token

type TokenType int

const (
	WS TokenType = iota
	STRING
	NUMBER
	IDENT
	OPERATOR
	WILDCARD
	FUNCTION
	PUNCTUATION
	EOF
)

type Token struct {
	Type  TokenType
	Value string
}

package Token

type Type int

const (
	WS Type = iota
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
	Type  Type
	Value string
}

package Lexer

import (
	"unicode"
)

func isWhitespace(ch rune) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func isDigit(ch rune) bool {
	return '0' <= ch && ch <= '9'
}

func isLetter(ch rune) bool {
	return unicode.IsLetter(ch) || ch == '_'
}

func isDoubleQuote(ch rune) bool {
	return ch == '"'
}

func isSingleQuote(ch rune) bool {
	return ch == '\''
}

func isLeadingSign(ch rune) bool {
	return ch == '+' || ch == '-'
}

func isOperator(ch rune) bool {
	return ch == '+' || ch == '-' || ch == '*' || ch == '/' || ch == '=' || ch == '<' || ch == '>' || ch == '!' || ch == '&' || ch == '|' || ch == '^' || ch == '%' || ch == '~' || ch == '?' || ch == '@' || ch == ':' || ch == '#'
}

func isWildcard(ch rune) bool {
	return ch == '*'
}

func isPunctuation(ch rune) bool {
	return ch == '(' || ch == ')' || ch == ',' || ch == '.' || ch == ':' || ch == '[' || ch == ']' || ch == '{' || ch == '}'
}

func isEOF(ch rune) bool {
	return ch == ';'
}

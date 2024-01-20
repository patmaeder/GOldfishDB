package Lexer

import (
	"DBMS/SQL/Token"
	"errors"
	"strings"
	"unicode/utf8"
)

type Lexer struct {
	Input   string
	Tokens  []Token.Token
	pointer int
}

func New(input string) *Lexer {
	return &Lexer{
		Input:   strings.TrimSpace(input),
		pointer: 0,
	}
}

func (l *Lexer) hasNext() bool {
	return l.pointer < len(l.Input)
}

func (l *Lexer) get(shift int) (rune, error) {
	if l.pointer+shift > len(l.Input) || l.pointer+shift < 0 {
		return rune(-1), errors.New("index out of scope")
	}

	r, _ := utf8.DecodeRuneInString(l.Input[l.pointer+shift : l.pointer+shift+1])
	return r, nil
}

func (l *Lexer) next() rune {
	// TODO: Neat to show
	defer func() {
		l.pointer++
	}()
	return l.current()
}

func (l *Lexer) previous() rune {
	ch, _ := l.get(-1)
	return ch
}

func (l *Lexer) current() rune {
	ch, _ := l.get(0)
	return ch
}

func (l *Lexer) peek() rune {
	ch, _ := l.get(1)
	return ch
}

func (l *Lexer) Lex() ([]Token.Token, error) {
	for l.hasNext() {
		char := l.current()
		token, err := l.scan(char)
		if err != nil {
			return nil, err
		}

		l.Tokens = append(l.Tokens, token)

		if token.Type == Token.EOF {
			break
		}

		l.next()
	}

	return l.Tokens, nil
}

func (l *Lexer) scan(ch rune) (Token.Token, error) {

	switch {
	case isWhitespace(ch):
		return l.scanWhitespace(), nil
	case isLetter(ch):
		return l.scanIdentifier(), nil
	case isDoubleQuote(ch):
		return l.scanQuotedIdentifier('"'), nil
	case isSingleQuote(ch):
		return l.scanQuotedIdentifier('\''), nil
	case isLeadingSign(ch):
		if isDigit(l.peek()) || l.peek() == '.' {
			return l.scanNumberWithLeadingSign(), nil
		}
		return l.scanOperator(), nil
	case isDigit(ch):
		return l.scanNumber(), nil
	case isWildcard(ch):
		return l.scanWildcard(), nil
	case isOperator(ch):
		return l.scanOperator(), nil
	case isPunctuation(ch):
		return l.scanPunctuation(), nil
	case isEOF(ch):
		return Token.Token{Type: Token.EOF}, nil
	}

	return Token.Token{}, errors.New("Unknown character: " + string(ch) + " near position " + string(rune(l.pointer)))
}

func (l *Lexer) scanWhitespace() Token.Token {
	for l.hasNext() && isWhitespace(l.peek()) {
		l.next()
	}
	return Token.Token{Type: Token.WS}
}

func (l *Lexer) scanIdentifier() Token.Token {
	start := l.pointer

	for {
		if isLetter(l.peek()) {
			// TODO: Check if open parenthesis -> function
			l.next()
		} else {
			break
		}
	}

	return Token.Token{Type: Token.IDENT, Value: l.Input[start : l.pointer+1]}
}

func (l *Lexer) scanQuotedIdentifier(delimiter rune) Token.Token {
	start := l.pointer + 1
	escapedChar := false

	for l.hasNext() {
		if l.peek() == delimiter {
			l.next()
			break
		}

		if isEOF(l.peek()) {
			break
		}

		if escapedChar {
			escapedChar = false
			l.next()
			continue
		}

		if l.peek() == '\\' {
			escapedChar = true
			l.next()
			continue
		}

		l.next()
	}

	return Token.Token{Type: Token.IDENT, Value: l.Input[start:l.pointer]}
}

func (l *Lexer) scanOperator() Token.Token {
	start := l.pointer
	for isOperator(l.peek()) {
		l.next()
	}

	return Token.Token{Type: Token.OPERATOR, Value: l.Input[start : l.pointer+1]}
}

func (l *Lexer) scanNumber() Token.Token {
	start := l.pointer
	l.scanNumeric()
	return Token.Token{Type: Token.NUMBER, Value: l.Input[start : l.pointer+1]}
}

func (l *Lexer) scanNumberWithLeadingSign() Token.Token {
	start := l.pointer
	l.scanNumeric()
	return Token.Token{Type: Token.NUMBER, Value: l.Input[start : l.pointer+1]}
}

func (l *Lexer) scanNumeric() {
	for isDigit(l.peek()) || l.peek() == '.' {
		l.next()
	}
}

func (l *Lexer) scanWildcard() Token.Token {
	start := l.pointer
	return Token.Token{Type: Token.WILDCARD, Value: l.Input[start : l.pointer+1]}
}

func (l *Lexer) scanPunctuation() Token.Token {
	start := l.pointer
	return Token.Token{Type: Token.PUNCTUATION, Value: l.Input[start : l.pointer+1]}
}

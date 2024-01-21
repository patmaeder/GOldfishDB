package Parser

import (
	"DBMS/SQL/Lexer"
	"DBMS/SQL/Token"
	"DBMS/storage"
	"DBMS/storage/command"
	"DBMS/storage/processors"
	"DBMS/storage/value"
	"DBMS/utils"
	"errors"
	"strconv"
	"strings"
)

type Parser struct {
	sql     string
	tokens  []Token.Token
	pointer int
	command command.Command
}

func New(sql string) *Parser {
	return &Parser{
		sql:     strings.TrimSpace(sql),
		pointer: 0,
	}
}

func (p *Parser) hasNext() bool {
	return p.pointer < len(p.tokens)
}

func (p *Parser) get(shift int) (Token.Token, error) {
	if p.pointer+shift > len(p.tokens) || p.pointer+shift < 0 {
		return Token.Token{}, errors.New("unexpected end of file")
	}

	return p.tokens[p.pointer+shift], nil
}

func (p *Parser) next() (Token.Token, error) {
	defer func() {
		p.pointer++
	}()

	if p.hasNext() {
		return p.current(), nil
	}
	return Token.Token{}, errors.New("unexpected end of input")
}

func (p *Parser) previous() Token.Token {
	token, err := p.get(-1)
	if err != nil {
		return p.current()
	}
	return token
}

func (p *Parser) current() Token.Token {
	ch, _ := p.get(0)
	return ch
}

func (p *Parser) peek() Token.Token {
	ch, _ := p.get(1)
	return ch
}

func (p *Parser) Parse() (command.Command, error) {
	if !strings.HasSuffix(p.sql, ";") {
		return nil, errors.New("missing semicolon at the end of the statement")
	}

	// Lex input string
	lexer := Lexer.New(p.sql)
	tokens, err := lexer.Lex()
	if err != nil {
		return nil, err
	}

	for _, token := range tokens {
		if token.Type != Token.WS {
			p.tokens = append(p.tokens, token)
		}
	}

	switch method, _ := p.next(); strings.ToUpper(method.Value) {
	case "SELECT":
		err = p.parseSelect()
	case "INSERT":
		err = p.parseInsert()
	case "UPDATE":
		err = p.parseUpdate()
	case "DELETE":
		err = p.parseDelete()
	case "CREATE":
		err = p.parseCreate()
	case "DROP":
		err = p.parseDrop()
	default:
		err = errors.New("unsupported method")
	}

	if err != nil {
		return nil, err
	}
	return p.command, nil
}

func (p *Parser) parseSelect() error {
	selectCommand := command.Select{}

	wildcard := false
	if p.current().Type == Token.WILDCARD {
		wildcard = true
		_, err := p.next()
		if err != nil {
			return err
		}
	}

	fields := make([][128]byte, 0)
	for {
		if strings.ToUpper(p.current().Value) == "FROM" {
			break
		}
		if p.current().Type == Token.PUNCTUATION {
			continue
		}

		v, err := utils.StringToByteArray(p.current().Value, 128)
		if err != nil {
			return errors.New("cannot parse " + p.current().Value + " to column name with max length of 128 bytes: " + p.current().Value)
		}

		fields = append(fields, [128]byte(v))

		_, err = p.next()
		if err != nil {
			return err
		}
	}

	if strings.ToUpper(p.current().Value) != "FROM" {
		return errors.New("unexpected token: " + p.current().Value)
	}

	_, err := p.next()
	if err != nil {
		return err
	}

	table, err := storage.GetTable(p.current().Value)
	if err != nil {
		return err
	}
	selectCommand.Table = table

	if wildcard {
		for _, column := range table.Columns {
			fields = append(fields, column.Name)
		}
	}

	selectCommand.Fields = fields

	_, err = p.next()
	if err != nil {
		return err
	}

	for p.current().Type != Token.EOF {
		switch strings.ToUpper(p.current().Value) {
		case "WHERE":
			where, err := p.parseWhere(table)
			if err != nil {
				return err
			}
			selectCommand.Where = where
		case "LIMIT":
			_, err = p.next()
			if err != nil {
				return err
			}

			v, err := strconv.Atoi(p.current().Value)
			if err != nil {
				return errors.New("cannot parse " + p.current().Value + " to valid LIMIT constraint")
			}
			selectCommand.Limit = v
		case "ORDER":
			order, err := p.parseOrder(table)
			if err != nil {
				return err
			}
			selectCommand.Order = order
		default:
			return errors.New("unknown select modifier: " + p.current().Value)
		}

		_, err = p.next()
		if err != nil {
			return err
		}
	}

	p.command = selectCommand
	return nil
}

func (p *Parser) parseUpdate() error {
	updateCommand := command.UpdateCommand{}

	table, err := storage.GetTable(p.current().Value)
	if err != nil {
		return err
	}
	updateCommand.Table = table

	_, err = p.next()
	if err != nil {
		return err
	}

	if strings.ToUpper(p.current().Value) != "SET" {
		return errors.New("unexpected token: " + p.current().Value)
	}

	values := storage.Row{Entries: make(map[[128]byte]value.Value)}
	columns := table.ConvertColumnsToMap()
	for {
		_, err = p.next()
		if err != nil {
			return err
		}

		columnName, err := utils.StringToByteArray(p.current().Value, 128)
		if err != nil {
			return errors.New("cannot parse " + p.current().Value + " to column name with max length of 128 bytes")
		}

		_, err = p.next()
		if err != nil {
			return err
		}

		if p.current().Value != "=" {
			return errors.New("unexpected token: " + p.current().Value)
		}

		_, err = p.next()
		if err != nil {
			return err
		}

		switch columns[[128]byte(columnName)].Type {
		case storage.INTEGER:
			if strings.ToUpper(p.current().Value) == "NULL" {
				values.Entries[[128]byte(columnName)] = value.IntegerNull()
				break
			}
			v, err := strconv.Atoi(p.current().Value)
			if err != nil {
				return errors.New("cannot parse " + p.current().Value + " to valid INTEGER")
			}
			values.Entries[[128]byte(columnName)] = value.Integer(v)
		case storage.REAL:
			if strings.ToUpper(p.current().Value) == "NULL" {
				values.Entries[[128]byte(columnName)] = value.RealNull()
				break
			}
			v, err := strconv.Atoi(p.current().Value)
			if err != nil {
				return errors.New("cannot parse " + p.current().Value + " to valid REAL")
			}
			values.Entries[[128]byte(columnName)] = value.Real(v)
		case storage.BOOLEAN:
			if strings.ToUpper(p.current().Value) == "NULL" {
				values.Entries[[128]byte(columnName)] = value.BooleanNull()
				break
			}
			v, err := strconv.ParseBool(p.current().Value)
			if err != nil {
				return errors.New("cannot parse " + p.current().Value + " to valid BOOLEAN")
			}
			values.Entries[[128]byte(columnName)] = value.Boolean(v)
		case storage.TEXT:
			if strings.ToUpper(p.current().Value) == "NULL" {
				values.Entries[[128]byte(columnName)] = value.TextNull()
				break
			}
			v, err := utils.StringToByteArray(p.current().Value, 1024)
			if err != nil {
				return errors.New("cannot parse " + p.current().Value + " to valid TEXT with max length of 1024 bytes")
			}
			values.Entries[[128]byte(columnName)] = value.Text([1024]byte(v))
		}

		if !p.hasNext() {
			return errors.New("unexpected end of input")
		}
		if p.peek().Value != "," {
			break
		}

		_, err = p.next()
		if err != nil {
			return err
		}
	}

	updateCommand.Values = values

	_, err = p.next()
	if err != nil {
		return err
	}

	if strings.ToUpper(p.current().Value) == "WHERE" {
		where, err := p.parseWhere(table)
		if err != nil {
			return err
		}
		updateCommand.Where = where

		_, err = p.next()
		if err != nil {
			return err
		}
	}

	if p.current().Type != Token.EOF {
		return errors.New("unexpected token: " + p.current().Value)
	}

	p.command = updateCommand
	return nil
}

func (p *Parser) parseInsert() error {
	insertCommand := command.Insert{}

	if strings.ToUpper(p.current().Value) != "INTO" {
		return errors.New("unexpected token: " + p.current().Value)
	}

	_, err := p.next()
	if err != nil {
		return err
	}

	table, err := storage.GetTable(p.current().Value)
	if err != nil {
		return err
	}
	insertCommand.Table = table
	tableColumns := table.ConvertColumnsToMap()

	_, err = p.next()
	if err != nil {
		return err
	}

	if p.current().Value != "(" {
		return errors.New("unexpected token: " + p.current().Value)
	}

	columns := make([][128]byte, 0)
	for p.current().Value != ")" {
		_, err = p.next()
		if err != nil {
			return err
		}

		v, err := utils.StringToByteArray(p.current().Value, 128)
		if err != nil {
			return errors.New("cannot parse " + p.current().Value + " to column name with max length of 128 bytes")
		}

		if _, exists := tableColumns[[128]byte(v)]; !exists {
			return errors.New("column " + p.current().Value + " does not exist on table " + table.Name)
		}

		columns = append(columns, [128]byte(v))

		_, err = p.next()
		if err != nil {
			return err
		}

		if p.current().Type != Token.PUNCTUATION {
			return errors.New("unexpected token: " + p.current().Value)
		}
	}

	_, err = p.next()
	if err != nil {
		return err
	}

	if strings.ToUpper(p.current().Value) != "VALUES" {
		return errors.New("unexpected token: " + p.current().Value)
	}

	_, err = p.next()
	if err != nil {
		return err
	}

	for p.current().Value == "(" {
		row := storage.Row{Entries: make(map[[128]byte]value.Value)}
		i := 0

		for p.current().Value != ")" {
			_, err = p.next()
			if err != nil {
				return err
			}

			switch tableColumns[columns[i]].Type {
			case storage.INTEGER:
				if strings.ToUpper(p.current().Value) == "NULL" {
					row.Entries[columns[i]] = value.IntegerNull()
					break
				}
				v, err := strconv.Atoi(p.current().Value)
				if err != nil {
					return errors.New("cannot parse " + p.current().Value + " to valid INTEGER")
				}
				row.Entries[columns[i]] = value.Integer(v)
			case storage.REAL:
				if strings.ToUpper(p.current().Value) == "NULL" {
					row.Entries[columns[i]] = value.RealNull()
					break
				}
				v, err := strconv.Atoi(p.current().Value)
				if err != nil {
					return errors.New("cannot parse " + p.current().Value + " token to valid REAL")
				}
				row.Entries[columns[i]] = value.Real(v)
			case storage.BOOLEAN:
				if strings.ToUpper(p.current().Value) == "NULL" {
					row.Entries[columns[i]] = value.BooleanNull()
					break
				}
				v, err := strconv.ParseBool(p.current().Value)
				if err != nil {
					return errors.New("cannot parse " + p.current().Value + " token to valid BOOLEAN")
				}
				row.Entries[columns[i]] = value.Boolean(v)
			case storage.TEXT:
				if strings.ToUpper(p.current().Value) == "NULL" {
					row.Entries[columns[i]] = value.TextNull()
					break
				}
				v, err := utils.StringToByteArray(p.current().Value, 1024)
				if err != nil {
					return errors.New("cannot parse " + p.current().Value + " to valid TEXT with max length of 1024 bytes")
				}
				row.Entries[columns[i]] = value.Text([1024]byte(v))
			}

			_, err = p.next()
			if err != nil {
				return err
			}

			if p.current().Type != Token.PUNCTUATION {
				return errors.New("unexpected token: " + p.current().Value)
			}

			i++
		}

		if len(row.Entries) != len(columns) {
			return errors.New("amount of specified columns and given values must match")
		}

		insertCommand.Rows = append(insertCommand.Rows, row)

		if p.peek().Value == "," {
			_, err = p.next()
			if err != nil {
				return err
			}
		}

		_, err = p.next()
		if err != nil {
			return err
		}
	}

	if p.current().Type != Token.EOF {
		return errors.New("unexpected token: " + p.current().Value)
	}

	p.command = insertCommand
	return nil
}

func (p *Parser) parseDelete() error {
	deleteCommand := command.Delete{}

	if strings.ToUpper(p.current().Value) != "FROM" {
		return errors.New("unexpected token: " + p.current().Value)
	}

	_, err := p.next()
	if err != nil {
		return err
	}

	table, err := storage.GetTable(p.current().Value)
	if err != nil {
		return err
	}
	deleteCommand.Table = table

	_, err = p.next()
	if err != nil {
		return err
	}

	if strings.ToUpper(p.current().Value) == "WHERE" {
		where, err := p.parseWhere(table)
		if err != nil {
			return err
		}
		deleteCommand.Where = where

		_, err = p.next()
		if err != nil {
			return err
		}
	}

	if p.current().Type != Token.EOF {
		return errors.New("unexpected token: " + p.current().Value)
	}

	p.command = deleteCommand
	return nil
}

func (p *Parser) parseCreate() error {
	switch strings.ToUpper(p.current().Value) {
	case "TABLE":
		return p.parseCreateTable()
	default:
		return errors.New("unexpected Token: " + p.current().Value)
	}
}

func (p *Parser) parseCreateTable() error {
	createTableCommand := command.CreateTable{}

	if strings.ToUpper(p.current().Value) == "TABLE" {
		_, err := p.next()
		if err != nil {
			return err
		}
	}

	if strings.ToUpper(p.current().Value) == "IF" {
		_, err := p.next()
		if err != nil {
			return err
		}

		if strings.ToUpper(p.current().Value) != "NOT" {
			return errors.New("unexpected token: " + p.current().Value)
		}
		_, err = p.next()
		if err != nil {
			return err
		}

		if strings.ToUpper(p.current().Value) != "EXISTS" {
			return errors.New("unexpected token: " + p.current().Value)
		}

		createTableCommand.IfNotExists = true
		_, err = p.next()
		if err != nil {
			return err
		}
	}

	tableName := p.current().Value

	_, err := p.next()
	if err != nil {
		return err
	}

	if p.current().Value != "(" {
		return errors.New("unexpected Token: " + p.current().Value)
	}

	columns := make([]*storage.Column, 0)
	for p.current().Value != ")" {
		_, err := p.next()
		if err != nil {
			return err
		}

		column := storage.Column{}
		v, err := utils.StringToByteArray(p.current().Value, 128)
		if err != nil {
			return errors.New("cannot parse " + p.current().Value + " to column name with max length of 128 bytes")
		}
		column.Name = [128]byte(v)

		_, err = p.next()
		if err != nil {
			return err
		}

		switch strings.ToUpper(p.current().Value) {
		case "INTEGER":
			column.Type = storage.INTEGER
		case "REAL":
			column.Type = storage.REAL
		case "BOOLEAN":
			column.Type = storage.BOOLEAN
		case "TEXT":
			column.Type = storage.TEXT
		default:
			return errors.New("unknown column type: " + p.current().Value)
		}

		_, err = p.next()
		if err != nil {
			return err
		}

		for p.current().Type != Token.PUNCTUATION {

			switch strings.ToUpper(p.current().Value) {
			case "PRIMARY":
				_, err = p.next()
				if err != nil {
					return err
				}

				if strings.ToUpper(p.current().Value) != "KEY" {
					return errors.New("unexpected token: " + p.current().Value)
				}
				column.Primary = true
				column.Unique = true
				column.NotNullable = true
			case "AUTOINCREMENT":
				column.Autoincrement = true
				column.Unique = true
				column.NotNullable = true
			case "UNIQUE":
				column.Unique = true
				column.NotNullable = true
			case "NOT":
				_, err = p.next()
				if err != nil {
					return err
				}

				if strings.ToUpper(p.current().Value) != "NULL" {
					return errors.New("unexpected token: " + p.current().Value)
				}
				column.NotNullable = true
			default:
				return errors.New("unexpected token: " + p.current().Value)
			}

			_, err = p.next()
			if err != nil {
				return err
			}
		}

		columns = append(columns, &column)
	}

	table, err := storage.NewTable(tableName, columns)
	if err != nil {
		return err
	}
	createTableCommand.Table = table

	_, err = p.next()
	if err != nil {
		return err
	}

	if p.current().Type != Token.EOF {
		return errors.New("unexpected token: " + p.current().Value)
	}

	p.command = createTableCommand
	return nil
}

func (p *Parser) parseDrop() error {
	switch strings.ToUpper(p.current().Value) {
	case "TABLE":
		return p.parseDropTable()
	default:
		return errors.New("unexpected token: " + p.current().Value)
	}
}

func (p *Parser) parseDropTable() error {
	dropTableCommand := command.DropTable{}

	if strings.ToUpper(p.current().Value) == "TABLE" {
		_, err := p.next()
		if err != nil {
			return err
		}
	}

	if strings.ToUpper(p.current().Value) == "IF" {
		_, err := p.next()
		if err != nil {
			return err
		}

		if strings.ToUpper(p.current().Value) != "EXISTS" {
			return errors.New("unexpected token: " + p.current().Value)
		}
		dropTableCommand.IfExists = true
		_, err = p.next()
		if err != nil {
			return err
		}
	}

	table, err := storage.GetTable(p.current().Value)
	if err != nil {
		return err
	}
	dropTableCommand.Table = table

	_, err = p.next()
	if err != nil {
		return err
	}

	if p.current().Type != Token.EOF {
		return errors.New("unexpected token: " + p.current().Value)
	}

	p.command = dropTableCommand
	return nil
}

func (p *Parser) parseWhere(table storage.Table) (map[[128]byte]value.Constraint, error) {
	where := make(map[[128]byte]value.Constraint)
	columns := table.ConvertColumnsToMap()

	for {
		constraint := value.Constraint{}

		_, err := p.next()
		if err != nil {
			return nil, err
		}

		columnName, err := utils.StringToByteArray(p.current().Value, 128)
		if err != nil {
			return nil, errors.New("cannot parse " + p.current().Value + " to column name with max length of 128 bytes")
		}

		if _, exists := columns[[128]byte(columnName)]; !exists {
			return nil, errors.New("column " + utils.ByteArrayToString(columnName[:]) + " does not exist")
		}

		_, err = p.next()
		if err != nil {
			return nil, err
		}

		switch strings.ToUpper(p.current().Value) {
		case "IS":
			if !p.hasNext() {
				return nil, errors.New("unexpected end of input")
			}
			if strings.ToUpper(p.peek().Value) == "NOT" {
				constraint.Operator = value.NOT_EQUAL
			} else {
				constraint.Operator = value.EQUAL
			}
		case "=":
			constraint.Operator = value.EQUAL
		case "!=":
			constraint.Operator = value.NOT_EQUAL
		case "<":
			constraint.Operator = value.LT
		case "<=":
			constraint.Operator = value.LT_EQUAL
		case ">":
			constraint.Operator = value.GT
		case ">=":
			constraint.Operator = value.GT_EQUAL
		default:
			return nil, errors.New("unknown operator: " + p.current().Value)
		}

		_, err = p.next()
		if err != nil {
			return nil, err
		}

		switch columns[[128]byte(columnName)].Type {
		case storage.INTEGER:
			if strings.ToUpper(p.current().Value) == "NULL" {
				constraint.Value = value.IntegerNull()
				break
			}
			v, err := strconv.Atoi(p.current().Value)
			if err != nil {
				return nil, errors.New("cannot parse " + p.current().Value + " to valid INTEGER")
			}
			constraint.Value = value.Integer(v)
		case storage.REAL:
			if strings.ToUpper(p.current().Value) == "NULL" {
				constraint.Value = value.RealNull()
				break
			}
			v, err := strconv.Atoi(p.current().Value)
			if err != nil {
				return nil, errors.New("cannot parse " + p.current().Value + " to valid REAL")
			}
			constraint.Value = value.Real(v)
		case storage.BOOLEAN:
			if strings.ToUpper(p.current().Value) == "NULL" {
				constraint.Value = value.BooleanNull()
				break
			}
			v, err := strconv.ParseBool(p.current().Value)
			if err != nil {
				return nil, errors.New("cannot parse " + p.current().Value + " to valid BOOLEAN")
			}
			constraint.Value = value.Boolean(v)
		case storage.TEXT:
			if strings.ToUpper(p.current().Value) == "NULL" {
				constraint.Value = value.TextNull()
				break
			}
			v, err := utils.StringToByteArray(p.current().Value, 1024)
			if err != nil {
				return nil, errors.New("cannot parse " + p.current().Value + " to valid TEXT with max length of 1024 bytes")
			}
			constraint.Value = value.Text([1024]byte(v))
		}

		where[[128]byte(columnName)] = constraint

		if !p.hasNext() {
			return nil, errors.New("unexpected end of input")
		}
		if strings.ToUpper(p.peek().Value) != "AND" {
			break
		}
	}

	return where, nil
}

func (p *Parser) parseOrder(table storage.Table) ([]processors.OrderInstruction, error) {
	order := make([]processors.OrderInstruction, 0)
	columns := table.ConvertColumnsToMap()

	_, err := p.next()
	if err != nil {
		return nil, err
	}

	if strings.ToUpper(p.current().Value) != "BY" {
		return nil, errors.New("unexpected token: " + p.current().Value)
	}

	for {
		instruction := processors.OrderInstruction{Direction: processors.ASC}
		_, err := p.next()
		if err != nil {
			return nil, err
		}

		columnName, err := utils.StringToByteArray(p.current().Value, 128)
		if err != nil {
			return nil, errors.New("cannot parse " + p.current().Value + " to column name with max length of 128 bytes")
		}

		if _, exists := columns[[128]byte(columnName)]; !exists {
			return nil, errors.New("column " + utils.ByteArrayToString(columnName[:]) + " does not exist")
		}
		instruction.Column = [128]byte(columnName)

		if strings.ToUpper(p.peek().Value) == "ASC" || strings.ToUpper(p.peek().Value) == "DESC" {
			_, err = p.next()
			if err != nil {
				return nil, err
			}

			if strings.ToUpper(p.current().Value) == "DESC" {
				instruction.Direction = processors.DESC
			}
		}

		order = append(order, instruction)

		if p.peek().Type == Token.PUNCTUATION {
			continue
		}

		break
	}

	return order, nil
}

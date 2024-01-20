package command

import (
	"DBMS/storage"
	"DBMS/storage/processors"
	"DBMS/storage/value"
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"os"
)

type Select struct {
	Table  storage.Table
	Fields [][128]byte
	Where  map[[128]byte]value.Constraint
	Limit  int
}

func (c Select) Validate() any {
	columns := c.Table.ConvertColumnsToMap()

	for _, field := range c.Fields {
		if _, exists := columns[field]; !exists {
			return errors.New("column " + string(field[:]) + " does not exist on table " + c.Table.Name)
		}
	}

	for field, constraint := range c.Where {
		switch columns[field].Type {
		case storage.BOOLEAN:
			if constraint.Operator != value.EQUAL && constraint.Operator != value.NOT_EQUAL {
				return errors.New("invalid binary operator on type BOOLEAN")
			}
		case storage.TEXT:
			if constraint.Operator != value.EQUAL && constraint.Operator != value.NOT_EQUAL {
				return errors.New("invalid binary operator on type TEXT")
			}
		}
	}

	if c.Limit < 0 {
		return errors.New("limit cannot be lower than 1")
	}

	return nil
}

func (c Select) Execute() any {
	err := c.Validate()
	if err != nil {
		return err
	}

	idbFile, _ := os.OpenFile(c.Table.GetIdbFilePath(), os.O_RDONLY, 0444)
	reader := bufio.NewReader(idbFile)
	defer idbFile.Close()

	columnMap := c.Table.ConvertColumnsToMap()
	rows := make([]storage.Row, 0)
	where := processors.Where(&c.Table, c.Where)
	limit := processors.Limit(c.Limit)

	yield := make(chan struct{})
	for rowId := range where.Limit(limit).Process(yield) {
		row := storage.NewRow(map[[128]byte]value.Value{})
		for _, field := range c.Fields {
			column := columnMap[field]
			bufferSize := int64(0)
			var mappingValue value.Value

			switch column.Type {
			case storage.INTEGER:
				bufferSize = value.IntegerLength
				mappingValue = new(value.IntegerValue)
			case storage.REAL:
				bufferSize = value.RealLength
				mappingValue = new(value.RealValue)
			case storage.BOOLEAN:
				bufferSize = value.BooleanLength
				mappingValue = new(value.BooleanValue)
			case storage.TEXT:
				bufferSize = value.TextLength
				mappingValue = new(value.TextValue)
			}

			buffer := make([]byte, bufferSize)
			idbFile.Seek(rowId*c.Table.RowLength+column.Offset, 0)
			_, err := reader.Read(buffer)
			if err != nil {
				return err
			}

			err = binary.Read(bytes.NewReader(buffer), binary.LittleEndian, mappingValue)
			if err != nil {
				return err
			}

			row.Entries[column.Name] = mappingValue
		}

		rows = append(rows, row)
	}

	queryResult := ""

	for i, field := range c.Fields {
		queryResult += string(field[:])
		if i+1 != len(c.Fields) {
			queryResult += ";"
		} else {
			queryResult += "\n"
		}
	}

	for _, row := range rows {
		for i, field := range c.Fields {
			queryResult += row.Entries[field].ToString()
			if i+1 != len(c.Fields) {
				queryResult += ";"
			} else {
				queryResult += "\n"
			}
		}
	}

	return "CODE 200: " + queryResult
}

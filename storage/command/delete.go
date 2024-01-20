package command

import (
	"DBMS/storage"
	"DBMS/storage/processors"
	"DBMS/storage/value"
	"bufio"
	"errors"
	"os"
)

type Delete struct {
	Table storage.Table
	Where map[[128]byte]value.Constraint
}

func (c Delete) Validate() any {
	columns := c.Table.ConvertColumnsToMap()
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

	return nil
}

func (c Delete) Execute() any {
	err := c.Validate()
	if err != nil {
		return err
	}

	idbFile, _ := os.OpenFile(c.Table.GetIdbFilePath(), os.O_RDWR, 0644)
	reader := bufio.NewReader(idbFile)
	writer := bufio.NewWriter(idbFile)
	defer idbFile.Close()

	where := processors.Where(&c.Table, c.Where)

	rowCount := 0
	yield := make(chan struct{})
	for rowId := range where.Reverse().Process(yield) {
		_, err := idbFile.Seek((rowId+1)*c.Table.RowLength, 0)
		if err != nil {
			continue
		}
		data, _ := reader.ReadBytes('\n')

		idbFile.Seek(rowId*c.Table.RowLength, 0)
		writer.Write(data)

		idbFile.Truncate(c.Table.RowLength)
		rowCount++
	}

	return "CODE 200: deleted " + string(rowCount) + " records"
}

package command

import (
	"DBMS/storage"
	"DBMS/storage/processors"
	"DBMS/storage/value"
	"bufio"
	"errors"
	"fmt"
	"os"
	"syscall"
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
	idbFileStat, _ := idbFile.Stat()
	defer func() {
		idbFile.Close()
		syscall.Flock(int(idbFile.Fd()), syscall.LOCK_UN)
	}()

	err = syscall.Flock(int(idbFile.Fd()), syscall.LOCK_EX)
	if err != nil {
		return err
	}

	where := processors.Where(&c.Table, c.Where)

	rowCount := int64(0)
	yield := make(chan struct{})
	for rowId := range where.Reverse().Process(yield) {
		_, err := idbFile.Seek((rowId+1)*c.Table.RowLength, 0)
		if err != nil {
			continue
		}
		reader := bufio.NewReader(idbFile)
		data, err := reader.ReadBytes('\n')

		_, err = idbFile.WriteAt(data, rowId*c.Table.RowLength)
		if err != nil {
			return nil
		}
		rowCount++
	}

	idbFile.Truncate(idbFileStat.Size() - rowCount*c.Table.RowLength)

	return "CODE 200: deleted " + fmt.Sprint(rowCount) + " records"
}

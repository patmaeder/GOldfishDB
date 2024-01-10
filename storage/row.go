package storage

import "DBMS/storage/value"

type Row struct {
	Entries map[[128]byte]value.Value
}

func NewRow(entries map[[128]byte]value.Value) Row {
	return Row{
		Entries: entries,
	}
}

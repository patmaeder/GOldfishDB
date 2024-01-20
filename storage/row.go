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

func (r Row) ToString() string {
	i := 0
	string := ""

	for _, value := range r.Entries {
		string += value.ToString()
		if i+1 != len(r.Entries) {
			string += ";"
		}
		i++
	}

	return string
}

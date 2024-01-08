package storage

type Row struct {
	Entries map[[128]byte]any
}

func NewRow(entries map[[128]byte]any) Row {
	/*convertedEntries := make(map[[128]byte]any)

	for columnName, value := range entries {
		columnNameBytes := []byte(columnName)
		columnNameByteArray := append(columnNameBytes, make([]byte, 128-len(columnNameBytes))...)
		convertedEntries[[128]byte(columnNameByteArray)] = value
	}*/

	return Row{
		Entries: entries,
	}
}

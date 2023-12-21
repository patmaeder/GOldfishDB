package storage

type Table struct {
	Database Database
	Name     string
}

func (t *Table) GetFrmPath() string {
	return t.Database.GetPath() + "/" + t.Name + ".frm"
}

func (t *Table) GetIdbPath() string {
	return t.Database.GetPath() + "/" + t.Name + ".idb"
}

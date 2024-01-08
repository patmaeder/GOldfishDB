package command

import (
	"errors"
	"os"
)

type DropTableCommand struct {
	Name     string
	IfExists bool
}

func DropTable(name string, ifExists bool) *DropTableCommand {
	return &DropTableCommand{
		Name:     name,
		IfExists: ifExists,
	}
}

func (c *DropTableCommand) Execute() error {
	FrmFilePath := os.Getenv("DATA_DIR") + "/" + c.Name + ".frm"
	IdbFilePath := os.Getenv("DATA_DIR") + "/" + c.Name + ".idb"

	_, err1 := os.Stat(FrmFilePath)
	_, err2 := os.Stat(IdbFilePath)
	if err := errors.Join(err1, err2); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if c.IfExists {
				return nil
			}
			return errors.New("a table with the provided name does not exists")
		}
		return err
	}

	err1 = os.Remove(FrmFilePath)
	err2 = os.Remove(IdbFilePath)
	if err := errors.Join(err1, err2); err != nil {
		return err
	}

	return nil
}

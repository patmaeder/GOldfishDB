package database

import (
	"errors"
	"os"
)

type Database struct {
	Name string
	Path string
}

func New(name string) (Database, error) {
	_, err := os.Stat(os.Getenv("DATA_DIR") + "/" + name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Database{}, errors.New("database does not exist")
		}
		return Database{}, err
	}

	return Database{Name: name, Path: os.Getenv("DATA_DIR") + "/" + name}, nil
}

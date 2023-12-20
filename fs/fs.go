package fs

import (
	"os"
	"regexp"
	"strings"
	"sync"
)

var mtx sync.Mutex

func Create(path string) error {
	if _, err := os.Stat(path); err == nil {
		return os.ErrExist
	}

	match, _ := regexp.MatchString(`^(\/?[\w\W]+\/?)+([^/]+\.)(\w+)$`, path)
	if !match {
		return os.MkdirAll(path, 0777)
	}

	directoryStructure := path[:strings.LastIndex(path, "/")]

	err := os.MkdirAll(directoryStructure, 0777)
	if err != nil {
		return err
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}

	defer file.Close()

	return nil
}

func read(path string, b []byte, offset int64) error {
	file, err := os.OpenFile(path, os.O_RDONLY, 0444)
	if err != nil {
		return err
	}

	defer file.Close()

	_, err = file.ReadAt(b, offset)
	return err
}

func ReadAll(path string, b []byte) error {
	fileStat, err := os.Stat(path)
	if err != nil {
		return err
	}

	return read(path, make([]byte, fileStat.Size()), 0)
}

func Read(path string, b []byte) error {
	return read(path, b, 0)
}

func ReadAt(path string, b []byte, offset int64) error {
	return read(path, b, offset)
}

func write(path string, data []byte, offset int64) error {
	file, err := os.OpenFile(path, os.O_WRONLY, 0666)
	if err != nil {
		return err
	}

	defer func() {
		file.Sync()
		mtx.Unlock()
		file.Close()
	}()

	mtx.Lock()
	_, err = file.WriteAt(data, offset)
	return err
}

func Write(path string, data []byte) error {
	fileStat, err := os.Stat(path)
	if err != nil {
		return err
	}

	return write(path, data, fileStat.Size())
}

func WriteAt(path string, data []byte, offset int64) error {
	return write(path, data, offset)
}

func Delete(path string) error {
	_, err := os.Stat(path)
	if err != nil {
		return err
	}

	return os.RemoveAll(path)
}

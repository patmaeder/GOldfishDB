package fs

import (
	"os"
	"regexp"
	"strings"
	"sync"
)

var mtx sync.Mutex

func Create(path string) error {
	containsFilenamePattern := `^(\/?[\w\W]+\/?)+([^/]+\.)(\w+)$`
	match, _ := regexp.MatchString(containsFilenamePattern, path)

	if match {
		directoryStructure := path[:strings.LastIndex(path, "/")]

		print(path)

		err := os.MkdirAll(directoryStructure, 0777)
		if err != nil {
			return err
		}

		file, err := os.Create(path)
		if err != nil {
			return err
		}

		defer func() {
			_ = file.Close()
		}()

		return nil
	}

	err := os.MkdirAll(path, 0777)
	if err != nil {
		return err
	}

	return nil
}

func read(filePath string, b []byte, offset int64) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}

	// TODO: Neat to show (auto close file at end and unlock)
	defer func() {
		mtx.Unlock()
		_ = file.Close()
	}()

	mtx.Lock()
	_, err = file.ReadAt(b, offset)
	if err != nil {
		return err
	}

	return nil
}

func ReadAll(filePath string, b []byte) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}

	fileStat, err := file.Stat()
	_ = file.Close()
	return read(filePath, make([]byte, fileStat.Size()), 0)
}

func Read(filePath string, b []byte) error {
	return read(filePath, b, 0)
}

func ReadAt(filePath string, b []byte, offset int64) error {
	return read(filePath, b, offset)
}

func write(filePath string, data []byte, offset int64) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}

	defer func() {
		mtx.Unlock()
		_ = file.Close()
	}()

	mtx.Lock()
	_, err = file.WriteAt(data, offset)

	if err != nil {
		return err
	}

	_ = file.Sync()
	return nil
}

func Write(filePath string, data []byte) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}

	fileStat, err := file.Stat()
	_ = file.Close()
	return write(filePath, data, fileStat.Size())
}

func WriteAt(filePath string, data []byte, offset int64) error {
	return write(filePath, data, offset)
}

func Delete(filePath string) error {
	err := os.Remove(filePath)
	if err != nil {
		return err
	}

	return nil
}

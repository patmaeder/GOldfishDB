package utils

import (
	"bytes"
	"errors"
	"strings"
)

func StringToByteArray(string string, length int) ([]byte, error) {
	stringBytes := []byte(string)
	if len(stringBytes) > length {
		return nil, errors.New("provided string longer than desired array length")
	}
	buffer := make([]byte, length-len(stringBytes))

	return append(stringBytes, buffer...), nil
}

func ByteArrayToString(byteArray []byte) string {
	buffer := bytes.NewBuffer(byteArray)
	trimmedString := strings.TrimRight(buffer.String(), string(0))
	return trimmedString
}

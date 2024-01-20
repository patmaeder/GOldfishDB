package utils

import "errors"

func StringToByteArray(string string, length int) ([]byte, error) {
	stringBytes := []byte(string)
	if len(stringBytes) > length {
		return nil, errors.New("provided string longer than desired array length")
	}
	buffer := make([]byte, length-len(stringBytes))

	return append(stringBytes, buffer...), nil
}

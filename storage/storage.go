package storage

import (
	"bytes"
	"io"
	"strconv"
)

type Storage interface {
	PutObject(key string, buf io.Reader) error
	GetObject(key string) ([]byte, error)
	DeleteObject(key string) error
}

func PutEmptyObject(s Storage, key string) error {
	buf := bytes.NewBuffer([]byte{})
	err := s.PutObject(key, buf)
	return err
}

func PutInt(s Storage, key string, num int) error {
	buf := bytes.NewBufferString(strconv.Itoa(num))
	err := s.PutObject(key, buf)
	return err
}

func GetInt(s Storage, key string, defaultVal int) (int, error) {
	buf, err := s.GetObject(key)
	if err != nil {
		serr, ok := err.(*Error)
		if ok && serr.IsErrNoSuchKey() {
			return defaultVal, nil
		}
		return 0, err
	}
	return strconv.Atoi(string(buf))
}

func ExistsObject(s Storage, key string) (bool, error) {
	_, err := s.GetObject(key)
	if err != nil {
		serr, ok := err.(*Error)
		if ok && serr.IsErrNoSuchKey() {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

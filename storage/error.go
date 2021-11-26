package storage

import "fmt"

var (
	ErrCodeNoSuchKey = 1
	ErrOther         = 0
)

type Error struct {
	code int
	err  error
}

func (e *Error) Error() string {
	switch e.code {
	case ErrCodeNoSuchKey:
		return fmt.Sprintf("ErrCodeNoSuchKey")
	}
	return e.err.Error()
}

func (e *Error) IsErrNoSuchKey() bool {
	return e.code == ErrCodeNoSuchKey
}

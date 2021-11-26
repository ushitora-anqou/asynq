package storage

import (
	"errors"
	"math/rand"
	"strconv"
	"time"
)

func getEnteringKey(index int) string {
	return "Entering_" + strconv.Itoa(index)
}

func getNumberKey(index int) string {
	return "Number_" + strconv.Itoa(index)
}

func sleepRandomTime() {
	seconds := rand.Intn(10) + 5
	time.Sleep(time.Duration(seconds) * time.Second)
}

type Mutex struct {
	stor             Storage
	locking          bool
	procID, numProcs int
}

func NewMutex(stor Storage, procID, numProcs int) *Mutex {
	return &Mutex{
		stor:     stor,
		locking:  false,
		procID:   procID,
		numProcs: numProcs,
	}
}

func (m *Mutex) Lock() error {
	if m.locking {
		return errors.New("Can't lock locking mutex")
	}

	// Entering[procID] = true
	keyMyEntering := getEnteringKey(m.procID)
	err := PutEmptyObject(m.stor, keyMyEntering)
	if err != nil {
		return err
	}

	// Number[procID] = 1 + max_i Number[i]
	maxNum := 0
	for i := 0; i < m.numProcs; i++ {
		keyNumber := getNumberKey(i)
		num, err := GetInt(m.stor, keyNumber, 0)
		if err != nil {
			return err
		}
		if num > maxNum {
			maxNum = num
		}
	}
	keyNumber := getNumberKey(m.procID)
	numberProc := 1 + maxNum
	err = PutInt(m.stor, keyNumber, numberProc)
	if err != nil {
		return err
	}

	// Entering[procID] = false
	m.stor.DeleteObject(keyMyEntering)

	for i := 0; i < m.numProcs; i++ {
		keyEntering := getEnteringKey(i)
		keyNumber := getNumberKey(i)

		// while (Entering[i]) ;
		for {
			exists, err := ExistsObject(m.stor, keyEntering)
			if err != nil {
				return err
			}
			if !exists {
				break
			}
			sleepRandomTime()
		}

		// while ((Number[i] != 0) && ((Number[i], i) < (Number[procID], procID))) ;
		for {
			num, err := GetInt(m.stor, keyNumber, 0)
			if err != nil {
				return err
			}
			if !(num != 0 && (num < numberProc || (num == numberProc && i < m.procID))) {
				break
			}
			sleepRandomTime()
		}
	}

	m.locking = true

	return nil
}

func (m *Mutex) Unlock() error {
	if !m.locking {
		return errors.New("Can't unlock not-locking mutex")
	}

	// Number[procID] = 0
	keyNumber := getNumberKey(m.procID)
	err := m.stor.DeleteObject(keyNumber)
	if err != nil {
		return err
	}

	m.locking = false

	return nil
}

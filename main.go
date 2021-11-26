package main

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/ushitora-anqou/asynq/storage"
)

func bench(stor storage.Storage, procID, numProcs int) error {
	var err error

	mtx := storage.NewMutex(stor, procID, numProcs)

	numUploading := 10
	for i := 0; i < numUploading; i++ {
		// Lock
		log.Printf("Lock %d %d\n", procID, i)
		err = mtx.Lock()
		if err != nil {
			return err
		}

		// Update NEXT file
		log.Printf("Update NEXT file%d %d\n", procID, i)
		no, err := storage.GetInt(stor, "NEXT", 1)
		if err != nil {
			return err
		}
		err = storage.PutInt(stor, "NEXT", no+1)
		if err != nil {
			return err
		}

		// Upload data_no_procID file
		log.Printf("Upload data file%d %d\n", procID, i)
		key := fmt.Sprintf("data_%d_%d", no, procID)
		err = storage.PutEmptyObject(stor, key)
		if err != nil {
			return err
		}

		// Unlock
		log.Printf("Unlock %d %d\n", procID, i)
		err = mtx.Unlock()
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	awsAccessKeyID, exists := os.LookupEnv("AWS_ACCESS_KEY_ID")
	if !exists {
		log.Fatalf("Set envvar AWS_ACCESS_KEY_ID\n")
	}
	awsSecretAccessKey, exists := os.LookupEnv("AWS_SECRET_ACCESS_KEY")
	if !exists {
		log.Fatalf("Set envvar S3_API_SECRET\n")
	}
	s3Endpoint, exists := os.LookupEnv("S3_ENDPOINT")
	if !exists {
		log.Fatalf("Set envvar S3_ENDPOINT\n")
	}
	s3Bucket, exists := os.LookupEnv("S3_BUCKET")
	if !exists {
		log.Fatalf("Set envvar S3_BUCKET\n")
	}
	s3Region, exists := os.LookupEnv("S3_REGION")
	if !exists {
		log.Fatalf("Set envvar S3_REGION\n")
	}

	stor := storage.NewS3(&storage.S3Config{
		AccessKeyID:     awsAccessKeyID,
		SecretAccessKey: awsSecretAccessKey,
		Endpoint:        s3Endpoint,
		Region:          s3Region,
		Bucket:          s3Bucket,
	})

	numProcs := 5
	var wg sync.WaitGroup
	for i := 0; i < numProcs; i++ {
		wg.Add(1)
		go func(procID int) {
			err := bench(stor, procID, numProcs)
			if err != nil {
				log.Fatal(err)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

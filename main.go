package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func upload(uploader *s3manager.Uploader, bucket, filepath string, buf *bytes.Buffer) error {
	var err error
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filepath),
		Body:   buf,
	})
	if err != nil {
		return err
	}

	return nil
}

func listFilesPrefix(svc *s3.S3, bucket, prefix string) ([]string, error) {
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: &prefix,
	})
	if err != nil {
		return nil, err
	}

	list := make([]string, 0)
	for _, item := range resp.Contents {
		list = append(list, *item.Key)
	}

	return list, nil
}

func listFiles(svc *s3.S3, bucket string) ([]string, error) {
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	if err != nil {
		return nil, err
	}

	list := make([]string, 0)
	for _, item := range resp.Contents {
		list = append(list, *item.Key)
	}

	return list, nil
}

func print_list(svc *s3.S3, bucket string) error {
	// Get the list of items
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	if err != nil {
		return err
	}

	for _, item := range resp.Contents {
		fmt.Println("Name:         ", *item.Key)
		fmt.Println("Last modified:", *item.LastModified)
		fmt.Println("Size:         ", *item.Size)
		fmt.Println("Storage class:", *item.StorageClass)
		fmt.Println("")
	}

	return nil
}

func uploadEmptyFile(uploader *s3manager.Uploader, bucket, filepath string) error {
	buf := bytes.NewBuffer([]byte{})

	var err error
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filepath),
		Body:   buf,
	})
	if err != nil {
		return err
	}

	return nil
}

func removeFile(svc *s3.S3, bucket, filepath string) error {
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filepath),
	})
	return err
}

func sleepRandomTime() {
	seconds := rand.Intn(10) + 5
	time.Sleep(time.Duration(seconds) * time.Second)
}

type S3Mutex struct {
	svc              *s3.S3
	uploader         *s3manager.Uploader
	downloader       *s3manager.Downloader
	bucket           string
	procId, numProcs int
	locking          bool
	numberFile       string
}

func NewS3Mutex(svc *s3.S3, uploader *s3manager.Uploader, downloader *s3manager.Downloader, bucket string, procId, numProcs int) *S3Mutex {
	return &S3Mutex{
		svc, uploader, downloader, bucket, procId, numProcs, false, "",
	}
}

func (m *S3Mutex) Lock() error {
	if m.locking {
		return errors.New("Can't lock locking mutex")
	}

	// Entering[i] = true
	fileEnteringI := fmt.Sprintf("Entering_%d", m.procId)
	err := uploadEmptyFile(m.uploader, m.bucket, fileEnteringI)
	if err != nil {
		return err
	}

	// Number[i] = 1 + max_i Number[i]
	files, err := listFiles(m.svc, m.bucket)
	if err != nil {
		return err
	}
	maxNum := 0
	for _, file := range files {
		sp := strings.Split(file, "_")
		if sp[0] != "Number" {
			continue
		}
		num, err := strconv.Atoi(sp[2])
		if err != nil {
			return err
		}
		if num > maxNum {
			maxNum = num
		}
	}
	numberProc := 1 + maxNum
	numberFile := fmt.Sprintf("Number_%d_%d", m.procId, numberProc)
	err = uploadEmptyFile(m.uploader, m.bucket, numberFile)
	if err != nil {
		return err
	}

	// Entering[i] = false
	removeFile(m.svc, m.bucket, fileEnteringI)

	for i := 0; i < m.numProcs; i++ {
		// while (Entering[i]) ;
		for {
			prefix := fmt.Sprintf("Entering_%d", i)
			list, err := listFilesPrefix(m.svc, m.bucket, prefix)
			if err != nil {
				return err
			}
			if len(list) == 0 {
				break
			}
			sleepRandomTime()
		}

		// while ((Number[i] != 0) && ((Number[i], i) < (Number[procId], procId))) ;
		for {
			prefix := fmt.Sprintf("Number_%d_", i)
			list, err := listFilesPrefix(m.svc, m.bucket, prefix)
			if err != nil {
				return err
			}
			if len(list) == 0 { // Number[i] == 0
				break
			}
			sp := strings.Split(list[0], "_")
			numberI, err := strconv.Atoi(sp[2])
			if err != nil {
				return err
			}
			if !(numberI < numberProc || (numberI == numberProc && i < m.procId)) {
				break
			}
			sleepRandomTime()
		}
	}

	m.numberFile = numberFile
	m.locking = true

	return nil
}

func (m *S3Mutex) Unlock() error {
	if !m.locking {
		return errors.New("Can't unlock not-locking mutex")
	}

	// Number[procId] = 0
	err := removeFile(m.svc, m.bucket, m.numberFile)
	if err != nil {
		return err
	}
	m.locking = false
	return nil
}

func bench(svc *s3.S3, uploader *s3manager.Uploader, downloader *s3manager.Downloader, bucket string, procId, numProcs int) error {
	file, err := os.Open("test.txt")
	if err != nil {
		return err
	}
	b, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(b)
	file.Close()

	s3mtx := NewS3Mutex(svc, uploader, downloader, bucket, procId, numProcs)

	numUploading := 10
	for i := 0; i < numUploading; i++ {
		s3mtx.Lock()

		list, err := listFilesPrefix(svc, bucket, "data_")
		if err != nil {
			return err
		}

		maxFileNo := 100
		for _, name := range list {
			elm := strings.Split(name, "_")
			fileNo, err := strconv.Atoi(elm[1])
			if err != nil {
				return err
			}
			if fileNo > maxFileNo {
				maxFileNo = fileNo
			}
		}

		newFileNo := maxFileNo + 1
		fmt.Printf("%d %d\n", newFileNo, procId)
		newFileName := fmt.Sprintf("data_%d_%d", newFileNo, procId)
		upload(uploader, bucket, newFileName, buf)

		s3mtx.Unlock()
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

	sess, _ := session.NewSession(&aws.Config{
		Credentials:                   credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, ""),
		Region:                        aws.String(s3Region),
		Endpoint:                      aws.String(s3Endpoint),
		CredentialsChainVerboseErrors: aws.Bool(true),
		S3ForcePathStyle:              aws.Bool(true),
	})

	numProcs := 5
	var wg sync.WaitGroup
	for i := 0; i < numProcs; i++ {
		wg.Add(1)
		go func(procId int) {
			uploader := s3manager.NewUploader(sess)
			downloader := s3manager.NewDownloader(sess)
			svc := s3.New(sess)
			err := bench(svc, uploader, downloader, s3Bucket, procId, numProcs)
			if err != nil {
				log.Fatal(err)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

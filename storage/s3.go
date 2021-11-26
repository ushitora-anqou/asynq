package storage

import (
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3 struct {
	svc        *s3.S3
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
	bucket     string
}

type S3Config struct {
	AccessKeyID, SecretAccessKey, Endpoint, Region, Bucket string
}

func NewS3(config *S3Config) *S3 {
	cred := credentials.NewStaticCredentials(config.AccessKeyID, config.SecretAccessKey, "")
	sess, _ := session.NewSession(&aws.Config{
		Credentials:                   cred,
		Region:                        aws.String(config.Region),
		Endpoint:                      aws.String(config.Endpoint),
		CredentialsChainVerboseErrors: aws.Bool(true),
		S3ForcePathStyle:              aws.Bool(true),
	})
	svc := s3.New(sess)
	return &S3{
		svc:        svc,
		uploader:   s3manager.NewUploader(sess),
		downloader: s3manager.NewDownloader(sess),
		bucket:     config.Bucket,
	}
}

func (s *S3) PutObject(key string, buf io.Reader) error {
	_, err := s.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   buf,
	})
	if err != nil {
		return &Error{ErrOther, err}
	}
	return nil
}

func (s *S3) GetObject(key string) ([]byte, error) {
	buf := aws.NewWriteAtBuffer([]byte{})
	_, err := s.downloader.Download(buf, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				return nil, &Error{ErrCodeNoSuchKey, nil}
			}
		}
		return nil, &Error{ErrOther, err}
	}
	return buf.Bytes(), nil
}

func (s *S3) DeleteObject(key string) error {
	_, err := s.svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	return err
}

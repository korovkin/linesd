package linesd

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	ERR_S3_FAILURE = errors.New("can't establish S3 connection")
)

func S3PutLocalFile(
	region *string,
	timeout time.Duration,
	bucketName string,
	localFilename string,
	remoteFilename string,
	contentType string) (*s3.PutObjectOutput, string, error) {

	S := time.Now()
	defer func() {
		log.Println("LINESD: S3PutLocalFile: DT:", time.Since(S).Seconds(), bucketName, remoteFilename)
	}()

	ctx := context.Background()
	var ctxCancelFn func()
	if timeout > 0 {
		ctx, ctxCancelFn = context.WithTimeout(ctx, timeout)
	}
	defer ctxCancelFn()

	var res *s3.PutObjectOutput = nil

	sess, err := session.NewSession(&aws.Config{Region: aws.String(*region)})
	CheckNotFatal(err)
	if err != nil {
		return res, remoteFilename, err
	}

	conf := aws.Config{Region: aws.String(*region)}
	svc := s3.New(sess, &conf)

	if svc == nil {
		return res, remoteFilename, ERR_S3_FAILURE
	}

	f, err := os.Open(localFilename)
	if f != nil {
		defer f.Close()
	}

	CheckNotFatal(err)
	if err != nil {
		return res, remoteFilename, err
	}

	params := &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(remoteFilename),
		Body:        f,
		ContentType: &contentType,
	}

	res, err = svc.PutObjectWithContext(ctx, params)
	CheckNotFatal(err)

	if err != nil {
		return res, remoteFilename, err
	}

	return res, remoteFilename, err
}

func S3PutBlob(
	region *string,
	timeout time.Duration,
	bucketName string,
	blob []byte,
	remoteFilename string,
	contentType string,
) (*s3.PutObjectOutput, string, error) {

	S := time.Now()
	defer func() {
		log.Println(
			"LINESD: S3PutBlob:", "DT:", time.Since(S).Seconds(), bucketName, remoteFilename)
	}()

	ctx := context.Background()
	var ctxCancelFn func()
	if timeout > 0 {
		ctx, ctxCancelFn = context.WithTimeout(ctx, timeout)
	}
	defer ctxCancelFn()

	var res *s3.PutObjectOutput = nil
	tfilesURL := ""

	sess, err := session.NewSession(&aws.Config{Region: aws.String(*region)})
	CheckNotFatal(err)

	if err != nil {
		return res, remoteFilename, err
	}

	conf := aws.Config{Region: aws.String(*region)}
	svc := s3.New(sess, &conf)

	if svc == nil {
		return res, remoteFilename, ERR_S3_FAILURE
	}

	CheckNotFatal(err)
	if err != nil {
		return res, tfilesURL, err
	}

	params := &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(remoteFilename),
		Body:        bytes.NewReader(blob),
		ContentType: &contentType,
	}

	res, err = svc.PutObjectWithContext(ctx, params)
	CheckNotFatal(err)

	if err != nil {
		return res, remoteFilename, err
	}

	return res, remoteFilename, err
}

func S3PutReader(
	region *string,
	bucketName string,
	blob io.ReadSeeker,
	remoteFilename string,
	contentType string,
) (*s3.PutObjectOutput, error) {
	// S := time.Now()
	// defer func() {
	// 	log.Println(
	// 		"S3PutReader:",
	// 		"DT:",
	// 		time.Since(S))
	// }()

	var res *s3.PutObjectOutput = nil

	sess, err := session.NewSession(&aws.Config{Region: aws.String(*region)})
	CheckNotFatal(err)

	if err != nil {
		return res, err
	}

	conf := aws.Config{Region: aws.String(*region)}
	svc := s3.New(sess, &conf)

	if svc == nil {
		return res, errors.New("can't establish S3 connection")
	}

	CheckNotFatal(err)
	if err != nil {
		return res, err
	}

	params := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(remoteFilename),
		Body:   blob,
	}

	params.ContentType = &contentType
	res, err = svc.PutObject(params)
	CheckNotFatal(err)

	if err != nil {
		return res, err
	}

	return res, nil
}

func S3Head(region *string, bucketName string, remoteFilename string) (bool, error) {
	doesExists := false
	sess, err := session.NewSession(&aws.Config{Region: aws.String(*region)})
	CheckFatal(err)
	if err != nil {
		return doesExists, err
	}

	conf := aws.Config{Region: aws.String(*region)}
	svc := s3.New(sess, &conf)

	if svc == nil {
		return doesExists, errors.New("can't establish S3 connection")
	}

	params := &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(remoteFilename),
	}

	_, err = svc.HeadObject(params)
	if err == nil {
		doesExists = true
	} else if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "NotFound" {
		doesExists = false
		err = nil
	} else {
		CheckNotFatal(err)
	}

	return doesExists, err
}

func S3Get(region *string, bucketName string, key string) ([]byte, string, error) {
	var content []byte
	var contentType string = ""

	// S := time.Now()
	// defer func() {
	// 	DT := time.Since(S)
	// 	log.Println("S3Get: DT:", DT.Seconds(), bucketName, key)
	// }()

	sess, err := session.NewSession(&aws.Config{Region: aws.String(*region)})
	CheckNotFatal(err)
	if err != nil {
		return content, contentType, err
	}

	svc := s3.New(sess, &aws.Config{Region: aws.String(*region)})

	if svc == nil {
		return content, contentType, errors.New("can't establish S3 connection")
	}

	params := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}

	res, err := svc.GetObject(params)
	if err != nil {
		return content, contentType, err
	}
	defer res.Body.Close()

	if res.ContentType != nil {
		contentType = *res.ContentType
	}
	content, err = ioutil.ReadAll(res.Body)
	if err != nil {
		return content, contentType, err
	}

	return content, contentType, err
}

func S3GetToLocalfile(region *string, bucketName string, key string, localFilename string) (string, error) {
	var contentType string = ""

	// S := time.Now()
	// defer func() {
	// 	DT := time.Since(S)
	// 	log.Println("S3GetToLocalfile: DT:", DT.Seconds(), bucketName, key, localFilename)
	// }()

	sess, err := session.NewSession(&aws.Config{Region: aws.String(*region)})
	CheckNotFatal(err)
	if err != nil {
		return contentType, err
	}

	svc := s3.New(sess, &aws.Config{Region: aws.String(*region)})

	if svc == nil {
		return contentType, errors.New("can't establish S3 connection")
	}

	params := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}

	res, err := svc.GetObject(params)
	if err != nil {
		return contentType, err
	}
	defer res.Body.Close()

	if res.ContentType != nil {
		contentType = *res.ContentType
	}

	localfile, err := os.OpenFile(localFilename, os.O_CREATE|os.O_WRONLY, 0666)
	CheckNotFatal(err)

	if err != nil {
		return contentType, err
	}

	defer localfile.Close()

	_, err = io.Copy(localfile, res.Body)
	CheckNotFatal(err)

	return contentType, err
}

func S3GetSignedURL(region *string, bucketName string, key string) (string, error) {
	svc := s3.New(session.New(&aws.Config{Region: region}))
	req, _ := svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	signedURL, err := req.Presign(24 * time.Hour)
	CheckNotFatal(err)

	if err != nil {
		return "", err
	}

	return signedURL, err
}

func S3Exists(region *string, bucketName string, remoteFilename string) (bool, *s3.HeadObjectOutput, error) {
	var output *s3.HeadObjectOutput

	exists := false
	sess, err := session.NewSession(&aws.Config{Region: aws.String(*region)})
	CheckFatal(err)
	if err != nil {
		return exists, output, err
	}

	conf := aws.Config{Region: aws.String(*region)}
	svc := s3.New(sess, &conf)

	if svc == nil {
		return exists, output, errors.New("can't establish S3 connection")
	}

	params := &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(remoteFilename),
	}

	output, err = svc.HeadObject(params)
	if err == nil {
		exists = true
	} else if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "NotFound" {
		exists = false
		err = nil
	} else {
		CheckNotFatal(err)
	}

	return exists, output, err
}

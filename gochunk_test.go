package gochunk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"strconv"
	"strings"
	"testing"
)

func TestNewChunkedUpload(t *testing.T) {
	assert := assert.New(t)
	fPath := "/home/tripg/workspace/OS-2.tiff"
	fPath = "/home/tripg/workspace/ubuntu.iso"

	ctx, cancel := context.WithCancel(context.Background())

	cu, err := NewChunkedUpload(fPath, WithCtx(ctx, cancel), WithNWorkers(4), WithUploadID(uuid.New()), WithHandlerFunc(func(request ChunkedUploadRequest) error {
		fmt.Println(request.Part, len(request.Content), request.Complete)
		return nil
	}))
	assert.NoError(err)

	err = cu.ChunkHandler()
	assert.NoError(err)
}

func TestNewChunkedUpload_Err(t *testing.T) {
	assert := assert.New(t)
	fPath := "/home/tripg/workspace/OS-2.tiff"

	ctx, cancel := context.WithCancel(context.Background())

	cu, err := NewChunkedUpload(fPath, WithCtx(ctx, cancel), WithNWorkers(4), WithUploadID(uuid.New()), WithHandlerFunc(func(request ChunkedUploadRequest) error {
		//fmt.Println(request.Part, len(request.Content), request.Complete)
		if request.Part == 10 {
			return errors.New("mock error")
		}
		return nil
	}))
	assert.NoError(err)

	err = cu.ChunkHandler()
	assert.Error(err)
}

func BenchmarkNewChunkedUpload(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fPath := "/home/tripg/workspace/OS-2.tiff"

		ctx, cancel := context.WithCancel(context.Background())

		cu, err := NewChunkedUpload(fPath, WithCtx(ctx, cancel), WithNWorkers(4), WithUploadID(uuid.New()), WithHandlerFunc(func(request ChunkedUploadRequest) error {
			//fmt.Println(request.Part, len(request.Content), request.Complete)
			return nil
		}))
		if err != nil {
			return
		}

		err = cu.ChunkHandler()
		if err != nil {
			return
		}
	}
}

func TestChunkedUpload_ChunkHandler(t *testing.T) {
	fPath := "/home/tripg/workspace/OS-2.tiff"
	fPath = "/home/tripg/workspace/ubuntu.iso"
	bucketName := "chunked-upload"

	assert := assert.New(t)

	minioClient, err := minio.New(
		"localhost:19000",
		&minio.Options{
			Creds: credentials.NewStaticV4("vinlab", "ax9F6q!8V@FdS6$%", ""),
		})
	assert.NoError(err)

	ctx, cancel := context.WithCancel(context.Background())

	cu, err := NewChunkedUpload(fPath, WithCtx(ctx, cancel), WithChunkSize(104_857_600), WithNWorkers(40), WithUploadID(uuid.New()), WithHandlerFunc(func(request ChunkedUploadRequest) error {
		_, err = minioClient.PutObject(
			context.Background(),
			bucketName,
			strings.ReplaceAll(request.UploadID.String(), "-", ".")+"."+strconv.Itoa(request.Part),
			bytes.NewReader(request.Content),
			int64(len(request.Content)),
			minio.PutObjectOptions{},
		)
		assert.NoError(err)
		return nil
	}))
	assert.NoError(err)

	err = cu.ChunkHandler()
	assert.NoError(err)
}

func TestChunkedUpload_ChunkHandler_2(t *testing.T) {
	//fPath := "/home/tripg/workspace/OS-2.tiff"
	//fPath = "/home/tripg/workspace/ubuntu.iso"

	fPaths := []string{
		"/home/tripg/workspace/OS-2.tiff",
		"/home/tripg/workspace/ubuntu.iso",
	}

	bucketName := "chunked-upload"

	assert := assert.New(t)

	minioClient, err := minio.New(
		"localhost:19000",
		&minio.Options{
			Creds: credentials.NewStaticV4("vinlab", "ax9F6q!8V@FdS6$%", ""),
		})
	assert.NoError(err)

	for _, fPath := range fPaths {
		fmt.Println("Process", fPath)
		ctx, cancel := context.WithCancel(context.Background())
		cu, err := NewChunkedUpload(fPath, WithCtx(ctx, cancel), WithNWorkers(20), WithUploadID(uuid.New()), WithHandlerFunc(func(request ChunkedUploadRequest) error {
			fmt.Println("processing", request.Part)
			_, err = minioClient.PutObject(
				context.Background(),
				bucketName,
				strings.ReplaceAll(request.UploadID.String(), "-", ".")+"."+strconv.Itoa(request.Part),
				bytes.NewReader(request.Content),
				int64(len(request.Content)),
				minio.PutObjectOptions{},
			)
			assert.NoError(err)
			return nil
		}))
		assert.NoError(err)

		err = cu.ChunkHandler()
		assert.NoError(err)
	}

	//cu, err := NewChunkedUpload(fPath, WithCtx(ctx, cancel), WithNWorkers(470), WithUploadID(uuid.New()), WithHandlerFunc(func(request ChunkedUploadRequest) error {
	//	fmt.Println("processing", request.Part)
	//	_, err = minioClient.PutObject(
	//		context.Background(),
	//		bucketName,
	//		strings.ReplaceAll(request.UploadID.String(), "-", ".")+"."+strconv.Itoa(request.Part),
	//		bytes.NewReader(request.Content),
	//		int64(len(request.Content)),
	//		minio.PutObjectOptions{},
	//	)
	//	assert.NoError(err)
	//	return nil
	//}))
	//assert.NoError(err)
	//
	//err = cu.ChunkHandler()
	//assert.NoError(err)
}

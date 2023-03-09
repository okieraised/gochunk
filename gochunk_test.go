package gochunk

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewChunkedUpload(t *testing.T) {
	assert := assert.New(t)
	fPath := "/home/tripg/workspace/OS-2.tiff"

	ctx, cancel := context.WithCancel(context.Background())

	cu, err := NewChunkedUpload(fPath, WithCtx(ctx, cancel), WithNWorkers(4), WithUploadID(uuid.New()), WithHandlerFunc(func(request ChunkedUploadRequest) error {
		//fmt.Println(request.Part, len(request.Content), request.Complete)
		return nil
	}))
	assert.NoError(err)

	defer cu.cancel()

	err = cu.ChunkedUploadHandler()
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

	defer cu.cancel()

	err = cu.ChunkedUploadHandler()
	assert.Error(err)
}

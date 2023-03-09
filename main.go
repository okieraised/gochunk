package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/gabriel-vasile/mimetype"
	"github.com/google/uuid"
	"io"
	"math"
	"os"
	"sync"
)

const (
	DefaultChunkSize = 10 * (1 << 20) // 10MiB
	DefaultNWorker   = 4
	DefaultMIMEType  = "application/octet-stream"
)

type ChunkedUploadRequest struct {
	UploadID uuid.UUID `json:"upload_id,omitempty"`
	Part     int       `json:"part,omitempty"`
	MD5      string    `json:"md5,omitempty"`
	Content  []byte    `json:"content,omitempty"`
	Complete bool      `json:"complete,omitempty"`
}

type ChunkedUpload struct {
	uploadID    uuid.UUID
	fPath       string
	mimeType    string
	chunkSize   int64
	nWorker     int
	ctx         context.Context
	cancel      context.CancelFunc
	handlerFunc func(ChunkedUploadRequest) error

	// internally used
	success int
	failure int
	total   int
	cUpload chan ChunkedUploadRequest
	cErr    chan error
}

func NewChunkedUpload(fPath string, opts ...func(*ChunkedUpload)) (*ChunkedUpload, error) {
	c := &ChunkedUpload{
		fPath:     fPath,
		nWorker:   DefaultNWorker,
		chunkSize: DefaultChunkSize,
	}

	mType, err := mimetype.DetectFile(fPath)
	if err != nil {
		return nil, err
	}
	c.mimeType = mType.String()

	for _, opt := range opts {
		opt(c)
	}
	c.cUpload = make(chan ChunkedUploadRequest, c.nWorker)
	c.cErr = make(chan error, c.nWorker)

	if c.uploadID == uuid.Nil {
		c.uploadID = uuid.New()
	}

	if c.ctx == nil {
		ctx, cancel := context.WithCancel(context.Background())
		c.ctx = ctx
		c.cancel = cancel
	}

	if c.handlerFunc == nil {
		return nil, errors.New("handler function to process chunks is required")
	}

	return c, nil
}

func WithChunkSize(chunkSize int64) func(*ChunkedUpload) {
	return func(c *ChunkedUpload) {
		if chunkSize <= 0 {
			chunkSize = DefaultChunkSize
		}
		c.chunkSize = chunkSize
		return
	}
}

func WithMimeType(mimeType string) func(*ChunkedUpload) {
	return func(c *ChunkedUpload) {
		c.mimeType = mimeType
	}
}

func WithNWorkers(n int) func(*ChunkedUpload) {
	return func(c *ChunkedUpload) {
		if n <= 0 {
			n = DefaultNWorker
		}
		c.nWorker = n
	}
}

func WithUploadID(id uuid.UUID) func(*ChunkedUpload) {
	return func(c *ChunkedUpload) {
		c.uploadID = id
	}
}

func WithCtx(ctx context.Context, cancelFunc context.CancelFunc) func(*ChunkedUpload) {
	return func(c *ChunkedUpload) {
		c.ctx = ctx
		c.cancel = cancelFunc
	}
}

func WithHandlerFunc(f func(ChunkedUploadRequest) error) func(upload *ChunkedUpload) {
	return func(c *ChunkedUpload) {
		c.handlerFunc = f
	}
}

func main() {
	fPath := "/home/tripg/workspace/OS-2.tiff"

	ctx, cancel := context.WithCancel(context.Background())

	cu, err := NewChunkedUpload(fPath, WithCtx(ctx, cancel), WithNWorkers(4), WithUploadID(uuid.New()), WithHandlerFunc(func(request ChunkedUploadRequest) error {
		fmt.Println(request.Part, len(request.Content), request.Complete)
		return nil
	}))
	if err != nil {
		fmt.Println(err)
		return
	}

	defer cu.cancel()

	err = cu.chunkedUploadHandler()
	if err != nil {
		fmt.Println(err)
		return
	}

}

func calChunkMD5(chunk []byte) string {
	checkSum := md5.Sum(chunk)
	return hex.EncodeToString(checkSum[:])
}

func (cu *ChunkedUpload) getNChunks(fInfo os.FileInfo) uint64 {
	return uint64(math.Ceil(float64(fInfo.Size()) / float64(cu.chunkSize)))
}

func (cu *ChunkedUpload) producer() {
	defer close(cu.cUpload)
	f, _ := os.Open(cu.fPath)
	defer f.Close()
	fInfo, err := f.Stat()
	if err != nil {
		cu.cErr <- err
		return
	}

	nChunks := cu.getNChunks(fInfo)
	cu.total = int(nChunks)

	for i := uint64(0); i < nChunks; i++ {
		select {
		case <-cu.ctx.Done():
			fmt.Println("Upload producer encountered an error")
			return
		default:
		}

		partSize := int(math.Min(float64(cu.chunkSize), float64(fInfo.Size()-int64(i)*cu.chunkSize)))
		partBuffer := make([]byte, partSize)
		_, err := f.Read(partBuffer)
		if err != nil {
			if err != io.EOF {
				cu.cErr <- err
				return
			}
		}
		md5Hash := calChunkMD5(partBuffer)

		chunkedReq := ChunkedUploadRequest{
			UploadID: cu.uploadID,
			Part:     int(i) + 1,
			MD5:      md5Hash,
			Content:  partBuffer,
		}

		if i == nChunks-1 {
			chunkedReq.Complete = true
		}
		cu.cUpload <- chunkedReq
	}
}
func (cu *ChunkedUpload) consumer(idx int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-cu.ctx.Done():
			fmt.Println("Worker", idx, "encountered an error")
			return
		default:
		}

		chunkedReq, ok := <-cu.cUpload
		if ok {
			err := cu.handlerFunc(chunkedReq)
			if err != nil {
				cu.failure++
				cu.cErr <- err
				cu.cancel()
				return
			} else {
				cu.success++
			}
		} else {
			return
		}
	}
}

func (cu *ChunkedUpload) chunkedUploadHandler() error {

	var wg sync.WaitGroup

	go cu.producer()

	for i := 1; i <= cu.nWorker; i++ {
		wg.Add(1)
		go cu.consumer(i, &wg)
	}

	wg.Wait()
	close(cu.cErr)

	for {
		err, ok := <-cu.cErr
		if ok {
			return err
		} else {
			break
		}
	}
	return nil
}

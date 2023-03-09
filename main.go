package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
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
	uploadID  uuid.UUID
	fPath     string
	mimeType  string
	chunkSize int64
	nWorker   int

	// internally used
	success int
	failure int
	total   int
	cUpload chan ChunkedUploadRequest
	cErr    chan error
	ctx     context.Context
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

func WithCtx(ctx context.Context) func(*ChunkedUpload) {
	return func(c *ChunkedUpload) {
		c.ctx = ctx
	}
}

func main() {
	fPath := "/home/tripg/workspace/OS-2.tiff"

	parts, err := getNumberOfChunk(fPath)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(parts)

	mtype, err := mimetype.DetectFile(fPath)
	fmt.Println(mtype.String(), mtype.Extension())

}

func getNumberOfChunk(filePath string) (uint64, error) {
	f, err := os.Stat(filePath)
	if err != nil {
		return 0, err
	}

	return uint64(math.Ceil(float64(f.Size()) / float64(DefaultChunkSize))), nil
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

	for i := uint64(0); i < nChunks; i++ {
		select {
		case <-cu.ctx.Done():
			fmt.Println("Upload producer encountered an error")
			return
		default:
		}

		partSize := int(math.Min(DefaultChunkSize, float64(fInfo.Size()-int64(i*DefaultChunkSize))))
		partBuffer := make([]byte, partSize)
		_, err := f.Read(partBuffer)
		if err != nil {
			if err != io.EOF {
				cu.cErr <- err
				return
			}
		}

		checkSum := md5.Sum(partBuffer)
		md5Hash := hex.EncodeToString(checkSum[:])

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
			fmt.Println("req", chunkedReq.Part)
		} else {
			return
		}
	}
}

//func (cu *ChunkedUpload) chunkedUploadHandler(uploadID uuid.UUID, filePath string) {
//
//	var wg2 sync.WaitGroup
//	cUpload := make(chan chunkedUploadRequest, uploadConfig.Concurrent)
//
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	go chunkGenerator(cUpload, ctx, uploadID, filePath)
//
//	for i := 1; i <= uploadConfig.Concurrent; i++ {
//		wg2.Add(1)
//		go chunkConsumer(i, cUpload, &wg2, filePath, ctx)
//	}
//
//	wg2.Wait()
//}

//func chunkConsumer(idx int, cUpload chan ChunkedUploadRequest, wg *sync.WaitGroup, filePath string, ctx context.Context) {
//	defer wg.Done()
//	for {
//		select {
//		case <-ctx.Done():
//			fmt.Println("Worker", idx, "encountered an error")
//			return
//		default:
//		}
//
//		chunkedReq, ok := <-cUpload
//		if ok {
//			_, code, err := shared.LabClient.UploadStudyByChunk(uploadConfig.Destination, chunkedReq)
//			if code != http.StatusCreated && code != http.StatusOK {
//				logger.Info(strings.Join([]string{fmt.Sprintf("%s (chunk %d)", filePath, chunkedReq.Part), "0", Err, err.Error()}, ","))
//			} else {
//				if chunkedReq.Complete {
//					uploadSuccess++
//				}
//				logger.Info(strings.Join([]string{fmt.Sprintf("%s (chunk %d)", filePath, chunkedReq.Part), "0", Ok}, ","))
//			}
//		} else {
//			return
//		}
//	}
//}
//
//func chunkedUploadHandler(uploadID uuid.UUID, filePath string) {
//
//	var wg2 sync.WaitGroup
//	cUpload := make(chan chunkedUploadRequest, uploadConfig.Concurrent)
//
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	go chunkGenerator(cUpload, ctx, uploadID, filePath)
//
//	for i := 1; i <= uploadConfig.Concurrent; i++ {
//		wg2.Add(1)
//		go chunkConsumer(i, cUpload, &wg2, filePath, ctx)
//	}
//
//	wg2.Wait()
//}

//func chunkProducer(ctx context.Context, cUpload chan ChunkedUploadRequest, uploadID uuid.UUID, filePath string) {
//	defer close(cUpload)
//	f, _ := os.Open(filePath)
//	defer f.Close()
//	fInfo, _ := f.Stat()
//
//	totalPartsNum, _ := getNumberOfChunk(filePath)
//
//	for i := uint64(0); i < totalPartsNum; i++ {
//		select {
//		case <-ctx.Done():
//			fmt.Println("Upload producer encountered an error")
//			return
//		default:
//		}
//
//		partSize := int(math.Min(DefaultChunkSize, float64(fInfo.Size()-int64(i*DefaultChunkSize))))
//		partBuffer := make([]byte, partSize)
//
//		f.Read(partBuffer)
//
//		checkSum := md5.Sum(partBuffer)
//		md5Hash := hex.EncodeToString(checkSum[:])
//
//		chunkedReq := ChunkedUploadRequest{
//			UploadID: uploadID,
//			Part:     int(i) + 1,
//			MD5:      md5Hash,
//			Content:  partBuffer,
//		}
//
//		if i == totalPartsNum-1 {
//			chunkedReq.Complete = true
//		}
//
//		cUpload <- chunkedReq
//	}
//}

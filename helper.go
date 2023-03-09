package gochunk

import (
	"crypto/md5"
	"encoding/hex"
)

func calChunkMD5(chunk []byte) string {
	checkSum := md5.Sum(chunk)
	return hex.EncodeToString(checkSum[:])
}

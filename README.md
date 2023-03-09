# gochunk

### Installation
To install the module, run the command:

```shell
go get -u github.com/okieraised/gochunk
```

### Usage

Example
```go
package main

import (
	"context"
	"fmt"
	
	"github.com/okieraised/gochunk"
	"github.com/google/uuid"
)

func main() {
	fPath := "/path/to/large/file"

	ctx, cancel := context.WithCancel(context.Background())

	cu, err := gochunk.NewChunkedUpload(fPath, 
		gochunk.WithCtx(ctx, cancel), 
		gochunk.WithNWorkers(4), 
		gochunk.WithUploadID(uuid.New()), 
		gochunk.WithHandlerFunc(func(request gochunk.ChunkedUploadRequest) error {
		return nil
	}))
	if err != nil {
		fmt.Println(err)
		return
    }

	err = cu.ChunkHandler()
	if err != nil {
		fmt.Println(err)
		return
	}
}
```
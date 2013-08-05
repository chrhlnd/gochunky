package main

import (
	"fmt"
	"bytes"
	"github.com/chrhlnd/gochunky/chunky"
)

// TESTING
const NUM_WORKERS = 1

func sesGotChunk( ses chunky.ChunkSession, chunk chunky.Chunk ) {
	d := chunk.GetData()

	fmt.Println("SERVER GOT: ", bytes.NewBuffer( d ).String() )

	b := new(bytes.Buffer)
	fmt.Fprintf(b,  "Got your %d bytes. bitch", len(d))

	ses.GetSend()( ses, b.Bytes(), nil )
}

func sesDone( ses chunky.ChunkSession ) {
	fmt.Println("sesDone!")
}

func beginSes( ses chunky.ChunkSession ) {
	ses.SetOnReceive( sesGotChunk )
	ses.SetOnClose( sesDone )
}

func main() {
	err := chunky.Serve( "0.0.0.0:8880", NUM_WORKERS, beginSes )
	if err != nil {
		fmt.Println("Got error:", err)
	}
}





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

	fmt.Println("Got Chunk data len",len(d))

	b := new(bytes.Buffer)
	fmt.Fprintf(b,  "Got your %d bytes. bitch!\r\n", len(d))

	ses.GetSend()( ses, b.Bytes(), nil )
}

func sesDone( ses chunky.ChunkSession ) {
	fmt.Println("sesDone!")
}

func beginSes( ses chunky.ChunkSession ) {
	ses.SetOnReceive( sesGotChunk )
	ses.SetOnClose( sesDone )
}

/*
func crazyWrite( out OutFn ) {
	b := new(bytes.Buffer)
	b.WriteString("KKK RAZY RIGHT\r\n")

	fmt.Println("Doing crazy line")
	out( b.Bytes() )
	fmt.Println("Done crazy line")
}
*/

/*
func chunkP( req ChunkService, d []byte, out OutFn ) {
	if len(d) > 0 {
		go crazyWrite( out )

		str := bytes.NewBuffer(d).String()
		fmt.Println("[",str,"]")
		b := new(bytes.Buffer)
		fmt.Fprintf(b,  "Got your %d bytes. bitch!\r\n", len(d))
		out( b.Bytes() )
	} else {
		fmt.Println("Closing got bytes of 0 size")
	}
}
*/


func main() {
	err := chunky.Serve( "0.0.0.0:8880", NUM_WORKERS, beginSes )
	if err != nil {
		fmt.Println("Got error:", err)
	}
}





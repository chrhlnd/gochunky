package main

import (
	"fmt"
	"bytes"
	"github.com/chrhlnd/gochunky/chunky"
)

// TESTING
const NUM_WORKERS = 1

var SENT int = 0

func sesGotChunk( ses chunky.ChunkSession, chunk chunky.Chunk ) {
	d := chunk.GetData()
	fmt.Println("CLIENT GOT: ", bytes.NewBuffer( d ).String() )

	if SENT == 0 {

		b := new(bytes.Buffer)
		fmt.Fprintf(b,  "I c ur bytes")

		ses.GetSend()( ses, b.Bytes(), nil )
	} else if SENT == 1 {
		fmt.Println("CLIENT closing...")
		ses.GetSend()( ses, (new (bytes.Buffer)).Bytes(), nil )
	}
	SENT++
}

func sesDone( ses chunky.ChunkSession ) {
	fmt.Println("sesDone!")
}

func beginSes( ses chunky.ChunkSession ) {
	ses.SetOnReceive( sesGotChunk )
	ses.SetOnClose( sesDone )
	fmt.Println("Began session, kicking off a cascade")

//type ChunkSender		func(ChunkSession,[]byte,map[string]string)(bool)
	buf := new (bytes.Buffer)
	buf.WriteString( "Hello from client" )
	ses.GetSend()( ses, buf.Bytes(), nil )
}

func main() {
//func Connect( ipport string, uri string, headers map[string]string, onbegin ChunkBeginSession)(error) {
	err := chunky.Connect( "127.0.0.1:8880", "/", nil, beginSes )
	if err != nil {
		fmt.Println("Got error:", err)
	}
}





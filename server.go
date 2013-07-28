package main

import (
	"net"
	"os"
	"fmt"
	"bytes"
	"strconv"
	"strings"
)

type ChunkService interface {
	URI()		(string)
	METHOD()	(string)
	PROTO()		(string)

	HEADERS()	(map[string]string)
	EXTENSIONS()(map[string]string)
	TRAILERS()	(map[string]string)
}

type OutFn func([]byte)

type Processor interface {
	request( in []byte, out OutFn )(bool)
}

type ChunkProcessor func ( req ChunkService, d []byte, out *bytes.Buffer )
type respond func( []byte )

type hState int
const (
	METHOD		= 0
	HEADERS		= 1
	BODY		= 2
	CHUNK		= 3
	BLANKLINE	= 4
	TRAILER		= 5
	DONE		= 6
)

type HTTPProcessor struct {
	state		hState
	nextstate	hState

	method		string
	uri			string
	proto		string
	headers		map[string]string
	extmap		map[string]string
	theaders	map[string]string

	resbuf	*bytes.Buffer
	inbuf	*bytes.Buffer

	chunksz	uint

	processor ChunkProcessor
}

var line_term			= [...]byte{0x0D,0x0A}
var LINE_TERM			= line_term[0:]
var kv_sep				= [...]byte{':'}
var KV_SEP				= kv_sep[0:]
var meth_sep			= [...]byte{' '}
var METH_SEP			= meth_sep[0:]
var extension_sep		= [...]byte{';'}
var EXTENSION_SEP		= extension_sep[0:]
var extension_kv_sep	= [...]byte{'='}
var EXTENSION_KV_SEP	= extension_kv_sep[0:]

const SPACE			= " "

func (self *HTTPProcessor) URI() (string) {
	return self.uri
}

func (self * HTTPProcessor) METHOD() (string) {
	return self.method
}

func (self *HTTPProcessor) PROTO() (string) {
	return self.proto
}

func (self *HTTPProcessor) HEADERS() (map[string]string) {
	return self.headers
}

func (self *HTTPProcessor) EXTENSIONS() (map[string]string) {
	return self.extmap
}

func (self *HTTPProcessor) TRAILERS() (map[string]string) {
	return self.theaders
}

func (self *HTTPProcessor) parseMethod() {
	if self.inbuf.Len() == 0 {
		return // need more data
	}

	data := self.inbuf.Bytes()

	var space int

	if self.method == "" {
		space = bytes.Index( data, METH_SEP )
		if space < 0 {
			return // need more data
		}
		self.method = bytes.NewBuffer( self.inbuf.Next( space ) ).String()
		self.inbuf.Next( len(METH_SEP) ) // advance pass seperator
	}

	if self.uri == "" {
		data = self.inbuf.Bytes()
		space = bytes.Index( data, METH_SEP )
		if space < 0 {
			return // more data
		}
		self.uri = bytes.NewBuffer( self.inbuf.Next( space ) ).String()
		self.inbuf.Next( len(METH_SEP) ) // advance pass the seperator
	}

	if self.proto == "" {
		data = self.inbuf.Bytes()
		space = bytes.Index( data, LINE_TERM )
		if space < 0 {
			return // more data
		}
		self.proto = bytes.NewBuffer( self.inbuf.Next(space) ).String()
		self.inbuf.Next( len(LINE_TERM) )
	}

	self.state = HEADERS
}


func (self *HTTPProcessor) parseHeaders() {
	if self.inbuf.Len() == 0 {
		return // more data
	}

	data := self.inbuf.Bytes()

	term := bytes.Index( data, LINE_TERM ) // look for a blank line
	if term == 0 {
		self.inbuf.Next( len(LINE_TERM) )
		self.state = BODY
		return
	}

	sep := bytes.Index( data, KV_SEP )

	if sep < 0 {
		return // don't have enough data yet
	}

	end := bytes.Index( data, LINE_TERM )

	if end < 0 {
		return // not enough data for a k,v pair
	}

	key := strings.Trim( bytes.NewBuffer( data[0:sep] ).String(), SPACE )
	val := strings.Trim( bytes.NewBuffer( data[sep+len(KV_SEP):end] ).String(), SPACE )

	if self.headers == nil { self.headers = make(map[string]string) }
	self.headers[key] = val

	// read out the data fron inbuf
	self.inbuf.Next( end + len(LINE_TERM) )
}

func (self *HTTPProcessor) parseTrailer() {
	data := self.inbuf.Bytes()

	if len(data) == 0 {
		self.state = DONE
		return // no trailer
	}

	term := bytes.Index( data, LINE_TERM )
	if term == 0 {
		self.inbuf.Next( len(LINE_TERM) )
		if self.processor != nil {
			sink := new (bytes.Buffer)
			self.processor( self, nil, sink )
		}
		self.state = DONE
		return
	}

	sep := bytes.Index( data, KV_SEP )
	if sep < 0 {
		return // more data
	}
	end := bytes.Index( data, LINE_TERM )
	if end < 0 {
		return // more data
	}

	key := strings.Trim( bytes.NewBuffer( data[0:sep] ).String(), SPACE )
	val := strings.Trim( bytes.NewBuffer( data[sep+len(KV_SEP):end] ).String(), SPACE )

	if self.theaders == nil { self.theaders = make(map[string]string) }
	self.theaders[key] = val

	// read out the data fron inbuf
	self.inbuf.Next( end + len(LINE_TERM) )
}

func (self *HTTPProcessor) parseExtensions()( map[string]string ) {
	var done	bool
	var ret		map[string]string

	for {
		data := self.inbuf.Bytes()

		eterm := bytes.Index( data, EXTENSION_SEP )
		if eterm < 0 {
			eterm = bytes.Index( data, LINE_TERM )
			done = true
		}

		var key string
		var val string

		eq := bytes.Index( data, EXTENSION_KV_SEP )
		if eq > -1 && eq < eterm {
			key = strings.Trim(bytes.NewBuffer( data[0:eq] ).String(), SPACE )
			val = strings.Trim(bytes.NewBuffer( data[eq+len(EXTENSION_KV_SEP):eterm] ).String(), SPACE )
		} else {
			key = strings.Trim(bytes.NewBuffer( data[0:eterm]).String(), SPACE )
			val = ""
		}

		ret = make(map[string]string)
		ret[key] = val

		self.inbuf.Next( eterm + len(EXTENSION_KV_SEP) ) // forward past the extension

		if done == true {
			break
		}
	}
	return ret
}

func (self *HTTPProcessor) parseChunkSz() {
	if self.inbuf.Len() == 0 {
		return // more data
	}

	data := self.inbuf.Bytes()

	term := bytes.Index( data, LINE_TERM ) //find a line terminator
	if term < 0 {
		return // not enough data yet
	}

	var octets string

	extension := bytes.Index( data, EXTENSION_SEP )
	if extension > -1 {
		octets = strings.Trim( bytes.NewBuffer( self.inbuf.Next( extension ) ).String(), SPACE )
		self.inbuf.Next( len(EXTENSION_SEP) )

		self.extmap = self.parseExtensions() // handles sucking up the LINE_TERM
	} else {
		octets = strings.Trim( bytes.NewBuffer( self.inbuf.Next( term ) ).String(), SPACE )
		self.inbuf.Next( len(LINE_TERM) ) // suckup the LINE_TERM
	}

	amt, err := strconv.ParseUint( octets, 16, 8 )
	if err != nil {
		self.state		= BLANKLINE
		self.nextstate	= BODY
		fmt.Println("error parsing chunksz ", err, " octets ", octets)
		return
	}
	self.state = CHUNK

	self.chunksz = uint(amt)
}

func (self *HTTPProcessor) startResponse( out OutFn ) {
	self.resbuf = new(bytes.Buffer)

	self.resbuf.WriteString( "HTTP/1.1 200 OK\r\n" )
	self.resbuf.WriteString( "Content-Type: application/json; charset=UTF-8\r\n" )
	self.resbuf.WriteString( "Transfer-Encoding: chunked\r\n" )
	self.resbuf.WriteString( "\r\n" )

	out( self.resbuf.Next( self.resbuf.Len() ) )
}

func (self *HTTPProcessor) endResponse( out OutFn ) {
	fmt.Println("endResponse")
	self.writeChunk(out)
}

func (self *HTTPProcessor) writeChunk( out OutFn ) {
	sizeDesc := new(bytes.Buffer)
	fmt.Fprintf(sizeDesc, "%X", self.resbuf.Len())
	sizeDesc.Write( LINE_TERM )
	self.resbuf.Write( LINE_TERM )
	out( sizeDesc.Next( sizeDesc.Len() ) )
	out( self.resbuf.Next(self.resbuf.Len() ) )
}

func (self *HTTPProcessor) parseChunk( out OutFn ) {
	if uint(self.inbuf.Len()) < self.chunksz {
		return // gimme more
	}

	d := self.inbuf.Next( int(self.chunksz) )
	if self.processor != nil {
		self.processor( self, d, self.resbuf )
		if self.resbuf.Len() > 0 {
			self.writeChunk( out )
		}
	}

	if self.chunksz == 0 {
		self.state		= BLANKLINE
		self.nextstate	= TRAILER
	} else {
		self.chunksz	= 0
		self.state		= BLANKLINE
		self.nextstate	= BODY
	}
}

func (self *HTTPProcessor) parseBlankLine() {
	data := self.inbuf.Bytes()

	pos := bytes.Index( data, LINE_TERM )
	if pos < 0 {
		return // keep sucking
	}

	data = self.inbuf.Next( pos + len( LINE_TERM ) )
	fmt.Println("DBG: tossing ", data, " Nextstate is ", self.nextstate)
	self.state = self.nextstate
}


func (self *HTTPProcessor) request( in []byte, out OutFn ) (bool) {
	if len(in) < 1 {
		return true
	}
	if self.inbuf == nil { self.inbuf = new(bytes.Buffer) }
	self.inbuf.Write(in)

	for {
		switch (self.state) {
			case METHOD:
				fmt.Println("Starting method...")
				self.parseMethod()
				break
			case HEADERS:
				fmt.Println("Starting headers...")
				self.parseHeaders()
				if self.state > HEADERS {
					fmt.Println("Starting response...")
					self.startResponse( out )
				}
				break
			case BODY:
				fmt.Println("Starting body...")
				self.parseChunkSz()
				break
			case CHUNK:
				fmt.Println("Starting chunk...")
				self.parseChunk( out )
				break
			case BLANKLINE:
				fmt.Println("Starting blankline...")
				self.parseBlankLine()
				break
			case TRAILER:
				fmt.Println("Starting trailer...")
				self.parseTrailer()
				break
			case DONE:
				self.endResponse( out )
				fmt.Println("Starting done...")
				fmt.Println("Closing data count in buffer is ", self.inbuf.Len())
				return true
		}
		if self.state != TRAILER && self.state != DONE {
			if self.inbuf.Len() < 1 {
				break
			}
		}
	}

	return false
}


const NUM_WORKERS = 1

func chunkP( req ChunkService, d []byte, out *bytes.Buffer ) {
	if len(d) > 0 {
		str := bytes.NewBuffer(d).String()
		fmt.Print(str)
		headers := req.HEADERS()
		fmt.Printf("\n--HEADERS--\n")
		for k,v := range(headers) {
			fmt.Printf( "%s = %s\n", k, v )
		}
		fmt.Printf("\n--HEADERS--\n")
		fmt.Fprintf(out, "Ok got your chunk of %d\r\n", len(d))
	} else {
		fmt.Println("Closing got bytes of 0 size")
	}
}

func main() {
	service := "0.0.0.0:8880"
	tcpAddr, err := net.ResolveTCPAddr("tcp4", service)
	checkError(err)

	listener, err := net.ListenTCP( "tcp", tcpAddr )
	checkError(err)

	passAccept := make( chan *net.TCPListener );

	done := make( chan bool );

	workerCount := NUM_WORKERS

	for i := 0; i < workerCount; i++ {
		go handleClient( i, passAccept, done, chunkP )
	}

	go handlePassAccept( passAccept, listener )

	for i := 0; i < workerCount; i++ {
		<-done
	}


	fmt.Println("Closing main listener")
	listener.Close()
}
func handlePassAccept( chanAccept chan *net.TCPListener, acceptor *net.TCPListener ) {
	chanAccept <- acceptor
}

func handleAccept( worker int, chanAccept chan *net.TCPListener, receiveConn chan *net.Conn ) {
	accept := <-chanAccept

	fmt.Println("Accepting on ", accept.Addr())
	conn, err := accept.Accept()
	checkError(err)

	receiveConn <- &conn

	go handlePassAccept( chanAccept, accept )
}

func handleClient( worker int, chanAccept chan *net.TCPListener, done chan bool, chunkCb ChunkProcessor ) {
	receiveConn := make( chan *net.Conn, 1 )

	spawn := 0

	go handleAccept( worker, chanAccept, receiveConn )

	var conn *net.Conn

	for {
		conn = <-receiveConn

		proc := new (HTTPProcessor)
		proc.processor = chunkCb

		go serviceClientConn( worker, conn, spawn, proc )
		go handleAccept( worker, chanAccept, receiveConn )
		spawn++;
	}

	done <- true
}

func serviceClientConn( parent int, conn *net.Conn, spawn int, processor Processor) {
	fmt.Println("Processing spawn ", spawn)
	var in [4096] byte
	var err2 error

	for {
		n, err := (*conn).Read( in[0:] )

		if err != nil {
			fmt.Fprintf(os.Stderr, "Read Fatal Error: %s\n", err.Error())
			break
		}

		isdone := processor.request( in[0:n], func ( out []byte ) {
			_, err2 = (*conn).Write( out )
			if err2 != nil {
				fmt.Println(os.Stderr, "Error writing to connection ", err2)
			}
		})

		if err2 != nil {
			fmt.Fprintf(os.Stderr, "Write Fatal Error: %s\n", err2.Error())
			break
		}

		if isdone {
			break
		}

		/*
		os.Stdout.Write( buf[0:n] )
		os.Stdout.Sync()
		*/
		/*
		_, err2 := (*conn).Write(buf[0:n])
		if err2 != nil {
			break
		}
		*/
	}

	fmt.Println("Closing spawn ", spawn)
	(*conn).Close()

}

func checkError( err error ) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal Error: %s", err.Error())
		os.Exit(1)
	}
}


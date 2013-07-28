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

type Injector interface {
	Write( data []byte )
}

type InjectorReceiver func ( inject Injector )

type ChunkProcessor func ( req ChunkService, chunk []byte, respond OutFn )
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

func (self *HTTPProcessor) parseTrailer( out OutFn ) {
	data := self.inbuf.Bytes()

	if len(data) == 0 {
		self.state = DONE
		return // no trailer
	}

	term := bytes.Index( data, LINE_TERM )
	if term == 0 {
		self.inbuf.Next( len(LINE_TERM) )
		if self.processor != nil {
			self.processor( self, nil, out )
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
	buff := new(bytes.Buffer)

	buff.WriteString( "HTTP/1.1 200 OK\r\n" )
	buff.WriteString( "Content-Type: application/json; charset=UTF-8\r\n" )
	buff.WriteString( "Transfer-Encoding: chunked\r\n" )
	buff.WriteString( "\r\n" )

	out( buff.Bytes() )
}

func (self *HTTPProcessor) endResponse( out OutFn ) {
	fmt.Println("endResponse")
	self.writeChunk(nil, out)
}

func (self *HTTPProcessor) writeChunk( data []byte, out OutFn ) {
	sizeDesc := new(bytes.Buffer)
	if data != nil {
		fmt.Fprintf(sizeDesc, "%X", len(data))
	} else {
		fmt.Fprint(sizeDesc, "0")
	}
	sizeDesc.Write( LINE_TERM )

	out( sizeDesc.Bytes() )
	if data != nil {
		out( data )
	}
	out( LINE_TERM )
}

func (self *HTTPProcessor) parseChunk( out OutFn ) {
	if uint(self.inbuf.Len()) < self.chunksz {
		return // gimme more
	}

	d := self.inbuf.Next( int(self.chunksz) )
	if self.processor != nil {
		self.processor( self, d, func (data []byte) {
			self.writeChunk(data, out)
		})
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
				self.parseTrailer( out )
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

func crazyWrite( out OutFn ) {
	b := new(bytes.Buffer)
	b.WriteString("KKK RAZY RIGHT\r\n")

	fmt.Println("Doing crazy line")
	out( b.Bytes() )
	fmt.Println("Done crazy line")
}

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

type dataT struct {
	data []byte
	closeup bool
	wrote chan bool
}

func connWriter( conn *net.Conn, dpipe chan *dataT ) {
	for {
		info := <-dpipe
		if info.data != nil {
			_, err2 := (*conn).Write( info.data )
			if err2 != nil {
				fmt.Fprintln(os.Stderr, "Error writing to connection ", err2)
			}
			info.wrote <- true
		}
		if info.closeup == true {
			(*conn).Close()
			break
		}
	}
}

func serviceClientConn( parent int, conn *net.Conn, spawn int, processor Processor) {
	fmt.Println("Processing spawn ", spawn)
	var in [4096] byte
	var isdone bool

	outchan := make( chan *dataT )

	var syncDT = new (dataT)
	syncDT.wrote = make( chan bool )

	var wsyn = make( chan bool, 1)
	wsyn <- true

	go connWriter( conn, outchan )

	for {
		n, err := (*conn).Read( in[0:] )

		if err != nil {
			fmt.Fprintf(os.Stderr, "Read Fatal Error: %s\n", err.Error())
			break
		}

		isdone = processor.request( in[0:n], func ( out []byte ) {
			<-wsyn
			fmt.Print("Writting [", bytes.NewBuffer(out).String(), "]")
			syncDT.data = out
			syncDT.closeup = false
			outchan <- syncDT
			<-syncDT.wrote
			wsyn <- true
		})

		if isdone {
			syncDT.data = nil
			syncDT.closeup = true
			outchan <- syncDT
			break
		}
	}

	//fmt.Println("Closing spawn ", spawn)
	//(*conn).Close()
}

func checkError( err error ) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal Error: %s", err.Error())
		os.Exit(1)
	}
}


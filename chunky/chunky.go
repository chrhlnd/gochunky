package chunky

import (
	"net"
	"os"
	"fmt"
	"bytes"
	"strconv"
	"strings"
)

/*
	Main flow works like this...

		Serve( someaddr, chunkbeginsessioncallback )
			- some client connects
				-> chunkbeginsessioncallback( sessionobj )
					*YOU* fillin sessionobj.OnReceive
						  fillin sessionobj.OnClose

				the OnReceive will be called with each new chunk
				the OnClose will be called when a close is detected

				the DoSend should be used by you to inject chunks going the other way

		Connect( someaddr, chunkbeginsessioncallback )
			- connect to some server
				-> chunkbeginsessioncallback( sessionobj )
					*YOU* fillin sessionobj.OnReceive
						  fillin sessionobj.OnClose

				*Same process as above

		NOTE DoSend is syncronized across all go routines so no dups of data should have to
		     be made in the runtime, the socket will buffer, but all sends sync for a connection

			 So if you've referenced the DoSend in multiple go routines its still going to send
			 syncronisly on that connection, which actually makes sense.

*/

type ChunkReceiver		func(ChunkSession,Chunk)
type ChunkSender		func(ChunkSession,[]byte,map[string]string)
type ChunkCloser		func(ChunkSession)
type ChunkBeginSession	func(ChunkSession)

type ChunkSession interface {
	URI()		(string)
	METHOD()	(string)
	PROTO()		(string)
	HEADERS()	(map[string]string)
	TRAILERS()	(map[string]string)

	SetOnReceive(ChunkReceiver)	// fill this in to get called
	SetOnClose(ChunkCloser)		// fill this one in too
	GetSend()(ChunkSender)			// use this to send
}

type Chunk interface {
	EXTENSIONS()(map[string]string)
	GetData()	([]byte)
}

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

type hTTPProcessor struct {
	state		hState
	nextstate	hState

	method		string
	uri			string
	proto		string
	headers		map[string]string
	extmap		map[string]string
	theaders	map[string]string

	inbuf		*bytes.Buffer

	chunksz		uint
	chunkData	[]byte

	onbegin		ChunkBeginSession
	onreceive	ChunkReceiver
	onclose		ChunkCloser
	send		func([]byte)
}

func (self *hTTPProcessor)SetOnReceive( fn ChunkReceiver) {
	self.onreceive = fn
}

func (self *hTTPProcessor)SetOnClose( fn ChunkCloser ) {
	self.onclose = fn
}

func chunkSend( obj ChunkSession, data []byte, ext map[string]string ) {
	hproc, ok := obj.(*hTTPProcessor)
	if ok {
		hproc.writeChunk( data, ext )
	}
}


func (self *hTTPProcessor)GetSend()(ChunkSender) {
	return chunkSend
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

func (self *hTTPProcessor) URI() (string) {
	return self.uri
}

func (self * hTTPProcessor) METHOD() (string) {
	return self.method
}

func (self *hTTPProcessor) PROTO() (string) {
	return self.proto
}

func (self *hTTPProcessor) HEADERS() (map[string]string) {
	return self.headers
}

func (self *hTTPProcessor) EXTENSIONS() (map[string]string) {
	return self.extmap
}

func (self *hTTPProcessor) TRAILERS() (map[string]string) {
	return self.theaders
}

func (self *hTTPProcessor) GetData()([]byte) {
	return self.chunkData
}

func (self *hTTPProcessor) parseMethod() {
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


func (self *hTTPProcessor) parseHeaders() {
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

func (self *hTTPProcessor) parseTrailer() {
	data := self.inbuf.Bytes()

	if len(data) == 0 {
		self.state = DONE
		return // no trailer
	}

	term := bytes.Index( data, LINE_TERM )
	if term == 0 {
		self.inbuf.Next( len(LINE_TERM) )
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

func (self *hTTPProcessor) parseExtensions()( map[string]string ) {
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

func (self *hTTPProcessor) parseChunkSz() {
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

func (self *hTTPProcessor) startResponse() {
	buff := new(bytes.Buffer)

	buff.WriteString( "HTTP/1.1 200 OK\r\n" )
	buff.WriteString( "Content-Type: application/json; charset=UTF-8\r\n" )
	buff.WriteString( "Transfer-Encoding: chunked\r\n" )
	buff.WriteString( "\r\n" )

	self.send( buff.Bytes() )
}

func (self *hTTPProcessor) endResponse() {
	fmt.Println("endResponse")
	self.writeChunk(nil,nil)
}

func (self *hTTPProcessor) writeChunk( data []byte, ext map[string]string ) {
	sizeDesc := new(bytes.Buffer)
	if data != nil {
		fmt.Fprintf(sizeDesc, "%X", len(data))
	} else {
		fmt.Fprint(sizeDesc, "0")
	}
	sizeDesc.Write( LINE_TERM )

	self.send( sizeDesc.Bytes() )
	if data != nil {
		self.send( data )
	}
	self.send( LINE_TERM )
}

func (self *hTTPProcessor) parseChunk() {
	if uint(self.inbuf.Len()) < self.chunksz {
		return // gimme more
	}

	self.chunkData = self.inbuf.Next( int(self.chunksz) )

	if self.chunksz == 0 {
		self.state		= BLANKLINE
		self.nextstate	= TRAILER
	} else {
		self.chunksz	= 0
		self.state		= BLANKLINE
		self.nextstate	= BODY
	}
}

func (self *hTTPProcessor) parseBlankLine() {
	data := self.inbuf.Bytes()

	pos := bytes.Index( data, LINE_TERM )
	if pos < 0 {
		return // keep sucking
	}

	data = self.inbuf.Next( pos + len( LINE_TERM ) )
	fmt.Println("DBG: tossing ", data, " Nextstate is ", self.nextstate)
	self.state = self.nextstate
}


func (self *hTTPProcessor) request( in []byte ) (bool) {
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
					if self.onbegin != nil {
						self.onbegin( self )
					}

					self.startResponse()
				}
				break
			case BODY:
				fmt.Println("Starting body...")
				self.parseChunkSz()
				break
			case CHUNK:
				fmt.Println("Starting chunk...")
				self.parseChunk()
				if self.state != CHUNK {
					if self.onreceive != nil {
						self.onreceive( self, self )
					}
				}
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
				self.endResponse()
				if self.onclose != nil {
					self.onclose( self )
				}
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

func Serve( addr string, workerCount int, begin ChunkBeginSession)(error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", addr)
	if err != nil {
		return fmt.Errorf("Resovle error %s", err)
	}

	listener, err := net.ListenTCP( "tcp", tcpAddr )
	if err != nil {
		return fmt.Errorf("Listen error: %s", err)
	}

	passAccept := make( chan *net.TCPListener );

	done := make( chan bool );

	for i := 0; i < workerCount; i++ {
		go handleClient( i, passAccept, done, begin )
	}

	go handlePassAccept( passAccept, listener )


	for i := 0; i < workerCount; i++ {
		<-done
	}

	fmt.Println("Closing main listener")
	listener.Close()
	return nil
}

func handlePassAccept( chanAccept chan *net.TCPListener, acceptor *net.TCPListener ) {
	chanAccept <- acceptor
}

func handleAccept( worker int, chanAccept chan *net.TCPListener, receiveConn chan *net.Conn ) {
	accept := <-chanAccept

	fmt.Println("Accepting on ", accept.Addr())
	conn, err := accept.Accept()
	if err != nil {
		fmt.Println("Accept error:",err)
		return
	}

	receiveConn <- &conn

	go handlePassAccept( chanAccept, accept )
}

func handleClient( worker int, chanAccept chan *net.TCPListener, done chan bool, onbegin ChunkBeginSession ) {
	receiveConn := make( chan *net.Conn, 1 )

	spawn := 0

	go handleAccept( worker, chanAccept, receiveConn )

	var conn *net.Conn

	for {
		conn = <-receiveConn

		proc := new (hTTPProcessor)
		proc.onbegin = onbegin

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

func serviceClientConn( parent int, conn *net.Conn, spawn int, processor *hTTPProcessor) {
	fmt.Println("Processing spawn ", spawn)
	var in [4096] byte
	var isdone bool

	outchan := make( chan *dataT )

	var syncDT = new (dataT)
	syncDT.wrote = make( chan bool )

	var wsyn = make( chan bool, 1)
	wsyn <- true

	go connWriter( conn, outchan )

	writer := func ( out []byte ) {
		<-wsyn
		syncDT.data		= out
		syncDT.closeup	= false
		outchan <- syncDT
		<-syncDT.wrote
		wsyn <- true
	}

	processor.send = writer

	for {
		n, err := (*conn).Read( in[0:] )

		if err != nil {
			fmt.Fprintf(os.Stderr, "Read Fatal Error: %s\n", err.Error())
			break
		}

		isdone = processor.request( in[0:n] )

		if isdone {
			syncDT.data = nil
			syncDT.closeup = true
			outchan <- syncDT
			break
		}
	}
}


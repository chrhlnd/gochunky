package chunky

import (
	"net"
	"os"
	"fmt"
	"bytes"
	"strconv"
	"strings"
	"io"
)

/*
	Main flow works like this...

		Serve( someaddr, workercount, chunkbeginsessioncallback )
			- some client connects
				-> chunkbeginsessioncallback( sessionobj )
					*YOU* SetOnReceive()
						  SetOnClose()

				the OnReceive will be called with each new chunk
				the OnClose will be called when a close is detected

				the DoSend should be used by you to inject chunks going the other way

		Connect( someaddr, ChunkBeginSession )(err)
			- connect to some server
				*YOU* SetOnReceive()
					  SetOnClose()

				*Same process as above
	
	NOTE: any connections' 'ChunkSender' is synced
			so copying the ChunkSender around to multiple producers should be safe.
			Not sure why you would want to do that, but I don't know what you're trying to do if
			you do something like this.
*/

type ChunkReceiver		func(ChunkSession,Chunk)
type ChunkSender		func(ChunkSession,[]byte,map[string]string)(bool)
type ChunkCloser		func(ChunkSession)
type ChunkBeginSession	func(ChunkSession)

type ChunkSession interface {
	URI()		(string)
	METHOD()	(string)
	PROTO()		(string)
	HEADERS()	(map[string]string)
	TRAILERS()	(map[string]string)

	CODE()		(string)
	MESSAGE()	(string)

	IsClosed()(bool)
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
	METHOD			= 0
	HEADERS			= 1
	BODY			= 2
	CHUNK			= 3
	BLANKLINE		= 4
	TRAILER			= 5
	DONE			= 6
	CLIENTSEND		= 7
	CLIENTCONNECT	= 8
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

	cheaders	map[string]string
	code		string // client side connection support
	message		string

	inbuf		*bytes.Buffer

	chunksz		uint
	chunkData	[]byte

	onbegin		ChunkBeginSession
	onreceive	ChunkReceiver
	onclose		ChunkCloser
	send		func([]byte)
	closed		bool
}

func (self *hTTPProcessor)SetOnReceive( fn ChunkReceiver) {
	self.onreceive = fn
}

func (self *hTTPProcessor)SetOnClose( fn ChunkCloser ) {
	self.onclose = fn
}

func chunkSend( self ChunkSession, data []byte, ext map[string]string )(bool) {
	if self.IsClosed() {
		return false
	}
	hproc, ok := self.(*hTTPProcessor)
	if ok {
		//fmt.Println( " Writing", data )
		hproc.writeChunk( data, ext )
		return true
	}
	return false
}

func (self *hTTPProcessor)GetSend()(ChunkSender) {
	return chunkSend
}

func (self *hTTPProcessor)IsClosed()(bool) {
	return self.closed == true
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

func (self *hTTPProcessor) CODE() (string) {
	return self.code
}

func (self *hTTPProcessor) MESSAGE() (string) {
	return self.message
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

func parseHeaders( inbuf *bytes.Buffer, headers map[string]string )(bool) {
	if inbuf.Len() == 0 {
		return false // more data
	}

	data := inbuf.Bytes()
	// fmt.Println( " Data is ", bytes.NewBuffer(data).String())

	term := bytes.Index( data, LINE_TERM ) // look for a blank line
	if term == 0 {
		inbuf.Next( len(LINE_TERM) )
		return true
	}

	sep := bytes.Index( data, KV_SEP )

	if sep < 0 {
		return false // don't have enough data yet
	}

	end := bytes.Index( data, LINE_TERM )

	if end < 0 {
		return false // not enough data for a k,v pair
	}


	key := strings.Trim( bytes.NewBuffer( data[0:sep] ).String(), SPACE )
	val := strings.Trim( bytes.NewBuffer( data[sep+len(KV_SEP):end] ).String(), SPACE )

	headers[key] = val

	// read out the data fron inbuf
	inbuf.Next( end + len(LINE_TERM) )
	return false // ok read 1 header
}

func (self *hTTPProcessor) parseHeaders() {
	if self.headers == nil {
		self.headers = make( map[string]string )
	}

	if parseHeaders( self.inbuf, self.headers ) {
		self.state = BODY
		return
	}
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
	//fmt.Println("endResponse")
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
	//fmt.Println("DBG: tossing ", data, " Nextstate is ", self.nextstate)
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
						self.closed = false
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
				//self.endResponse()
				if self.onclose != nil {
					self.onclose( self )
				}
				self.closed = true
				fmt.Println("Starting done...")
				fmt.Println("Closing data count in buffer is ", self.inbuf.Len())
				return true
			case CLIENTCONNECT:
				self.parseClientConnect()
				break
		}
		if self.state != TRAILER && self.state != DONE {
			if self.inbuf.Len() < 1 {
				break
			}
		}
	}

	return false
}

func (self *hTTPProcessor) parseClientConnect() {

	data := self.inbuf.Bytes()

	pos := bytes.Index( data, LINE_TERM )
	if pos < 0 {
		return // need more
	}

	var space int

	if self.proto == "" {
		space = bytes.Index( data, METH_SEP )
		if space < 0 {
			return // mal formed response?
		}
		self.proto = bytes.NewBuffer( self.inbuf.Next( space ) ).String()
		self.inbuf.Next( len(METH_SEP) ) // advance pass seperator
		data = self.inbuf.Bytes()
	}

	if self.code == "" {
		space = bytes.Index( data, METH_SEP )
		if space < 0 {
			return // mal formed response?
		}
		self.code = bytes.NewBuffer( self.inbuf.Next( space ) ).String()
		self.inbuf.Next( len(METH_SEP) ) // advance pass seperator
		data = self.inbuf.Bytes()
	}

	if self.message == "" {
		pos = bytes.Index( data, LINE_TERM )
		if pos < 0 {
			return // mal formed
		}
		self.message = bytes.NewBuffer( self.inbuf.Next( pos ) ).String()
		self.inbuf.Next( len(LINE_TERM) )
		data = self.inbuf.Bytes()
	}

	if self.headers == nil {
		self.headers = make( map[string]string )
	}

	if parseHeaders( self.inbuf, self.headers ) {
		self.state = BODY
		self.onbegin( self )
		// ok connection setup now call the session begin
		return
	}
}

func ( self *hTTPProcessor ) sendConnect() {
	// start a http session
	buff := new(bytes.Buffer)

	fmt.Fprintf(buff, "POST %s HTTP/1.1\r\n", self.uri)
	if self.cheaders != nil {
		for key, val := range( self.cheaders ) {
			if key != "Content-Type" && key != "Transfer-Encoding" {
				fmt.Fprintf(buff, "%s: %s\r\n", key, val)
			}
		}
	}
	fmt.Fprintf(buff, "Transfer-Encoding: chunked\r\n" )
	buff.WriteString( "\r\n" )

	self.send( buff.Bytes() )

	self.state = CLIENTCONNECT
}

// make a client connection
func Connect( ipport string, uri string, headers map[string]string, onbegin ChunkBeginSession)(error) {
	conn, err := net.Dial( "tcp", ipport )
	if err != nil {
		return fmt.Errorf("Resolve error %s", err)
	}

	proc := new (hTTPProcessor)
	proc.onbegin	= onbegin
	proc.cheaders	= headers
	proc.uri		= uri

	proc.state = CLIENTSEND

	serviceClientConn( 0, &conn, 0, proc )
	return nil
}

// make a server
func Serve( addr string, workerCount int, begin ChunkBeginSession)(error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", addr)
	if err != nil {
		return fmt.Errorf("Resolve error %s", err)
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
		if info.data != nil && len(info.data) > 0 {
			_, err2 := (*conn).Write( info.data )
			if err2 != nil {
				fmt.Fprintln(os.Stderr,
						"Error writing to connection(",
						err2,
						")data(",
						info.data,
						")")
			}
			info.wrote <- true
		}
		if info.closeup == true {
			//fmt.Println("Closing WRITER")
			(*conn).Close()
			fmt.Println("Communicating KILL")
			//info.wrote <- true
			fmt.Println("Done with write loop")
			break
		}
	}
}

func serviceClientConn( parent int, conn *net.Conn, spawn int, processor *hTTPProcessor) {
	//fmt.Println("Processing spawn ", spawn)
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
	if processor.state == CLIENTSEND {
		processor.sendConnect()
	}

	for {
		n, err := (*conn).Read( in[0:] )

		if err != nil {
			if err == io.EOF {
				//fmt.Println("Connection closed by EOF")
			} else {
				fmt.Fprintf(os.Stderr, "Read Fatal Error: %s\n", err.Error())
			}
			break
		}

		isdone = processor.request( in[0:n] )

		if isdone {
			syncDT.data = nil
			syncDT.closeup = true
			//fmt.Println("Closing sender")
			outchan <- syncDT
			//fmt.Println("Done Closing sender")
			break
		}
	}
	fmt.Println("Done servicing conn")
}


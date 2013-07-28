
server: server.go
	go build -gcflags "-N -l"  server.go

release: server.go
	go build server.go

clean:
	rm -f server

all: server

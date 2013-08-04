
debug: chunky/chunky.go chunky_srv.go
	go build -gcflags "-N -l" chunky_srv.go

release: chunky/chunky.go chunky_srv.go
	go build chunky_srv.go

clean:
	rm -f chunky_srv

all: debug

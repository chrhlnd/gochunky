all: debug_server debug_client

debug_server: chunky/chunky.go chunky_srv.go
	go build -o $@ -gcflags "-N -l" chunky_srv.go

release_server: chunky/chunky.go chunky_srv.go
	go build -o $@ chunky_srv.go

debug_client: chunky/chunky.go chunky_client.go
	go build -o $@ -gcflags "-N -l" chunky_client.go

release_client: chunky/chunky.go chunky_client.go
	go build -o $@ chunky_client.go

clean:
	rm -f debug_server
	rm -f release_server
	rm -f debug_client
	rm -f release_client


all: build-linux

build: clean
	@echo build mac
	go build -o ./pikaso/pikaso ./pikaso/main.go
	go build -o ./slotinfo/slotinfo ./slotinfo/main.go

build-linux: clean
	@echo build linux
	GOOS=linux GOARCH=amd64 go build -o ./pikaso/pikaso ./pikaso/main.go
	GOOS=linux GOARCH=amd64 go build -o ./slotinfo/slotinfo ./slotinfo/main.go

clean:
	@rm -rf pikaso/pikaso
	@rm -rf slotinfo/slotinfo

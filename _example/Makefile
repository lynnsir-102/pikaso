all: build

build: clean 
	@echo build linux
	GOOS=linux GOARCH=amd64 go build -o _example ./

build-mac: clean
	@echo build mac
	go build -o _example ./

clean:
	@rm -rf _example
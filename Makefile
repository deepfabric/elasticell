RELEASE_VERSION    = $(release_version)

ifeq ("$(RELEASE_VERSION)","")
	RELEASE_VERSION		:= "dev"
endif

ROOT_DIR 	          = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))/
VERSION_PATH          = $(shell echo $(ROOT_DIR) | sed -e "s;${GOPATH}/src/;;g")pkg/util
LD_GIT_COMMIT         = -X '$(VERSION_PATH).GitCommit=`git rev-parse --short HEAD`'
LD_BUILD_TIME         = -X '$(VERSION_PATH).BuildTime=`date +%FT%T%z`'
LD_GO_VERSION         = -X '$(VERSION_PATH).GoVersion=`go version`'
LD_ELASTICELL_VERSION = -X '$(VERSION_PATH).Version=$(RELEASE_VERSION)'
LD_FLAGS              = -ldflags "$(LD_GIT_COMMIT) $(LD_BUILD_TIME) $(LD_GO_VERSION) $(LD_ELASTICELL_VERSION) -w -s"

GOOS 		= linux
CGO_ENABLED = 0
DIST_DIR 	= $(ROOT_DIR)dist/

.PHONY: release
release: dist_dir pd cell proxy;

.PHONY: pd
pd: ; $(info ======== compiled elasticell-pd:)
	env CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) go build -mod vendor -a -installsuffix cgo -o $(DIST_DIR)pd $(LD_FLAGS) $(ROOT_DIR)cmd/pd/*.go

.PHONY: proxy
proxy: ; $(info ======== compiled elasticell-proxy:)
	env CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) go build -mod vendor -a -installsuffix cgo -o $(DIST_DIR)proxy $(LD_FLAGS) $(ROOT_DIR)cmd/proxy/*.go

.PHONY: cell
cell: ; $(info ======== compiled elasticell-cell:)
	env GO111MODULE=off GOOS=$(GOOS) go build -o $(DIST_DIR)cell $(LD_FLAGS) $(ROOT_DIR)cmd/cell/*.go

.PHONY: dist_dir
dist_dir: ; $(info ======== prepare distribute dir:)
	mkdir -p $(DIST_DIR)
	@rm -rf $(DIST_DIR)*

.PHONY: clean
clean: ; $(info ======== clean all:)
	rm -rf $(DIST_DIR)*

.PHONY: help
help:
	@echo "build release binary: \n\t\tmake release\n"
	@echo "clean all binary: \n\t\tmake clean\n"

UNAME_S := $(shell uname -s)

.DEFAULT_GOAL := release
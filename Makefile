# objcli Makefile
#
# Common targets:
#   make build       - build local binary into ./objcli
#   make install     - install binary to $(PREFIX)/bin (default /usr/local)
#   make install-man - install man page to $(MANDIR) (default $(PREFIX)/share/man/man1)
#   make uninstall   - remove installed binary and man page
#   make test        - go test ./...
#   make vet         - go vet ./...
#   make fmt         - gofmt all .go files
#   make clean       - remove built artifacts
#   make version     - print resolved version metadata

BIN          := objcli
PREFIX       ?= /usr/local
BINDIR       ?= $(PREFIX)/bin
MANDIR       ?= $(PREFIX)/share/man/man1

# Version metadata, injected via -ldflags.
GIT_TAG      := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
GIT_COMMIT   := $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
BUILD_TIME   := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -X 'main.versionTag=$(GIT_TAG)' \
           -X 'main.versionCommit=$(GIT_COMMIT)' \
           -X 'main.versionTime=$(BUILD_TIME)'

GO_BUILD_FLAGS ?= -trimpath
GOFLAGS_EXTRA  ?=

.PHONY: all build install install-man uninstall test vet fmt clean version help

all: build

build:
	go build $(GO_BUILD_FLAGS) -ldflags "$(LDFLAGS)" $(GOFLAGS_EXTRA) -o $(BIN) .

install: build
	install -d $(DESTDIR)$(BINDIR)
	install -m 0755 $(BIN) $(DESTDIR)$(BINDIR)/$(BIN)
	$(MAKE) install-man

install-man:
	install -d $(DESTDIR)$(MANDIR)
	install -m 0644 docs/$(BIN).1 $(DESTDIR)$(MANDIR)/$(BIN).1

uninstall:
	rm -f $(DESTDIR)$(BINDIR)/$(BIN)
	rm -f $(DESTDIR)$(MANDIR)/$(BIN).1

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

clean:
	rm -f $(BIN)

version:
	@echo "GIT_TAG    = $(GIT_TAG)"
	@echo "GIT_COMMIT = $(GIT_COMMIT)"
	@echo "BUILD_TIME = $(BUILD_TIME)"
	@echo "PREFIX     = $(PREFIX)"
	@echo "BINDIR     = $(BINDIR)"
	@echo "MANDIR     = $(MANDIR)"

help:
	@grep -E '^[a-zA-Z_-]+:.*?# .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?# "}; {printf "  %-15s %s\n", $$1, $$2}'

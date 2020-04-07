# Variables
BUILD_DIR 		:= build
GITHASH 		:= $(shell git rev-parse HEAD)
VERSION			:= $(shell git describe --abbrev=0 --tags --always)
DATE			:= $(shell TZ=UTC date -u '+%Y-%m-%dT%H:%M:%SZ UTC')
LINT_PATHS		:= ./...
FORMAT_PATHS 	:= .

# Compilation variables
CC 					:= go build
DFLAGS 				:= -race
CFLAGS 				:= -X 'main.githash=$(GITHASH)' \
            -X 'main.date=$(DATE)' \
            -X 'main.version=0.2.0'
PLATFORMS=darwin linux windows
ARCHITECTURES=386 amd64

# Makefile variables
VPATH 				:= $(BUILD_DIR)


.SECONDEXPANSION:
.PHONY: all
all: init format lint release

.PHONY: init
init:
	curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh
	go get -u github.com/onsi/ginkgo/ginkgo
	go get -u golang.org/x/tools/cmd/cover
	go get -u github.com/modocache/gover
	go get -u github.com/golangci/golangci-lint/cmd/golangci-lint
	go get golang.org/x/tools/cmd/goimports

.PHONY: cleanmake
clean:
	rm -rf $(BUILD_DIR)
	rm -rf dist

.PHONY: format
format:
	gofmt -w -s $(FORMAT_PATHS)
	goimports -w $(FORMAT_PATHS)

.PHONY: lint
lint:
	@command -v golangci-lint >/dev/null 2>&1 || { echo >&2 "golangci-lint is required but not available please follow instructions from https://github.com/golangci/golangci-lint"; exit 1; }
	golangci-lint run  --config golangci.yml


.PHONY: test
test:
	$(GOPATH)/bin/ginkgo -r --randomizeAllSpecs --randomizeSuites --failOnPending --cover --trace --progress --compilers=2

.PHONY: testrun
testrun:
	$(GOPATH)/bin/ginkgo watch -r ./

.PHONY: cover
cover:
	$(GOPATH)/bin/gover . coverage.txt


.PHONY: dev
dev: format lint build

.PHONY: build
build:
	$(CC) $(DFLAGS) -ldflags "-s -w $(CFLAGS)" -o $(BUILD_DIR)/lookatch-agent

.PHONY: release
release:
	$(CC) -ldflags "-s -w $(CFLAGS)" -o $(BUILD_DIR)/lookatch-agent

.PHONY: dist
dist:
		@for GOOS in $(PLATFORMS) ; do \
            for GOARCH in $(ARCHITECTURES) ; do \
                GOOS=$${GOOS} GOARCH=$${GOARCH} $(CC) -ldflags "-s -w $(CFLAGS)" -o $(BUILD_DIR)/$${GOOS}/$${GOARCH}/lookatch-agent; \
			done \
		done

.PHONY: install
install: release
	cp -v $(BUILD_DIR)/lookatch-agent $(GOPATH)/bin/lookatch-agent

.PHONY: deb
deb:
	rm -f lookatch-agent*.deb
	rm -f package/deb/input-*
	for ARCH in $(ARCHITECTURES) ; do \
		sed "s/%ARCH%/$${ARCH}/g" <package/deb/input >package/deb/input-$${ARCH}; \
		fpm -m "<Pirionfr>" \
		  --description "replicate and synchronize your data" \
			--url "https://github.com/Pirionfr/lookatch-agent" \
			--license "Apache-2.0" \
			--version $(VERSION) \
			-n lookatch-agent \
			-d logrotate \
			-s dir \
			-t deb \
			-a $${ARCH} \
			--deb-user lookatch \
			--deb-group lookatch \
			--deb-no-default-config-files \
			--config-files /etc/lookatch/config.json \
			--deb-systemd package/rpm/lookatch-agent.service \
			--directories /var/log/lookatch-agent \
			--before-install package/deb/before-install.sh \
			--after-install package/deb/after-install.sh \
			--before-upgrade package/deb/before-upgrade.sh \
			--after-upgrade package/deb/after-upgrade.sh \
			--before-remove package/deb/before-remove.sh \
			--after-remove package/deb/after-remove.sh \
			--inputs package/deb/input-$${ARCH} ; \
	done

.PHONY: rpm
rpm:
	rm -f lookatch-agent*.rpm
	rm -f package/rpm/input-*
	for ARCH in $(ARCHITECTURES) ; do \
		sed "s/%ARCH%/$${ARCH}/g" <package/rpm/input >package/rpm/input-$${ARCH}; \
		fpm -m "<Pirionfr>" \
		  --description "replicate and synchronize your data" \
		    --url "https://github.com/Pirionfr/lookatch-agent" \
			--license "Apache-2.0" \
			--version $(VERSION) \
			-n lookatch-agent \
			-s dir \
			-t rpm \
			-a $${ARCH} \
			--rpm-user lookatch \
			--rpm-group lookatch \
			--config-files /etc/lookatch/config.json \
			--before-install package/rpm/before-install.sh \
			--after-install package/rpm/after-install.sh \
			--before-upgrade package/rpm/before-upgrade.sh \
			--after-upgrade package/rpm/after-upgrade.sh \
			--before-remove package/rpm/before-remove.sh \
			--after-remove package/rpm/after-remove.sh \
			--inputs package/rpm/input-$${ARCH} ; \
	done

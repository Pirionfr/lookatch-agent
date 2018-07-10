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
            -X 'main.version=$(VERSION)'
CROSS				:= GOOS=linux GOARCH=amd64

# Makefile variables
VPATH 				:= $(BUILD_DIR)


.SECONDEXPANSION:
.PHONY: all
all: init dep format lint release

.PHONY: init
init:
	go get -u github.com/golang/dep/...
	go get -u github.com/alecthomas/gometalinter
	go get -u github.com/onsi/ginkgo/ginkgo
	go get -u golang.org/x/tools/cmd/cover
	go get -u github.com/modocache/gover
	$(GOPATH)/bin/gometalinter --install --no-vendored-linters

.PHONY: clean
clean:
	rm -rf $(BUILD_DIR)
	rm -rf dist

.PHONY: dep
dep:
	$(GOPATH)/bin/dep ensure -v

.PHONY: format
format:
	gofmt -w -s $(FORMAT_PATHS)

.PHONY: lint
lint:
	$(GOPATH)/bin/gometalinter --disable-all --config .gometalinter.json $(LINT_PATHS)

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
	$(CROSS) $(CC) -ldflags "-s -w $(CFLAGS)" -o $(BUILD_DIR)/lookatch-agent

.PHONY: install
install: release
	cp -v $(BUILD_DIR)/lookatch-agent $(GOPATH)/bin/lookatch-agent

.PHONY: deb
deb:
		rm -f lookatch-agent*.deb
		fpm -m "<Pirionfr>" \
		  --description "replicate and synchronize your data" \
			--url "https://github.com/Pirionfr/lookatch-agent" \
			--license "Apache-2.0" \
			--version $(VERSION) \
			-n lookatch-agent \
			-d logrotate \
			-s dir \
			-t deb \
			-a amd64 \
			--deb-user lookatch \
			--deb-group lookatch \
			--deb-no-default-config-files \
			--config-files /etc/lookatch/config.json \
			--deb-init package/deb/lookatch-agent.init \
			--directories /var/log/lookatch-agent \
			--before-install package/deb/before-install.sh \
			--after-install package/deb/after-install.sh \
			--before-upgrade package/deb/before-upgrade.sh \
			--after-upgrade package/deb/after-upgrade.sh \
			--before-remove package/deb/before-remove.sh \
			--after-remove package/deb/after-remove.sh \
			--inputs package/deb/input

.PHONY: rpm
rpm:
		rm -f lookatch-agent*.rpm
		fpm -m "<Pirionfr>" \
		  --description "replicate and synchronize your data" \
		    --url "https://github.com/Pirionfr/lookatch-agent" \
			--license "Apache-2.0" \
			--version $(VERSION) \
			-n lookatch-agent \
			-d logrotate \
			-s dir \
			-t rpm \
			-a amd64 \
			--rpm-user lookatch \
			--rpm-group lookatch \
			--config-files /etc/lookatch/config.json \
			--rpm-init package/rpm/lookatch-agent.init \
			--before-install package/rpm/before-install.sh \
			--after-install package/rpm/after-install.sh \
			--before-upgrade package/rpm/before-upgrade.sh \
			--after-upgrade package/rpm/after-upgrade.sh \
			--before-remove package/rpm/before-remove.sh \
			--after-remove package/rpm/after-remove.sh \
			--inputs package/rpm/input
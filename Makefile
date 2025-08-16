PKGNAME=$(shell go list)

ifeq ($(RACE),1)
RACE_FLAG=-race
$(warning "data race detector enabled")
endif

ifneq ($(TEST_TO_RUN),)
$(info Running selected tests $(TEST_TO_RUN))
SELECTED_TEST_OPTION=-run $(TEST_TO_RUN)
endif

GOTEST=go test -v -timeout=600s -count=1 $(RACE_FLAG) $(SELECTED_TEST_OPTION)

.PHONY: test
test:
	$(GOTEST) $(PKGNAME)


# static checks
GOLANGCI_LINT_VERSION=v2.1.6
EXTRA_LINTERS=-E misspell -E rowserrcheck -E unconvert -E prealloc
.PHONY: static-check
static-check:
	golangci-lint run --timeout 3m $(EXTRA_LINTERS)

.PHONY: install-static-check-tools
install-static-check-tools:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

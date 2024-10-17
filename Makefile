BINARY_NAME=rmqp

MAIN_PATH=cmd/producer/main.go

build:
	@echo "Building the Go project..."
	go build -o $(BINARY_NAME) $(MAIN_PATH)

clean:
	@echo "Cleaning up..."
	rm -f $(BINARY_NAME)

run: build
	@echo "Running the application..."
	./$(BINARY_NAME)

install:
	@echo "Installing the binary..."
	go install $(MAIN_PATH)

.PHONY: build clean run install

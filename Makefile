run:
	@go run ./cmd/server

test:
	@echo "You got your server running?"
	@echo "if not, then tests will fail"
	@echo "run it with 'make run'"
	@go run ./cmd/stress_test

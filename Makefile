help:
	@echo ==========HELP==========
	@echo "docker    : build docker container "
	@echo "run-local : runs the built docker container locally"
	@echo "test      : runs the go tests"

docker:
	@docker build --no-cache -t eqr-worker .

run-local: docker
	@docker run -it eqr-worker

test:
	@bash bypassRepoFiles.sh
	@cd unitTests && bash runTests.sh
	@go test ./kinsumer/...

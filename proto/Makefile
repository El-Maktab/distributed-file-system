PROTO_DIR := .
OUT_DIR := ../pkg/api/dfs

.PHONY: gen

gen:
	protoc --proto_path=$(PROTO_DIR) --go_out=$(OUT_DIR) --go_opt=paths=source_relative --go-grpc_out=$(OUT_DIR) --go-grpc_opt=paths=source_relative $(PROTO_DIR)/dfs.proto

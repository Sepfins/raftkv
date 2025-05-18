FROM golang:1.22-alpine

WORKDIR /app

RUN apk add --no-cache git

COPY . .

RUN go mod download

# Build binaries
RUN GOARCH=amd64 GOOS=linux go build -o shardctrlernode ./launcher/shardctrler
RUN GOARCH=amd64 GOOS=linux go build -o shardkvnode ./launcher/shardkv
RUN GOARCH=amd64 GOOS=linux go build -o raftnode ./launcher/raftnode
RUN GOARCH=amd64 GOOS=linux go build -o shardkv_client ./launcher/clients/shardkv_client.go
RUN GOARCH=amd64 GOOS=linux go build -o shardctrler_client ./launcher/clients/shardctrler_client.go

# Set proper permissions and verify
RUN chmod 755 /app/shardkv /app/shardctrler /app/raftnode /app/shardkv_client /app/shardctrler_client && \
    ls -l /app

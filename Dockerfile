# Build stage
FROM golang:1.24-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/mcp-rethinkdb-server ./main.go

# Runtime stage
FROM alpine:3.20

RUN adduser -D -H -s /sbin/nologin mcp
USER mcp

WORKDIR /app
COPY --from=build /out/mcp-rethinkdb-server /app/mcp-rethinkdb-server

# MCP uses stdio by default
ENTRYPOINT ["/app/mcp-rethinkdb-server"]

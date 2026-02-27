package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"mcp-rethinkdb-server/server"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

func main() {
	// Get RethinkDB connection settings from environment or defaults
	host := os.Getenv("RETHINKDB_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("RETHINKDB_PORT")
	if port == "" {
		port = "28015"
	}

	address := fmt.Sprintf("%s:%s", host, port)

	// Connect to RethinkDB
	connectOpts := r.ConnectOpts{
		Address: address,
	}

	// Optional auth
	if user := os.Getenv("RETHINKDB_USER"); user != "" {
		connectOpts.Username = user
	}
	if password := os.Getenv("RETHINKDB_PASSWORD"); password != "" {
		connectOpts.Password = password
	}

	session, err := r.Connect(connectOpts)
	if err != nil {
		log.Fatalf("Failed to connect to RethinkDB at %s: %v", address, err)
	}
	defer session.Close()

	// Log connection success to stderr (stdout is for MCP communication)
	fmt.Fprintf(os.Stderr, "Connected to RethinkDB at %s\n", address)

	// Create RethinkDB server handler
	rdbServer := server.NewRethinkDBServer(session)

	// Create MCP server
	mcpServer := mcp.NewServer(&mcp.Implementation{
		Name:    "mcp-rethinkdb-server",
		Version: "1.0.0",
	}, nil)

	// Register all tools
	rdbServer.RegisterTools(mcpServer)

	// Run server over stdio
	if err := mcpServer.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

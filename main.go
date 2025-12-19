package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

var session *r.Session

// Tool Input/Output structs

type EmptyInput struct{}

type ListDatabasesOutput struct {
	Databases []string `json:"databases"`
}

type ListTablesInput struct {
	Database string `json:"database" jsonschema_description:"The database name to list tables from"`
}

type ListTablesOutput struct {
	Database string   `json:"database"`
	Tables   []string `json:"tables"`
}

type QueryTableInput struct {
	Database string                 `json:"database" jsonschema_description:"The database name"`
	Table    string                 `json:"table" jsonschema_description:"The table name"`
	Filter   map[string]interface{} `json:"filter,omitempty" jsonschema_description:"Optional filter object for the query"`
	Limit    int                    `json:"limit,omitempty" jsonschema_description:"Maximum number of results (default 100, max 1000)"`
	OrderBy  string                 `json:"order_by,omitempty" jsonschema_description:"Optional field to order results by"`
}

type QueryTableOutput struct {
	Database string        `json:"database"`
	Table    string        `json:"table"`
	Count    int           `json:"count"`
	Results  []interface{} `json:"results"`
}

type TableInfoInput struct {
	Database string `json:"database" jsonschema_description:"The database name"`
	Table    string `json:"table" jsonschema_description:"The table name"`
}

type TableInfoOutput struct {
	Database    string        `json:"database"`
	Table       string        `json:"table"`
	PrimaryKey  string        `json:"primary_key"`
	Indexes     []string      `json:"indexes"`
	DocCount    int           `json:"doc_count"`
	TableStatus []interface{} `json:"table_status,omitempty"`
}

// Tool handlers

func listDatabases(ctx context.Context, req *mcp.CallToolRequest, input EmptyInput) (*mcp.CallToolResult, ListDatabasesOutput, error) {
	cursor, err := r.DBList().Run(session)
	if err != nil {
		return nil, ListDatabasesOutput{}, fmt.Errorf("failed to list databases: %w", err)
	}
	defer cursor.Close()

	var databases []string
	if err := cursor.All(&databases); err != nil {
		return nil, ListDatabasesOutput{}, fmt.Errorf("failed to read databases: %w", err)
	}

	return nil, ListDatabasesOutput{Databases: databases}, nil
}

func listTables(ctx context.Context, req *mcp.CallToolRequest, input ListTablesInput) (*mcp.CallToolResult, ListTablesOutput, error) {
	if input.Database == "" {
		return nil, ListTablesOutput{}, fmt.Errorf("database name is required")
	}

	cursor, err := r.DB(input.Database).TableList().Run(session)
	if err != nil {
		return nil, ListTablesOutput{}, fmt.Errorf("failed to list tables: %w", err)
	}
	defer cursor.Close()

	var tables []string
	if err := cursor.All(&tables); err != nil {
		return nil, ListTablesOutput{}, fmt.Errorf("failed to read tables: %w", err)
	}

	if tables == nil {
		tables = []string{}
	}

	return nil, ListTablesOutput{Database: input.Database, Tables: tables}, nil
}

func queryTable(ctx context.Context, req *mcp.CallToolRequest, input QueryTableInput) (*mcp.CallToolResult, QueryTableOutput, error) {
	if input.Database == "" || input.Table == "" {
		return nil, QueryTableOutput{}, fmt.Errorf("database and table names are required")
	}

	// Set default and max limit
	limit := input.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	// Build query
	query := r.DB(input.Database).Table(input.Table)

	// Apply filter if provided
	if input.Filter != nil && len(input.Filter) > 0 {
		query = query.Filter(input.Filter)
	}

	// Apply order by if provided
	if input.OrderBy != "" {
		query = query.OrderBy(input.OrderBy)
	}

	// Apply limit
	query = query.Limit(limit)

	cursor, err := query.Run(session)
	if err != nil {
		return nil, QueryTableOutput{}, fmt.Errorf("failed to execute query: %w", err)
	}
	defer cursor.Close()

	var results []interface{}
	if err := cursor.All(&results); err != nil {
		return nil, QueryTableOutput{}, fmt.Errorf("failed to read results: %w", err)
	}

	return nil, QueryTableOutput{
		Database: input.Database,
		Table:    input.Table,
		Count:    len(results),
		Results:  results,
	}, nil
}

func tableInfo(ctx context.Context, req *mcp.CallToolRequest, input TableInfoInput) (*mcp.CallToolResult, TableInfoOutput, error) {
	if input.Database == "" || input.Table == "" {
		return nil, TableInfoOutput{}, fmt.Errorf("database and table names are required")
	}

	output := TableInfoOutput{
		Database: input.Database,
		Table:    input.Table,
	}

	// Get table info
	cursor, err := r.DB(input.Database).Table(input.Table).Info().Run(session)
	if err != nil {
		return nil, TableInfoOutput{}, fmt.Errorf("failed to get table info: %w", err)
	}
	defer cursor.Close()

	var info map[string]interface{}
	if err := cursor.One(&info); err != nil {
		return nil, TableInfoOutput{}, fmt.Errorf("failed to read table info: %w", err)
	}

	if pk, ok := info["primary_key"].(string); ok {
		output.PrimaryKey = pk
	}

	// Get indexes
	indexCursor, err := r.DB(input.Database).Table(input.Table).IndexList().Run(session)
	if err == nil {
		defer indexCursor.Close()
		var indexes []string
		if err := indexCursor.All(&indexes); err == nil {
			output.Indexes = indexes
		}
	}
	if output.Indexes == nil {
		output.Indexes = []string{}
	}

	// Get document count
	countCursor, err := r.DB(input.Database).Table(input.Table).Count().Run(session)
	if err == nil {
		defer countCursor.Close()
		var count int
		if err := countCursor.One(&count); err == nil {
			output.DocCount = count
		}
	}

	return nil, output, nil
}

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
	var err error
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

	session, err = r.Connect(connectOpts)
	if err != nil {
		log.Fatalf("Failed to connect to RethinkDB at %s: %v", address, err)
	}
	defer session.Close()

	// Log connection success to stderr (stdout is for MCP communication)
	fmt.Fprintf(os.Stderr, "Connected to RethinkDB at %s\n", address)

	// Create MCP server
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "mcp-rethinkdb-server",
		Version: "1.0.0",
	}, nil)

	// Register tools
	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_databases",
		Description: "List all databases in RethinkDB",
	}, listDatabases)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_tables",
		Description: "List all tables in a RethinkDB database",
	}, listTables)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "query_table",
		Description: "Query data from a RethinkDB table (read-only). Supports filtering, ordering, and limiting results.",
	}, queryTable)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "table_info",
		Description: "Get table information including primary key, indexes, and document count",
	}, tableInfo)

	// Run server over stdio
	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

// Helper function for pretty printing (useful for debugging)
func prettyJSON(v interface{}) string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}

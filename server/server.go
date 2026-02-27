// Package server provides an MCP tool server for RethinkDB, including handlers
// for listing databases, tables, querying, and writing data.
package server

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

// RethinkDBServer holds the session and provides MCP tool handlers.
type RethinkDBServer struct {
	session *r.Session
}

// NewRethinkDBServer creates a new server with the given RethinkDB session.
func NewRethinkDBServer(session *r.Session) *RethinkDBServer {
	return &RethinkDBServer{session: session}
}

// ─── Input/Output structs ────────────────────────────────────────────────────

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
	Database string         `json:"database" jsonschema_description:"The database name"`
	Table    string         `json:"table" jsonschema_description:"The table name"`
	Filter   map[string]any `json:"filter,omitempty" jsonschema_description:"Optional filter object for the query"`
	Limit    int            `json:"limit,omitempty" jsonschema_description:"Maximum number of results (default 100, max 1000)"`
	OrderBy  string         `json:"order_by,omitempty" jsonschema_description:"Optional field to order results by"`
}

type QueryTableOutput struct {
	Database string `json:"database"`
	Table    string `json:"table"`
	Count    int    `json:"count"`
	Results  []any  `json:"results"`
}

type TableInfoInput struct {
	Database string `json:"database" jsonschema_description:"The database name"`
	Table    string `json:"table" jsonschema_description:"The table name"`
}

type TableInfoOutput struct {
	Database    string   `json:"database"`
	Table       string   `json:"table"`
	PrimaryKey  string   `json:"primary_key"`
	Indexes     []string `json:"indexes"`
	DocCount    int      `json:"doc_count"`
	TableStatus []any    `json:"table_status,omitempty"`
}

type WriteDataInput struct {
	Database  string          `json:"database" jsonschema_description:"The database name"`
	Table     string          `json:"table" jsonschema_description:"The table name"`
	Data      json.RawMessage `json:"data" jsonschema_description:"The data to write. For insert/update/upsert: a document or array of documents. For delete: a document with the primary key field, or a filter object to match multiple documents."`
	Operation string          `json:"operation,omitempty" jsonschema_description:"The write operation: insert (default), update, upsert, or delete"`
}

type WriteDataOutput struct {
	Database   string `json:"database"`
	Table      string `json:"table"`
	Operation  string `json:"operation"`
	Inserted   int    `json:"inserted"`
	Replaced   int    `json:"replaced"`
	Unchanged  int    `json:"unchanged"`
	Deleted    int    `json:"deleted"`
	Errors     int    `json:"errors"`
	FirstError string `json:"first_error,omitempty"`
}

// ─── Tool handlers ───────────────────────────────────────────────────────────

func (s *RethinkDBServer) ListDatabases(ctx context.Context, req *mcp.CallToolRequest, input EmptyInput) (*mcp.CallToolResult, ListDatabasesOutput, error) {
	cursor, err := r.DBList().Run(s.session)
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

func (s *RethinkDBServer) ListTables(ctx context.Context, req *mcp.CallToolRequest, input ListTablesInput) (*mcp.CallToolResult, ListTablesOutput, error) {
	if input.Database == "" {
		return nil, ListTablesOutput{}, fmt.Errorf("database name is required")
	}

	cursor, err := r.DB(input.Database).TableList().Run(s.session)
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

func (s *RethinkDBServer) QueryTable(ctx context.Context, req *mcp.CallToolRequest, input QueryTableInput) (*mcp.CallToolResult, QueryTableOutput, error) {
	if input.Database == "" || input.Table == "" {
		return nil, QueryTableOutput{}, fmt.Errorf("database and table names are required")
	}

	limit := input.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	query := r.DB(input.Database).Table(input.Table)

	if len(input.Filter) > 0 {
		query = query.Filter(input.Filter)
	}

	if input.OrderBy != "" {
		query = query.OrderBy(input.OrderBy)
	}

	query = query.Limit(limit)

	cursor, err := query.Run(s.session)
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

func (s *RethinkDBServer) TableInfo(ctx context.Context, req *mcp.CallToolRequest, input TableInfoInput) (*mcp.CallToolResult, TableInfoOutput, error) {
	if input.Database == "" || input.Table == "" {
		return nil, TableInfoOutput{}, fmt.Errorf("database and table names are required")
	}

	output := TableInfoOutput{
		Database: input.Database,
		Table:    input.Table,
	}

	cursor, err := r.DB(input.Database).Table(input.Table).Info().Run(s.session)
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

	indexCursor, err := r.DB(input.Database).Table(input.Table).IndexList().Run(s.session)
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

	countCursor, err := r.DB(input.Database).Table(input.Table).Count().Run(s.session)
	if err == nil {
		defer countCursor.Close()
		var count int
		if err := countCursor.One(&count); err == nil {
			output.DocCount = count
		}
	}

	return nil, output, nil
}

func (s *RethinkDBServer) WriteData(ctx context.Context, req *mcp.CallToolRequest, input WriteDataInput) (*mcp.CallToolResult, WriteDataOutput, error) {
	if input.Database == "" || input.Table == "" {
		return nil, WriteDataOutput{}, fmt.Errorf("database and table names are required")
	}

	if len(input.Data) == 0 {
		return nil, WriteDataOutput{}, fmt.Errorf("data is required")
	}

	operation := input.Operation
	if operation == "" {
		operation = "insert"
	}

	// Validate operation
	switch operation {
	case "insert", "update", "upsert", "delete":
		// valid
	default:
		return nil, WriteDataOutput{}, fmt.Errorf("invalid operation %q: must be one of insert, update, upsert, delete", operation)
	}

	// Parse the data - could be a single document or an array
	var data interface{}
	if err := json.Unmarshal(input.Data, &data); err != nil {
		return nil, WriteDataOutput{}, fmt.Errorf("failed to parse data: %w", err)
	}

	table := r.DB(input.Database).Table(input.Table)
	var writeResp r.WriteResponse
	var err error

	switch operation {
	case "insert":
		writeResp, err = table.Insert(data).RunWrite(s.session)

	case "update":
		// For update: if data is a single doc with a primary key, use Get().Update()
		// If it's an array, use insert with conflict: "update"
		writeResp, err = table.Insert(data, r.InsertOpts{Conflict: "update"}).RunWrite(s.session)

	case "upsert":
		// Upsert: insert with conflict: "replace"
		writeResp, err = table.Insert(data, r.InsertOpts{Conflict: "replace"}).RunWrite(s.session)

	case "delete":
		// For delete: use the data as a filter to find and delete matching documents
		// If the data has an "id" field and nothing else, delete by primary key
		docMap, isMap := data.(map[string]interface{})
		if isMap {
			if id, hasID := docMap["id"]; hasID && len(docMap) == 1 {
				// Delete by primary key
				writeResp, err = table.Get(id).Delete().RunWrite(s.session)
			} else {
				// Delete by filter
				writeResp, err = table.Filter(data).Delete().RunWrite(s.session)
			}
		} else {
			// Array of IDs or documents - try filter
			writeResp, err = table.Filter(data).Delete().RunWrite(s.session)
		}
	}

	if err != nil {
		return nil, WriteDataOutput{}, fmt.Errorf("failed to execute %s: %w", operation, err)
	}

	output := WriteDataOutput{
		Database:  input.Database,
		Table:     input.Table,
		Operation: operation,
		Inserted:  writeResp.Inserted,
		Replaced:  writeResp.Replaced,
		Unchanged: writeResp.Unchanged,
		Deleted:   writeResp.Deleted,
		Errors:    writeResp.Errors,
	}
	if writeResp.FirstError != "" {
		output.FirstError = writeResp.FirstError
	}

	return nil, output, nil
}

// ─── Tool Registration ──────────────────────────────────────────────────────

func (s *RethinkDBServer) RegisterTools(mcpServer *mcp.Server) {
	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "list_databases",
		Description: "List all databases in RethinkDB",
	}, s.ListDatabases)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "list_tables",
		Description: "List all tables in a RethinkDB database",
	}, s.ListTables)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "query_table",
		Description: "Query data from a RethinkDB table. Supports filtering, ordering, and limiting results.",
	}, s.QueryTable)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "table_info",
		Description: "Get table information including primary key, indexes, and document count",
	}, s.TableInfo)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "write_data",
		Description: "Write data to a RethinkDB table. Supports insert, update, upsert, and delete operations. Data can be a single document or an array of documents.",
	}, s.WriteData)
}

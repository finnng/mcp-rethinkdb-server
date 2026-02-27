// Package server provides an MCP tool server for RethinkDB, including handlers
// for listing databases, tables, querying, and writing data.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

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
	Database string `json:"database" jsonschema:"The database name to list tables from"`
}

type ListTablesOutput struct {
	Database string   `json:"database"`
	Tables   []string `json:"tables"`
}

type QueryTableInput struct {
	Database string         `json:"database" jsonschema:"The database name"`
	Table    string         `json:"table" jsonschema:"The table name"`
	Filter   map[string]any `json:"filter,omitempty" jsonschema:"Optional filter object for the query"`
	Limit    int            `json:"limit,omitempty" jsonschema:"Maximum number of results (default 100, max 1000)"`
	OrderBy  string         `json:"order_by,omitempty" jsonschema:"Optional field to order results by"`
}

type QueryTableOutput struct {
	Database        string  `json:"database"`
	Table           string  `json:"table"`
	Count           int     `json:"count"`
	Results         []any   `json:"results"`
	ExecutionTimeMs float64 `json:"execution_time_ms"`
}

type TableInfoInput struct {
	Database string `json:"database" jsonschema:"The database name"`
	Table    string `json:"table" jsonschema:"The table name"`
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
	Database  string          `json:"database" jsonschema:"The database name"`
	Table     string          `json:"table" jsonschema:"The table name"`
	Data      json.RawMessage `json:"data" jsonschema:"The data to write. For insert/update/upsert: a document or array of documents. For delete: a document with the primary key field, or a filter object to match multiple documents."`
	Operation string          `json:"operation,omitempty" jsonschema:"The write operation: insert (default), update, upsert, or delete"`
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

type AggregateInput struct {
	Database         string         `json:"database" jsonschema:"The database name"`
	Table            string         `json:"table" jsonschema:"The table name"`
	Operation        string         `json:"operation" jsonschema:"Aggregation operation: count, sum, avg, min, max, or group"`
	Field            string         `json:"field,omitempty" jsonschema:"Field to aggregate on (required for sum, avg, min, max, group)"`
	Filter           map[string]any `json:"filter,omitempty" jsonschema:"Optional filter to apply before aggregation"`
	GroupAggregation string         `json:"group_aggregation,omitempty" jsonschema:"When operation is group, apply this aggregation per group: count, sum, avg, min, max"`
}

type AggregateOutput struct {
	Database  string `json:"database"`
	Table     string `json:"table"`
	Operation string `json:"operation"`
	Field     string `json:"field,omitempty"`
	Value     any    `json:"value"`
}

type AdvancedQueryInput struct {
	Database      string          `json:"database" jsonschema:"The database name"`
	Table         string          `json:"table" jsonschema:"The table name"`
	Operation     string          `json:"operation" jsonschema:"Operation: eq_join, between, contains, or map"`
	JoinField     string          `json:"join_field,omitempty" jsonschema:"Field to join on (for eq_join)"`
	JoinTable     string          `json:"join_table,omitempty" jsonschema:"Table to join with (for eq_join)"`
	Index         string          `json:"index,omitempty" jsonschema:"Index to use (for between)"`
	LowerBound    json.RawMessage `json:"lower_bound,omitempty" jsonschema:"Lower bound value (for between, inclusive)"`
	UpperBound    json.RawMessage `json:"upper_bound,omitempty" jsonschema:"Upper bound value (for between, exclusive)"`
	ContainsField string          `json:"contains_field,omitempty" jsonschema:"Array field to check (for contains)"`
	ContainsValue json.RawMessage `json:"contains_value,omitempty" jsonschema:"Value to look for in the array field (for contains)"`
	MapExpr       map[string]any  `json:"map_expr,omitempty" jsonschema:"Object with field names set to true to pluck from each document (for map)"`
	Limit         int             `json:"limit,omitempty" jsonschema:"Maximum number of results (default 100, max 1000)"`
}

type AdvancedQueryOutput struct {
	Database  string `json:"database"`
	Table     string `json:"table"`
	Operation string `json:"operation"`
	Count     int    `json:"count"`
	Results   []any  `json:"results"`
}

type SchemaInspectorInput struct {
	Database   string `json:"database" jsonschema:"The database name"`
	Table      string `json:"table" jsonschema:"The table name"`
	SampleSize int    `json:"sample_size,omitempty" jsonschema:"Number of documents to sample for schema inference (default 100)"`
}

type FieldInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type SchemaInspectorOutput struct {
	Database   string      `json:"database"`
	Table      string      `json:"table"`
	PrimaryKey string      `json:"primary_key"`
	Indexes    []string    `json:"indexes"`
	DocCount   int         `json:"doc_count"`
	SampleSize int         `json:"sample_size"`
	Fields     []FieldInfo `json:"fields"`
}

type IndexInfoInput struct {
	Database string `json:"database" jsonschema:"The database name"`
	Table    string `json:"table" jsonschema:"The table name"`
}

type IndexDetail struct {
	Name     string `json:"name"`
	Ready    bool   `json:"ready"`
	Multi    bool   `json:"multi"`
	Geo      bool   `json:"geo"`
	Outdated bool   `json:"outdated"`
}

type IndexInfoOutput struct {
	Database string        `json:"database"`
	Table    string        `json:"table"`
	Indexes  []IndexDetail `json:"indexes"`
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

	start := time.Now()

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

	elapsed := time.Since(start)

	return nil, QueryTableOutput{
		Database:        input.Database,
		Table:           input.Table,
		Count:           len(results),
		Results:         results,
		ExecutionTimeMs: float64(elapsed.Microseconds()) / 1000.0,
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

func (s *RethinkDBServer) Aggregate(ctx context.Context, req *mcp.CallToolRequest, input AggregateInput) (*mcp.CallToolResult, AggregateOutput, error) {
	if input.Database == "" || input.Table == "" {
		return nil, AggregateOutput{}, fmt.Errorf("database and table names are required")
	}

	switch input.Operation {
	case "count", "sum", "avg", "min", "max", "group":
		// valid
	default:
		return nil, AggregateOutput{}, fmt.Errorf("invalid aggregation operation %q: must be one of count, sum, avg, min, max, group", input.Operation)
	}

	if input.Operation != "count" && input.Field == "" {
		return nil, AggregateOutput{}, fmt.Errorf("field is required for %s operation", input.Operation)
	}

	query := r.DB(input.Database).Table(input.Table)

	if len(input.Filter) > 0 {
		query = query.Filter(input.Filter)
	}

	var term r.Term
	switch input.Operation {
	case "count":
		term = query.Count()
	case "sum":
		term = query.Sum(input.Field)
	case "avg":
		term = query.Avg(input.Field)
	case "min":
		term = query.Min(input.Field)
	case "max":
		term = query.Max(input.Field)
	case "group":
		grouped := query.Group(input.Field)
		switch input.GroupAggregation {
		case "count":
			term = grouped.Count().Ungroup()
		case "sum":
			term = grouped.Sum(input.Field).Ungroup()
		case "avg":
			term = grouped.Avg(input.Field).Ungroup()
		case "min":
			term = grouped.Min(input.Field).Ungroup()
		case "max":
			term = grouped.Max(input.Field).Ungroup()
		default:
			term = grouped.Ungroup()
		}
	}

	cursor, err := term.Run(s.session)
	if err != nil {
		return nil, AggregateOutput{}, fmt.Errorf("failed to execute aggregation: %w", err)
	}
	defer cursor.Close()

	var value interface{}
	if input.Operation == "group" {
		var results []interface{}
		if err := cursor.All(&results); err != nil {
			return nil, AggregateOutput{}, fmt.Errorf("failed to read group results: %w", err)
		}
		value = results
	} else {
		if err := cursor.One(&value); err != nil {
			return nil, AggregateOutput{}, fmt.Errorf("failed to read aggregation result: %w", err)
		}
	}

	return nil, AggregateOutput{
		Database:  input.Database,
		Table:     input.Table,
		Operation: input.Operation,
		Field:     input.Field,
		Value:     value,
	}, nil
}

func applyLimit(limit int) int {
	if limit <= 0 {
		return 100
	}
	if limit > 1000 {
		return 1000
	}
	return limit
}

func (s *RethinkDBServer) AdvancedQuery(ctx context.Context, req *mcp.CallToolRequest, input AdvancedQueryInput) (*mcp.CallToolResult, AdvancedQueryOutput, error) {
	if input.Database == "" || input.Table == "" {
		return nil, AdvancedQueryOutput{}, fmt.Errorf("database and table names are required")
	}

	limit := applyLimit(input.Limit)

	table := r.DB(input.Database).Table(input.Table)
	var results []interface{}

	switch input.Operation {
	case "eq_join":
		if input.JoinField == "" {
			return nil, AdvancedQueryOutput{}, fmt.Errorf("join_field is required for eq_join")
		}
		if input.JoinTable == "" {
			return nil, AdvancedQueryOutput{}, fmt.Errorf("join_table is required for eq_join")
		}
		cursor, err := table.EqJoin(input.JoinField, r.DB(input.Database).Table(input.JoinTable)).Limit(limit).Run(s.session)
		if err != nil {
			return nil, AdvancedQueryOutput{}, fmt.Errorf("failed to execute eq_join: %w", err)
		}
		defer cursor.Close()
		if err := cursor.All(&results); err != nil {
			return nil, AdvancedQueryOutput{}, fmt.Errorf("failed to read eq_join results: %w", err)
		}

	case "between":
		if input.Index == "" {
			return nil, AdvancedQueryOutput{}, fmt.Errorf("index is required for between")
		}
		var lower, upper interface{}
		if len(input.LowerBound) > 0 {
			if err := json.Unmarshal(input.LowerBound, &lower); err != nil {
				return nil, AdvancedQueryOutput{}, fmt.Errorf("failed to parse lower_bound: %w", err)
			}
		} else {
			lower = r.MinVal
		}
		if len(input.UpperBound) > 0 {
			if err := json.Unmarshal(input.UpperBound, &upper); err != nil {
				return nil, AdvancedQueryOutput{}, fmt.Errorf("failed to parse upper_bound: %w", err)
			}
		} else {
			upper = r.MaxVal
		}
		cursor, err := table.Between(lower, upper, r.BetweenOpts{Index: input.Index}).Limit(limit).Run(s.session)
		if err != nil {
			return nil, AdvancedQueryOutput{}, fmt.Errorf("failed to execute between: %w", err)
		}
		defer cursor.Close()
		if err := cursor.All(&results); err != nil {
			return nil, AdvancedQueryOutput{}, fmt.Errorf("failed to read between results: %w", err)
		}

	case "contains":
		if input.ContainsField == "" {
			return nil, AdvancedQueryOutput{}, fmt.Errorf("contains_field is required for contains")
		}
		var val interface{}
		if len(input.ContainsValue) > 0 {
			if err := json.Unmarshal(input.ContainsValue, &val); err != nil {
				return nil, AdvancedQueryOutput{}, fmt.Errorf("failed to parse contains_value: %w", err)
			}
		} else {
			return nil, AdvancedQueryOutput{}, fmt.Errorf("contains_value is required for contains")
		}
		cursor, err := table.Filter(func(row r.Term) r.Term {
			return row.Field(input.ContainsField).Contains(val)
		}).Limit(limit).Run(s.session)
		if err != nil {
			return nil, AdvancedQueryOutput{}, fmt.Errorf("failed to execute contains: %w", err)
		}
		defer cursor.Close()
		if err := cursor.All(&results); err != nil {
			return nil, AdvancedQueryOutput{}, fmt.Errorf("failed to read contains results: %w", err)
		}

	case "map":
		if len(input.MapExpr) == 0 {
			return nil, AdvancedQueryOutput{}, fmt.Errorf("map_expr is required for map")
		}
		var fields []interface{}
		for k := range input.MapExpr {
			fields = append(fields, k)
		}
		cursor, err := table.Pluck(fields...).Limit(limit).Run(s.session)
		if err != nil {
			return nil, AdvancedQueryOutput{}, fmt.Errorf("failed to execute map/pluck: %w", err)
		}
		defer cursor.Close()
		if err := cursor.All(&results); err != nil {
			return nil, AdvancedQueryOutput{}, fmt.Errorf("failed to read map results: %w", err)
		}

	default:
		return nil, AdvancedQueryOutput{}, fmt.Errorf("invalid operation %q: must be one of eq_join, between, contains, map", input.Operation)
	}

	return nil, AdvancedQueryOutput{
		Database:  input.Database,
		Table:     input.Table,
		Operation: input.Operation,
		Count:     len(results),
		Results:   results,
	}, nil
}

func (s *RethinkDBServer) SchemaInspector(ctx context.Context, req *mcp.CallToolRequest, input SchemaInspectorInput) (*mcp.CallToolResult, SchemaInspectorOutput, error) {
	if input.Database == "" || input.Table == "" {
		return nil, SchemaInspectorOutput{}, fmt.Errorf("database and table names are required")
	}

	sampleSize := input.SampleSize
	if sampleSize <= 0 {
		sampleSize = 100
	}

	output := SchemaInspectorOutput{
		Database:   input.Database,
		Table:      input.Table,
		SampleSize: sampleSize,
	}

	// Get primary key
	infoCursor, err := r.DB(input.Database).Table(input.Table).Info().Run(s.session)
	if err != nil {
		return nil, SchemaInspectorOutput{}, fmt.Errorf("failed to get table info: %w", err)
	}
	defer infoCursor.Close()
	var info map[string]interface{}
	if err := infoCursor.One(&info); err != nil {
		return nil, SchemaInspectorOutput{}, fmt.Errorf("failed to read table info: %w", err)
	}
	if pk, ok := info["primary_key"].(string); ok {
		output.PrimaryKey = pk
	}

	// Get indexes
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

	// Get doc count
	countCursor, err := r.DB(input.Database).Table(input.Table).Count().Run(s.session)
	if err == nil {
		defer countCursor.Close()
		var count int
		if err := countCursor.One(&count); err == nil {
			output.DocCount = count
		}
	}

	// Sample documents to infer schema
	cursor, err := r.DB(input.Database).Table(input.Table).Limit(sampleSize).Run(s.session)
	if err != nil {
		return nil, SchemaInspectorOutput{}, fmt.Errorf("failed to sample documents: %w", err)
	}
	defer cursor.Close()

	var docs []map[string]interface{}
	if err := cursor.All(&docs); err != nil {
		return nil, SchemaInspectorOutput{}, fmt.Errorf("failed to read sample documents: %w", err)
	}

	// Collect all field names and their types
	fieldTypes := make(map[string]string)
	for _, doc := range docs {
		for k, v := range doc {
			if _, exists := fieldTypes[k]; !exists {
				fieldTypes[k] = inferType(v)
			}
		}
	}

	fields := make([]FieldInfo, 0, len(fieldTypes))
	for name, typ := range fieldTypes {
		fields = append(fields, FieldInfo{Name: name, Type: typ})
	}
	output.Fields = fields

	return nil, output, nil
}

func inferType(v interface{}) string {
	if v == nil {
		return "null"
	}
	rt := reflect.TypeOf(v)
	switch rt.Kind() {
	case reflect.String:
		return "string"
	case reflect.Float64, reflect.Float32, reflect.Int, reflect.Int64:
		return "number"
	case reflect.Bool:
		return "bool"
	case reflect.Slice:
		return "array"
	case reflect.Map:
		return "object"
	default:
		return rt.String()
	}
}

func (s *RethinkDBServer) IndexInfo(ctx context.Context, req *mcp.CallToolRequest, input IndexInfoInput) (*mcp.CallToolResult, IndexInfoOutput, error) {
	if input.Database == "" || input.Table == "" {
		return nil, IndexInfoOutput{}, fmt.Errorf("database and table names are required")
	}

	output := IndexInfoOutput{
		Database: input.Database,
		Table:    input.Table,
	}

	cursor, err := r.DB(input.Database).Table(input.Table).IndexStatus().Run(s.session)
	if err != nil {
		return nil, IndexInfoOutput{}, fmt.Errorf("failed to get index status: %w", err)
	}
	defer cursor.Close()

	var statuses []map[string]interface{}
	if err := cursor.All(&statuses); err != nil {
		return nil, IndexInfoOutput{}, fmt.Errorf("failed to read index status: %w", err)
	}

	indexes := make([]IndexDetail, 0, len(statuses))
	for _, s := range statuses {
		detail := IndexDetail{}
		if name, ok := s["index"].(string); ok {
			detail.Name = name
		}
		if ready, ok := s["ready"].(bool); ok {
			detail.Ready = ready
		}
		if multi, ok := s["multi"].(bool); ok {
			detail.Multi = multi
		}
		if geo, ok := s["geo"].(bool); ok {
			detail.Geo = geo
		}
		if outdated, ok := s["outdated"].(bool); ok {
			detail.Outdated = outdated
		}
		indexes = append(indexes, detail)
	}
	output.Indexes = indexes

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

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "aggregate",
		Description: "Run aggregation operations on a RethinkDB table: count, sum, avg, min, max, or group. Supports optional filtering and group-level aggregations.",
	}, s.Aggregate)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "advanced_query",
		Description: "Run advanced queries on a RethinkDB table: eq_join (join two tables by field), between (range query on an index), contains (filter by array field contents), or map (pluck specific fields from documents).",
	}, s.AdvancedQuery)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "schema_inspector",
		Description: "Inspect the schema of a RethinkDB table by sampling documents. Returns field names and inferred types, primary key, indexes, and document count.",
	}, s.SchemaInspector)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "index_info",
		Description: "Get detailed information about all secondary indexes on a RethinkDB table, including ready status, multi, geo, and outdated flags.",
	}, s.IndexInfo)
}

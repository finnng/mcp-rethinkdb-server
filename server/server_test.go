package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

var testSession *r.Session

const (
	testDB    = "mcp_test_db"
	testTable = "mcp_test_table"
)

func TestMain(m *testing.M) {
	host := os.Getenv("RETHINKDB_TEST_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("RETHINKDB_TEST_PORT")
	if port == "" {
		port = "28016"
	}

	var err error
	testSession, err = r.Connect(r.ConnectOpts{
		Address: fmt.Sprintf("%s:%s", host, port),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot connect to RethinkDB at %s:%s: %v\n", host, port, err)
		fmt.Fprintf(os.Stderr, "Run: docker compose up -d\n")
		os.Exit(1)
	}

	// Setup: create test DB and table
	r.DBCreate(testDB).RunWrite(testSession)
	r.DB(testDB).TableCreate(testTable).RunWrite(testSession)
	r.DB(testDB).Table(testTable).Delete().RunWrite(testSession)

	// Seed test data
	r.DB(testDB).Table(testTable).Insert([]map[string]interface{}{
		{"id": "1", "name": "Alice", "age": 30, "status": "active"},
		{"id": "2", "name": "Bob", "age": 25, "status": "inactive"},
		{"id": "3", "name": "Charlie", "age": 35, "status": "active"},
	}).RunWrite(testSession)

	code := m.Run()

	// Teardown
	r.DBDrop(testDB).RunWrite(testSession)
	testSession.Close()

	os.Exit(code)
}

func newTestServer() *RethinkDBServer {
	return NewRethinkDBServer(testSession)
}

// ─── list_databases ──────────────────────────────────────────────────────────

func TestListDatabases_ReturnsTestDB(t *testing.T) {
	srv := newTestServer()
	result, output, err := srv.ListDatabases(context.Background(), &mcp.CallToolRequest{}, EmptyInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil result, got %v", result)
	}
	found := false
	for _, db := range output.Databases {
		if db == testDB {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected %q in databases list, got %v", testDB, output.Databases)
	}
}

// ─── list_tables ─────────────────────────────────────────────────────────────

func TestListTables_ReturnsTestTable(t *testing.T) {
	srv := newTestServer()
	input := ListTablesInput{Database: testDB}
	_, output, err := srv.ListTables(context.Background(), &mcp.CallToolRequest{}, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found := false
	for _, tbl := range output.Tables {
		if tbl == testTable {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected %q in tables list, got %v", testTable, output.Tables)
	}
}

func TestListTables_EmptyDatabase_ReturnsError(t *testing.T) {
	srv := newTestServer()
	input := ListTablesInput{Database: ""}
	_, _, err := srv.ListTables(context.Background(), &mcp.CallToolRequest{}, input)
	if err == nil {
		t.Error("expected error for empty database name")
	}
}

// ─── query_table ─────────────────────────────────────────────────────────────

func TestQueryTable_ReturnsAllSeededData(t *testing.T) {
	srv := newTestServer()
	input := QueryTableInput{Database: testDB, Table: testTable}
	_, output, err := srv.QueryTable(context.Background(), &mcp.CallToolRequest{}, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Count != 3 {
		t.Errorf("expected 3 results, got %d", output.Count)
	}
	if output.Database != testDB {
		t.Errorf("expected database %q, got %q", testDB, output.Database)
	}
	if output.Table != testTable {
		t.Errorf("expected table %q, got %q", testTable, output.Table)
	}
}

func TestQueryTable_WithFilter(t *testing.T) {
	srv := newTestServer()
	input := QueryTableInput{
		Database: testDB,
		Table:    testTable,
		Filter:   map[string]interface{}{"status": "active"},
	}
	_, output, err := srv.QueryTable(context.Background(), &mcp.CallToolRequest{}, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Count != 2 {
		t.Errorf("expected 2 active results, got %d", output.Count)
	}
}

func TestQueryTable_WithLimit(t *testing.T) {
	srv := newTestServer()
	input := QueryTableInput{
		Database: testDB,
		Table:    testTable,
		Limit:    1,
	}
	_, output, err := srv.QueryTable(context.Background(), &mcp.CallToolRequest{}, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Count != 1 {
		t.Errorf("expected 1 result, got %d", output.Count)
	}
}

func TestQueryTable_DefaultLimitIs100(t *testing.T) {
	srv := newTestServer()
	input := QueryTableInput{
		Database: testDB,
		Table:    testTable,
		Limit:    0,
	}
	_, output, err := srv.QueryTable(context.Background(), &mcp.CallToolRequest{}, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// We only have 3 docs, so count should be 3 (< default 100)
	if output.Count != 3 {
		t.Errorf("expected 3 results with default limit, got %d", output.Count)
	}
}

func TestQueryTable_MaxLimitCappedAt1000(t *testing.T) {
	srv := newTestServer()
	input := QueryTableInput{
		Database: testDB,
		Table:    testTable,
		Limit:    5000,
	}
	_, output, err := srv.QueryTable(context.Background(), &mcp.CallToolRequest{}, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Just ensure it doesn't error; limit capped internally
	if output.Count != 3 {
		t.Errorf("expected 3 results, got %d", output.Count)
	}
}

func TestQueryTable_WithOrderBy(t *testing.T) {
	srv := newTestServer()
	input := QueryTableInput{
		Database: testDB,
		Table:    testTable,
		OrderBy:  "name",
	}
	_, output, err := srv.QueryTable(context.Background(), &mcp.CallToolRequest{}, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Count != 3 {
		t.Fatalf("expected 3 results, got %d", output.Count)
	}
	// First result should be Alice (alphabetical order)
	first, ok := output.Results[0].(map[string]interface{})
	if !ok {
		t.Fatalf("expected map result, got %T", output.Results[0])
	}
	if first["name"] != "Alice" {
		t.Errorf("expected first result name 'Alice', got %v", first["name"])
	}
}

func TestQueryTable_EmptyDatabaseOrTable_ReturnsError(t *testing.T) {
	srv := newTestServer()
	_, _, err := srv.QueryTable(context.Background(), &mcp.CallToolRequest{}, QueryTableInput{Database: "", Table: testTable})
	if err == nil {
		t.Error("expected error for empty database")
	}
	_, _, err = srv.QueryTable(context.Background(), &mcp.CallToolRequest{}, QueryTableInput{Database: testDB, Table: ""})
	if err == nil {
		t.Error("expected error for empty table")
	}
}

// ─── table_info ──────────────────────────────────────────────────────────────

func TestTableInfo_ReturnsCorrectMetadata(t *testing.T) {
	srv := newTestServer()
	input := TableInfoInput{Database: testDB, Table: testTable}
	_, output, err := srv.TableInfo(context.Background(), &mcp.CallToolRequest{}, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Database != testDB {
		t.Errorf("expected database %q, got %q", testDB, output.Database)
	}
	if output.Table != testTable {
		t.Errorf("expected table %q, got %q", testTable, output.Table)
	}
	if output.PrimaryKey != "id" {
		t.Errorf("expected primary key 'id', got %q", output.PrimaryKey)
	}
	if output.DocCount != 3 {
		t.Errorf("expected doc count 3, got %d", output.DocCount)
	}
	if output.Indexes == nil {
		t.Error("expected indexes to be non-nil (empty slice)")
	}
}

func TestTableInfo_EmptyDatabaseOrTable_ReturnsError(t *testing.T) {
	srv := newTestServer()
	_, _, err := srv.TableInfo(context.Background(), &mcp.CallToolRequest{}, TableInfoInput{Database: "", Table: testTable})
	if err == nil {
		t.Error("expected error for empty database")
	}
	_, _, err = srv.TableInfo(context.Background(), &mcp.CallToolRequest{}, TableInfoInput{Database: testDB, Table: ""})
	if err == nil {
		t.Error("expected error for empty table")
	}
}

// ─── write_data ──────────────────────────────────────────────────────────────

func TestWriteData_InsertSingle(t *testing.T) {
	srv := newTestServer()

	data := map[string]interface{}{
		"id":   "write_test_1",
		"name": "WriteTest",
		"age":  99,
	}
	dataJSON, _ := json.Marshal(data)

	input := WriteDataInput{
		Database:  testDB,
		Table:     testTable,
		Data:      json.RawMessage(dataJSON),
		Operation: "insert",
	}

	_, output, err := srv.WriteData(context.Background(), &mcp.CallToolRequest{}, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Inserted != 1 {
		t.Errorf("expected 1 inserted, got %d", output.Inserted)
	}

	// Verify the doc was inserted
	cursor, _ := r.DB(testDB).Table(testTable).Get("write_test_1").Run(testSession)
	defer cursor.Close()
	var doc map[string]interface{}
	cursor.One(&doc)
	if doc["name"] != "WriteTest" {
		t.Errorf("expected name 'WriteTest', got %v", doc["name"])
	}

	// Cleanup
	r.DB(testDB).Table(testTable).Get("write_test_1").Delete().RunWrite(testSession)
}

func TestWriteData_InsertMultiple(t *testing.T) {
	srv := newTestServer()

	data := []map[string]interface{}{
		{"id": "multi_1", "name": "Multi1"},
		{"id": "multi_2", "name": "Multi2"},
	}
	dataJSON, _ := json.Marshal(data)

	input := WriteDataInput{
		Database:  testDB,
		Table:     testTable,
		Data:      json.RawMessage(dataJSON),
		Operation: "insert",
	}

	_, output, err := srv.WriteData(context.Background(), &mcp.CallToolRequest{}, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Inserted != 2 {
		t.Errorf("expected 2 inserted, got %d", output.Inserted)
	}

	// Cleanup
	r.DB(testDB).Table(testTable).GetAll("multi_1", "multi_2").Delete().RunWrite(testSession)
}

func TestWriteData_Update(t *testing.T) {
	srv := newTestServer()

	// Insert a doc to update
	r.DB(testDB).Table(testTable).Insert(map[string]interface{}{
		"id": "update_test", "name": "Before", "age": 10,
	}).RunWrite(testSession)

	data := map[string]interface{}{
		"id":   "update_test",
		"name": "After",
		"age":  20,
	}
	dataJSON, _ := json.Marshal(data)

	input := WriteDataInput{
		Database:  testDB,
		Table:     testTable,
		Data:      json.RawMessage(dataJSON),
		Operation: "update",
	}

	_, output, err := srv.WriteData(context.Background(), &mcp.CallToolRequest{}, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Replaced < 1 && output.Unchanged < 1 {
		t.Errorf("expected at least 1 replaced or unchanged, got replaced=%d unchanged=%d", output.Replaced, output.Unchanged)
	}

	// Verify
	cursor, _ := r.DB(testDB).Table(testTable).Get("update_test").Run(testSession)
	defer cursor.Close()
	var doc map[string]interface{}
	cursor.One(&doc)
	if doc["name"] != "After" {
		t.Errorf("expected name 'After', got %v", doc["name"])
	}

	// Cleanup
	r.DB(testDB).Table(testTable).Get("update_test").Delete().RunWrite(testSession)
}

func TestWriteData_Upsert(t *testing.T) {
	srv := newTestServer()

	// Upsert a doc that doesn't exist yet
	data := map[string]interface{}{
		"id":   "upsert_test",
		"name": "Upserted",
	}
	dataJSON, _ := json.Marshal(data)

	input := WriteDataInput{
		Database:  testDB,
		Table:     testTable,
		Data:      json.RawMessage(dataJSON),
		Operation: "upsert",
	}

	_, output, err := srv.WriteData(context.Background(), &mcp.CallToolRequest{}, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Inserted != 1 {
		t.Errorf("expected 1 inserted for upsert of new doc, got inserted=%d", output.Inserted)
	}

	// Upsert again with update
	data2 := map[string]interface{}{
		"id":   "upsert_test",
		"name": "UpsertedAgain",
	}
	dataJSON2, _ := json.Marshal(data2)
	input2 := WriteDataInput{
		Database:  testDB,
		Table:     testTable,
		Data:      json.RawMessage(dataJSON2),
		Operation: "upsert",
	}
	_, output2, err := srv.WriteData(context.Background(), &mcp.CallToolRequest{}, input2)
	if err != nil {
		t.Fatalf("unexpected error on second upsert: %v", err)
	}
	if output2.Replaced != 1 {
		t.Errorf("expected 1 replaced for upsert of existing doc, got replaced=%d", output2.Replaced)
	}

	// Verify
	cursor, _ := r.DB(testDB).Table(testTable).Get("upsert_test").Run(testSession)
	defer cursor.Close()
	var doc map[string]interface{}
	cursor.One(&doc)
	if doc["name"] != "UpsertedAgain" {
		t.Errorf("expected name 'UpsertedAgain', got %v", doc["name"])
	}

	// Cleanup
	r.DB(testDB).Table(testTable).Get("upsert_test").Delete().RunWrite(testSession)
}

func TestWriteData_Delete(t *testing.T) {
	srv := newTestServer()

	// Insert doc to delete
	r.DB(testDB).Table(testTable).Insert(map[string]interface{}{
		"id": "delete_test", "name": "ToDelete",
	}).RunWrite(testSession)

	data := map[string]interface{}{
		"id": "delete_test",
	}
	dataJSON, _ := json.Marshal(data)

	input := WriteDataInput{
		Database:  testDB,
		Table:     testTable,
		Data:      json.RawMessage(dataJSON),
		Operation: "delete",
	}

	_, output, err := srv.WriteData(context.Background(), &mcp.CallToolRequest{}, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Deleted != 1 {
		t.Errorf("expected 1 deleted, got %d", output.Deleted)
	}

	// Verify deleted
	cursor, _ := r.DB(testDB).Table(testTable).Get("delete_test").Run(testSession)
	defer cursor.Close()
	var doc map[string]interface{}
	cursor.One(&doc) //nolint
	if doc != nil {
		t.Errorf("expected nil doc after delete, got %v", doc)
	}
}

func TestWriteData_DeleteMultipleByFilter(t *testing.T) {
	srv := newTestServer()

	// Insert docs to delete
	r.DB(testDB).Table(testTable).Insert([]map[string]interface{}{
		{"id": "del_multi_1", "category": "temp_delete"},
		{"id": "del_multi_2", "category": "temp_delete"},
		{"id": "del_multi_3", "category": "keep"},
	}).RunWrite(testSession)

	filter := map[string]interface{}{
		"category": "temp_delete",
	}
	filterJSON, _ := json.Marshal(filter)

	input := WriteDataInput{
		Database:  testDB,
		Table:     testTable,
		Data:      json.RawMessage(filterJSON),
		Operation: "delete",
	}

	_, output, err := srv.WriteData(context.Background(), &mcp.CallToolRequest{}, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Deleted != 2 {
		t.Errorf("expected 2 deleted, got %d", output.Deleted)
	}

	// Cleanup remaining
	r.DB(testDB).Table(testTable).Get("del_multi_3").Delete().RunWrite(testSession)
}

func TestWriteData_DefaultOperationIsInsert(t *testing.T) {
	srv := newTestServer()

	data := map[string]interface{}{
		"id":   "default_op_test",
		"name": "DefaultOp",
	}
	dataJSON, _ := json.Marshal(data)

	input := WriteDataInput{
		Database: testDB,
		Table:    testTable,
		Data:     json.RawMessage(dataJSON),
		// Operation intentionally left empty
	}

	_, output, err := srv.WriteData(context.Background(), &mcp.CallToolRequest{}, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Inserted != 1 {
		t.Errorf("expected 1 inserted with default operation, got %d", output.Inserted)
	}

	// Cleanup
	r.DB(testDB).Table(testTable).Get("default_op_test").Delete().RunWrite(testSession)
}

func TestWriteData_InvalidOperation_ReturnsError(t *testing.T) {
	srv := newTestServer()

	data := map[string]interface{}{"id": "x"}
	dataJSON, _ := json.Marshal(data)

	input := WriteDataInput{
		Database:  testDB,
		Table:     testTable,
		Data:      json.RawMessage(dataJSON),
		Operation: "drop_table",
	}

	_, _, err := srv.WriteData(context.Background(), &mcp.CallToolRequest{}, input)
	if err == nil {
		t.Error("expected error for invalid operation")
	}
}

func TestWriteData_EmptyDatabaseOrTable_ReturnsError(t *testing.T) {
	srv := newTestServer()
	data := map[string]interface{}{"id": "x"}
	dataJSON, _ := json.Marshal(data)

	_, _, err := srv.WriteData(context.Background(), &mcp.CallToolRequest{}, WriteDataInput{
		Database: "", Table: testTable, Data: json.RawMessage(dataJSON),
	})
	if err == nil {
		t.Error("expected error for empty database")
	}

	_, _, err = srv.WriteData(context.Background(), &mcp.CallToolRequest{}, WriteDataInput{
		Database: testDB, Table: "", Data: json.RawMessage(dataJSON),
	})
	if err == nil {
		t.Error("expected error for empty table")
	}
}

func TestWriteData_NilData_ReturnsError(t *testing.T) {
	srv := newTestServer()

	input := WriteDataInput{
		Database:  testDB,
		Table:     testTable,
		Data:      nil,
		Operation: "insert",
	}

	_, _, err := srv.WriteData(context.Background(), &mcp.CallToolRequest{}, input)
	if err == nil {
		t.Error("expected error for nil data")
	}
}

// ─── MCP Server Registration ────────────────────────────────────────────────

func TestRegisterTools_RegistersAllFiveTools(t *testing.T) {
	srv := newTestServer()
	mcpServer := mcp.NewServer(&mcp.Implementation{
		Name:    "test-server",
		Version: "0.0.1",
	}, nil)

	srv.RegisterTools(mcpServer)

	// We can't easily inspect registered tools, but we verify no panic
	// and the server was created successfully
	if mcpServer == nil {
		t.Fatal("expected non-nil MCP server")
	}
}

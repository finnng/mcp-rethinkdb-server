# MCP RethinkDB Server

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Version](https://img.shields.io/badge/Go-1.23%2B-blue)](https://golang.org/dl/)
[![MCP](https://img.shields.io/badge/MCP-Compatible-green)](https://modelcontextprotocol.io/)
[![Docker Hub](https://img.shields.io/badge/Docker-finn13/mcp--rethinkdb--server-blue)](https://hub.docker.com/r/finn13/mcp-rethinkdb-server)

A Model Context Protocol (MCP) server that provides access to RethinkDB databases. This server enables AI assistants like Claude to query, explore, and write RethinkDB data.

## Features

- **Five tools available**:
  - `list_databases` - List all databases
  - `list_tables` - List tables in a database
  - `query_table` - Query data with filtering, ordering, and limits
  - `table_info` - Get table metadata (primary key, indexes, doc count)
  - `write_data` - Insert, update, upsert, or delete documents
- **Easy integration** with Claude Desktop and other MCP clients
- **Secure connection** support with username/password authentication
- **Docker support** - Pre-built image available on Docker Hub

## Prerequisites

- **Go 1.23 or higher** - [Download Go](https://golang.org/dl/) (for building from source)
- **RethinkDB** - Running instance (local or remote)
  - [RethinkDB Installation Guide](https://rethinkdb.com/docs/install/)
  - Default connection: `localhost:28015`
- **MCP Client** (optional for testing):
  - Claude Desktop app, or
  - MCP Inspector: `npx @modelcontextprotocol/inspector`

## Installation

### Option 1: Using Docker (Recommended)

```bash
docker pull finn13/mcp-rethinkdb-server

# Run with default settings (connects to localhost:28015)
docker run -e RETHINKDB_HOST=host.docker.internal finn13/mcp-rethinkdb-server

# Run with custom settings
docker run \
  -e RETHINKDB_HOST=your-host \
  -e RETHINKDB_PORT=28015 \
  -e RETHINKDB_USER=admin \
  -e RETHINKDB_PASSWORD=secret \
  finn13/mcp-rethinkdb-server
```

### Option 2: Build from Source

```bash
# Clone the repository
git clone https://github.com/finn13/mcp-rethinkdb-server.git
cd mcp-rethinkdb-server

# Install dependencies
go mod tidy

# Build the binary
go build -o mcp-rethinkdb-server .
```

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `RETHINKDB_HOST` | `localhost` | RethinkDB host |
| `RETHINKDB_PORT` | `28015` | RethinkDB port |
| `RETHINKDB_USER` | (none) | Optional username |
| `RETHINKDB_PASSWORD` | (none) | Optional password |

## Usage

### Claude Desktop Configuration

Add to your Claude Desktop config:

**macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
**Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
**Linux**: `~/.config/Claude/claude_desktop_config.json`

#### Using Docker Image (Recommended):
```json
{
  "mcpServers": {
    "rethinkdb": {
      "command": "docker",
      "args": [
        "run",
        "-i",
        "--rm",
        "-e",
        "RETHINKDB_HOST=host.docker.internal",
        "finn13/mcp-rethinkdb-server"
      ]
    }
  }
}
```

#### Using Local Binary:
```json
{
  "mcpServers": {
    "rethinkdb": {
      "command": "/absolute/path/to/mcp-rethinkdb-server",
      "env": {
        "RETHINKDB_HOST": "localhost",
        "RETHINKDB_PORT": "28015"
      }
    }
  }
}
```

**Important**: After updating the config, restart Claude Desktop completely.

### Run Directly

```bash
# Default connection (localhost:28015)
./mcp-rethinkdb-server

# Custom host/port
RETHINKDB_HOST=myhost RETHINKDB_PORT=28015 ./mcp-rethinkdb-server

# With authentication
RETHINKDB_HOST=myhost RETHINKDB_USER=admin RETHINKDB_PASSWORD=secret ./mcp-rethinkdb-server
```

## Tool Examples

### list_databases

Lists all databases in RethinkDB.

```json
{
  "name": "list_databases"
}
```

Response:
```json
{
  "databases": ["test", "production", "analytics"]
}
```

### list_tables

Lists all tables in a specific database.

```json
{
  "name": "list_tables",
  "arguments": {
    "database": "test"
  }
}
```

Response:
```json
{
  "database": "test",
  "tables": ["users", "orders", "products"]
}
```

### query_table

Query data from a table with optional filtering, ordering, and limits.

```json
{
  "name": "query_table",
  "arguments": {
    "database": "test",
    "table": "users",
    "filter": {"status": "active"},
    "limit": 50,
    "order_by": "created_at"
  }
}
```

Response:
```json
{
  "database": "test",
  "table": "users",
  "count": 50,
  "results": [...]
}
```

**Parameters:**
- `database` (required): Database name
- `table` (required): Table name
- `filter` (optional): Filter object for matching documents
- `limit` (optional): Max results (default: 100, max: 1000)
- `order_by` (optional): Field to sort by

### table_info

Get table metadata including primary key, indexes, and document count.

```json
{
  "name": "table_info",
  "arguments": {
    "database": "test",
    "table": "users"
  }
}
```

Response:
```json
{
  "database": "test",
  "table": "users",
  "primary_key": "id",
  "indexes": ["email", "created_at"],
  "doc_count": 15420
}
```

### write_data

Write data to a table. Supports `insert`, `update`, `upsert`, and `delete` operations. Data can be a single document or an array of documents.

```json
{
  "name": "write_data",
  "arguments": {
    "database": "test",
    "table": "users",
    "operation": "insert",
    "data": {"id": "abc123", "name": "Alice", "status": "active"}
  }
}
```

Response:
```json
{
  "database": "test",
  "table": "users",
  "operation": "insert",
  "inserted": 1,
  "replaced": 0,
  "unchanged": 0,
  "deleted": 0,
  "errors": 0
}
```

**Parameters:**
- `database` (required): Database name
- `table` (required): Table name
- `data` (required): A single document or array of documents. For `delete`, a document with only `id` deletes by primary key; any other fields are used as a filter to match multiple documents.
- `operation` (optional): One of `insert` (default), `update`, `upsert`, `delete`

**Operations:**
| Operation | Behaviour |
|-----------|-----------|
| `insert` | Insert document(s); errors on duplicate key |
| `update` | Insert with conflict strategy `update` — merges fields into existing documents |
| `upsert` | Insert with conflict strategy `replace` — creates if missing, fully replaces if exists |
| `delete` | Delete by primary key (when data has only `id`) or by filter (any other fields) |

## Development

### Project Structure

```
mcp-rethinkdb-server/
├── main.go                 # Entry point: config, connection, MCP wiring
├── server/
│   ├── server.go           # RethinkDBServer struct with all tool handlers
│   └── server_test.go      # Integration tests (TDD)
├── docker-compose.yml      # Test RethinkDB instance (port 28016)
└── Dockerfile
```

### Running Tests

Tests require a RethinkDB instance. Start one with Docker Compose:

```bash
docker compose up -d
go test ./server/ -v
```

The test suite uses port `28016` by default to avoid conflicts with other running RethinkDB instances. Override with environment variables:

```bash
RETHINKDB_TEST_HOST=localhost RETHINKDB_TEST_PORT=28016 go test ./server/ -v
```

Tests automatically create and tear down a `mcp_test_db` database.

### Test with MCP Inspector

```bash
go build -o mcp-rethinkdb-server .
npx @modelcontextprotocol/inspector ./mcp-rethinkdb-server
```

Open your browser to the URL shown (usually http://localhost:5173) to interact with the server.

## Roadmap

### Advanced Query Support
- [x] **Joins**: Support for `eqJoin` operations via the `advanced_query` tool
- [x] **Aggregations**: `group`, `ungroup`, `count`, `sum`, `avg`, `min`, `max` via the `aggregate` tool
- [x] **Map/Reduce**: Support for `map` (field plucking) via the `advanced_query` tool
- [x] **Advanced Filtering**: Support for `between`, `contains` via the `advanced_query` tool
- [ ] **Geospatial Queries**: Support for `getIntersecting` and geospatial indexes

### Additional Tools
- [x] **Write Data**: Insert, update, upsert, and delete documents via the `write_data` tool
- [x] **Schema Inspector**: Explore table schemas via the `schema_inspector` tool (samples documents, infers field types, reports primary key and indexes)
- [x] **Index Management**: View index details via the `index_info` tool (ready status, multi, geo, outdated flags)
- [ ] **Query Builder**: Interactive query construction with validation
- [ ] **Changefeeds**: Real-time data monitoring (read-only subscriptions)

### Performance & Features
- [ ] **Query Caching**: Cache frequently accessed queries
- [ ] **Parallel Queries**: Support multiple simultaneous queries
- [ ] **Result Streaming**: Stream large result sets efficiently
- [x] **Query Statistics**: Execution time returned in `query_table` responses (`execution_time_ms`)

### Developer Experience
- [ ] **Interactive Examples**: More comprehensive example queries
- [ ] **Query Validation**: Better error messages and query syntax validation
- [ ] **Connection Pooling**: Improved connection management
- [ ] **TLS/SSL Support**: Secure connections to RethinkDB

## Troubleshooting

### Connection Issues

**Problem**: Server can't connect to RethinkDB
**Solution**:
- Verify RethinkDB is running: `rethinkdb --version`
- Check the host and port settings
- Test connection manually

### Claude Desktop Integration

**Problem**: Server not showing up in Claude
**Solution**:
- Verify the path/docker command in config is correct
- Restart Claude Desktop completely
- Check Claude logs: `~/Library/Logs/Claude/` (macOS)

**Problem**: Permission denied when executing binary
**Solution**:
```bash
chmod +x /path/to/mcp-rethinkdb-server
```

### Query Limitations

**Problem**: Query returns fewer results than expected
**Solution**:
- Default limit is 100 documents
- Maximum limit is 1000 documents
- Use the `limit` parameter to adjust

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make your changes with tests: `go test ./...`
4. Commit: `git commit -am 'Add feature'`
5. Push: `git push origin feature-name`
6. Create a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Links

- [Model Context Protocol Documentation](https://modelcontextprotocol.io/)
- [RethinkDB Documentation](https://rethinkdb.com/docs/)
- [Docker Hub Image](https://hub.docker.com/r/finn13/mcp-rethinkdb-server)
- [Report Issues](https://github.com/finnng/mcp-rethinkdb-server/issues)
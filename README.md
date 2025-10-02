# tap-redash

A [Singer](https://singer.io) tap for extracting data from [Redash](https://redash.io).

## Installation

```bash
pip install singer-python requests
```

## Quick Start

Create `config.json`:
```json
{
  "BASE_URL": "https://redash.example.com",
  "API_KEY": "your-api-key-here"
}
```

Discover and sync:
```bash
# Discover all queries
python tap_redash.py --config config.json --discover > catalog.json

# Sync to target
python tap_redash.py --config config.json --catalog catalog.json | target-jsonl
```

## Configuration

### Required
- `BASE_URL` - Your Redash instance URL
- `API_KEY` - Your Redash API key

### Optional
- `QUERY_ID` - Sync only a specific query (omit to sync all queries)
- `key_properties` - Array of primary key column names

## Usage

### Sync all queries
```bash
python tap_redash.py --config config.json --catalog catalog.json
```

### Sync specific query
Add `"QUERY_ID": "123"` to your config, then run without catalog:
```bash
python tap_redash.py --config config.json
```

### Stream selection
Edit `catalog.json` and set `"selected": false` for streams you want to skip.

## Features

- Automatically discovers all queries in your Redash instance
- Infers schemas from query results
- Supports stream selection via Singer catalog
- Handles individual query failures gracefully
- Sanitizes query names into valid stream identifiers

## Authentication

Get your API key from your Redash user settings. The key needs permission to list queries and view results.

## Limitations

- Full table replication only (no incremental sync)
- No support for parameterized queries

## Resources

- [Singer Specification](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md)
- [Redash API Documentation](https://redash.io/help/user-guide/integrations-and-api)

# MCP Server for Snowflake

This is a MCP server implementation for interacting with Snowflake databases.

## Installation

1. Clone this repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file with your Snowflake credentials:
```bash
SNOWFLAKE_USER=your_username      # Your username
SNOWFLAKE_PASSWORD=your_password  # Your password
SNOWFLAKE_ACCOUNT=your_account    # Your account
SNOWFLAKE_DATABASE=your_database  # Your database
SNOWFLAKE_WAREHOUSE=your_warehouse # Your warehouse
```

Add MCP client configuration:
```json
{
  "servers": {
    "snowflake": {
      "command": ["python", "-m", "mcp_snowflake_service"]
    }
  }
}
```

## Usage

Start the server:
```bash
python -m mcp_snowflake_service
```

The server will start listening for MCP client connections.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
import os
import asyncio
import logging
import json
import time
from typing import Optional, Any
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector
from mcp.server import Server
from mcp.server import stdio
from mcp.types import Tool, ServerResult, TextContent
from contextlib import closing

# Load environment variables from .env file
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Disable debug logging from snowflake connector
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)
logging.getLogger('snowflake.connector.telemetry').setLevel(logging.WARNING)
logging.getLogger('snowflake.connector.network').setLevel(logging.WARNING)
logging.getLogger('snowflake.connector.auth').setLevel(logging.WARNING)
logging.getLogger('snowflake.connector.vendored').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

logger = logging.getLogger('snowflake_server')

class SnowflakeConnection:
    """
    Snowflake database connection management class
    """
    def __init__(self):
        # Initialize configuration
        self.config: dict[str, str] = {}
        
        # Required environment variables
        env_vars = {
            "user": "SNOWFLAKE_USER",
            "password": "SNOWFLAKE_PASSWORD",
            "account": "SNOWFLAKE_ACCOUNT",
            "database": "SNOWFLAKE_DATABASE",
            "schema": "SNOWFLAKE_SCHEMA",
            "warehouse": "SNOWFLAKE_WAREHOUSE",
            "role": "SNOWFLAKE_ROLE"
        }
        
        # Load all environment variables
        for config_key, env_var in env_vars.items():
            value = os.getenv(env_var)
            if not value:
                raise ValueError(f"Missing required environment variable: {env_var}")
            self.config[config_key] = value
            
        self.conn: Optional[snowflake.connector.SnowflakeConnection] = None
        
        # Check available warehouses
        self.list_available_warehouses()

    def list_available_warehouses(self):
        """
        List all warehouses available to the current user
        """
        try:
            # Use main connection with proper warehouse and role
            conn = self.ensure_connection()
            if not conn:
                raise Exception("Failed to establish connection")
                
            cursor = conn.cursor()
            cursor.execute("SHOW WAREHOUSES")
            warehouses = cursor.fetchall()
            logger.info("Available warehouses:")
            for warehouse in warehouses:
                logger.info(f"Name: {warehouse[0]}, Size: {warehouse[1]}, State: {warehouse[3]}")
            cursor.close()
        except Exception as e:
            logger.error(f"Error listing warehouses: {str(e)}")
            raise

    def ensure_connection(self) -> snowflake.connector.SnowflakeConnection:
        """
        Ensure database connection is available, create new connection if it doesn't exist or is disconnected
        """
        try:
            if not self.conn:
                logger.info("Creating new connection...")
                
                # First create connection without warehouse
                self.conn = snowflake.connector.connect(
                    user=self.config["user"],
                    password=self.config["password"],
                    account=self.config["account"],
                    role=self.config["role"],
                    network_timeout=15,
                    login_timeout=15
                )
                
                # Set up session in a transaction to ensure atomicity
                with self.conn.cursor() as cursor:
                    cursor.execute("BEGIN")
                    try:
                        # First check role
                        cursor.execute("SELECT CURRENT_ROLE()")
                        result = cursor.fetchone()
                        current_role = result[0] if result else "Unknown"
                        logger.info(f"Connected with role: {current_role}")
                        
                        # Check warehouse grants
                        cursor.execute(f"""
                            SELECT privilege, granted_on, name 
                            FROM information_schema.object_privileges 
                            WHERE privilege = 'USAGE' 
                            AND granted_on = 'WAREHOUSE'
                            AND grantee_name = '{current_role}'
                        """)
                        warehouse_grants = cursor.fetchall()
                        logger.info(f"Warehouse grants for role {current_role}: {warehouse_grants}")
                        
                        cursor.execute(f"USE ROLE {self.config['role']}")
                        cursor.execute(f"USE WAREHOUSE {self.config['warehouse']}")
                        cursor.execute(f"USE DATABASE {self.config['database']}")
                        cursor.execute(f"USE SCHEMA {self.config['schema']}")
                        cursor.execute("COMMIT")
                    except Exception as e:
                        cursor.execute("ROLLBACK")
                        raise Exception(f"Failed to set up session context: {str(e)}")
                    
                    # Verify connection
                    cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE(), CURRENT_ROLE()")
                    context = cursor.fetchone()
                    if context:
                        logger.info(f"Connected successfully - Database: {context[0]}, Schema: {context[1]}, Warehouse: {context[2]}, Role: {context[3]}")
            
            return self.conn
            
        except Exception as e:
            logger.error(f"Connection error: {str(e)}")
            if self.conn:
                self.conn.close()
                self.conn = None
            raise

    def execute_query(self, query: str) -> list[dict[str, Any]]:
        """
        Execute SQL query and return results
        
        Args:
            query (str): SQL query statement
            
        Returns:
            list[dict[str, Any]]: List of query results
        """
        start_time = time.time()
        logger.info(f"Executing query: {query[:200]}...")  #  Log only first 200 characters
        
        try:
            conn = self.ensure_connection()
            with conn.cursor() as cursor:
                # Use transaction for write operations
                if any(query.strip().upper().startswith(word) for word in ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER']):
                    cursor.execute("BEGIN")
                    try:
                        cursor.execute(query)
                        conn.commit()
                        logger.info(f"Write query executed in {time.time() - start_time:.2f}s")
                        return [{"affected_rows": cursor.rowcount}]
                    except Exception as e:
                        conn.rollback()
                        raise
                else:
                    # Read operations
                    cursor.execute(query)
                    if cursor.description:
                        columns = [col[0] for col in cursor.description]
                        rows = cursor.fetchall()
                        results = [dict(zip(columns, row)) for row in rows]
                        logger.info(f"Read query returned {len(results)} rows in {time.time() - start_time:.2f}s")
                        return results
                    return []
                
        except snowflake.connector.errors.ProgrammingError as e:
            logger.error(f"SQL Error: {str(e)}")
            logger.error(f"Error Code: {getattr(e, 'errno', 'unknown')}")
            raise
        except Exception as e:
            logger.error(f"Query error: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            raise

    def close(self):
        """
        Close database connection
        """
        if self.conn:
            try:
                self.conn.close()
                logger.info("Connection closed")
            except Exception as e:
                logger.error(f"Error closing connection: {str(e)}")
            finally:
                self.conn = None

class SnowflakeServer(Server):
    """
    Snowflake MCP server class, handles client interactions
    """
    def __init__(self):
        super().__init__(name="snowflake-server")
        self.db = SnowflakeConnection()
        logger.info("SnowflakeServer initialized")

        @self.list_tools()
        async def handle_tools():
            """
            Return list of available tools
            """
            return [
                Tool(
                    name="execute_query",
                    description="Execute a SQL query on Snowflake",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "SQL query to execute"
                            }
                        },
                        "required": ["query"]
                    }
                )
            ]

        @self.call_tool()
        async def handle_call_tool(name: str, arguments: dict) -> list[TextContent]:
            """
            Handle tool call requests
            
            Args:
                name (str): Tool name
                arguments (dict): Tool arguments
                
            Returns:
                list[TextContent]: Execution results
            """
            if name == "execute_query":
                start_time = time.time()
                try:
                    result = self.db.execute_query(arguments["query"])
                    execution_time = time.time() - start_time
                    
                    return [TextContent(
                        type="text",
                        text=f"Results (execution time: {execution_time:.2f}s):\n{result}"
                    )]
                except Exception as e:
                    error_message = f"Error executing query: {str(e)}"
                    logger.error(error_message)
                    return [TextContent(
                        type="text",
                        text=error_message
                    )]
            return [TextContent(
                type="text",
                text=f"Unknown tool: {name}"
            )]

    def __del__(self):
        """
        Clean up resources, close database connection
        """
        if hasattr(self, 'db'):
            self.db.close()

async def main():
    """
    Main function, starts server and handles requests
    """
    try:
        server = SnowflakeServer()
        initialization_options = server.create_initialization_options()
        logger.info("Starting server")
        
        async with stdio.stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                initialization_options
            )
    except Exception as e:
        logger.critical(f"Server failed: {str(e)}", exc_info=True)
        raise
    finally:
        logger.info("Server shutting down")

if __name__ == "__main__":
    asyncio.run(main())
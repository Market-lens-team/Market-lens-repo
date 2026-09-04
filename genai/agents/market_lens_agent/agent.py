import os

from dotenv import load_dotenv
from google.oauth2 import service_account

from google.adk.agents.llm_agent import Agent

from .prompts.semantic_layer import MARKETLENS_SEMANTIC_LAYER

from google.adk.integrations.bigquery import (
    BigQueryCredentialsConfig,
    BigQueryToolset,
)

from google.adk.integrations.bigquery.config import (
    BigQueryToolConfig,
    WriteMode,
)


# Load service account environment variables
load_dotenv(
    "C:/Market_Lens/Market-lens-repo/genai/agents/market_lens_agent/.env"
)


# Authenticate using Service Account
credentials = service_account.Credentials.from_service_account_file(
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
)


# BigQuery credentials configuration
credentials_config = BigQueryCredentialsConfig(
    credentials=credentials
)


# Read-only BigQuery access
tool_config = BigQueryToolConfig(
    write_mode=WriteMode.BLOCKED
)


# BigQuery Tool
bigquery_toolset = BigQueryToolset(
    credentials_config=credentials_config,
    bigquery_tool_config=tool_config,
)


# MarketLens Agent
root_agent = Agent(
    model="gemini-3.1-flash-lite",
    name="market_lens_agent",
    description=(
        "MarketLens financial data analysis agent. "
        "Always retrieves financial answers from BigQuery Gold layer."
    ),
    instruction=MARKETLENS_SEMANTIC_LAYER,
    tools=[bigquery_toolset],
)
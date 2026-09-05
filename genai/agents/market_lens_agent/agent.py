import os

from google.auth import default

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


# -----------------------------------------
# Google Cloud configuration
# -----------------------------------------

os.environ["GOOGLE_CLOUD_PROJECT"] = "market-lens-506611"
os.environ["GOOGLE_CLOUD_LOCATION"] = "global"
os.environ["GOOGLE_GENAI_USE_VERTEXAI"] = "TRUE"


# -----------------------------------------
# Authentication
#
# Local:
#   uses gcloud application default credentials
#
# Cloud Run:
#   uses attached service account:
#   marketlens-genai-sa@
#
# -----------------------------------------

credentials, project = default()


# -----------------------------------------
# BigQuery credentials configuration
# -----------------------------------------

credentials_config = BigQueryCredentialsConfig(
    credentials=credentials
)


# -----------------------------------------
# Read-only BigQuery access
# -----------------------------------------

tool_config = BigQueryToolConfig(
    write_mode=WriteMode.BLOCKED
)


# -----------------------------------------
# BigQuery Tool
# -----------------------------------------

bigquery_toolset = BigQueryToolset(
    credentials_config=credentials_config,
    bigquery_tool_config=tool_config,
)


# -----------------------------------------
# MarketLens Agent
# -----------------------------------------

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
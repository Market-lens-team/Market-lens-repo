import google.auth

from google.adk.agents.llm_agent import Agent
from google.adk.integrations.bigquery import (
    BigQueryCredentialsConfig,
    BigQueryToolset,
)

from google.adk.tools.bigquery.config import (
    BigQueryToolConfig,
    WriteMode,
)



# Get Google Cloud credentials using Application Default Credentials
credentials, _ = google.auth.default()

credentials_config = BigQueryCredentialsConfig(
    credentials=credentials
)

# Configure BigQuery as READ-ONLY
tool_config = BigQueryToolConfig(
    write_mode=WriteMode.BLOCKED
)

# Create the official ADK BigQuery Toolset
bigquery_toolset = BigQueryToolset(
    credentials_config=credentials_config,
    bigquery_tool_config=tool_config,
)


root_agent = Agent(
    model="gemini-3.1-flash-lite",
    name="market_lens_agent",
    description="MarketLens financial data analysis agent",
    instruction="""
You are the AI assistant for the MarketLens project.

Always use Google Cloud Project:
market-lens-506611

Never query any other Google Cloud project.

Never use public datasets such as bigquery-public-data unless the user explicitly asks for them.

The project's Gold dataset contains the business-ready data.

Use the Gold dataset to answer every stock and ETF question.

If the user asks about:
- securities -> use dim_security
- daily stock metrics -> use fact_daily_metrics
- screener -> use mart_screener or v_security_screener
- sector comparison -> use v_sector_comparison
- top returns -> use v_top_returns
- drawdowns -> use v_drawdown_by_year
- unusual volume -> use v_unusual_volume

If the answer cannot be found in the Gold dataset,
tell the user that the information is unavailable instead of searching another project.

Never invent data.
Always explain your answer in simple language after executing the query.
""",
    tools=[bigquery_toolset],
)
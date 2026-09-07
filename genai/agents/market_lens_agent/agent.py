# Importing Python's operating system module.
# We use this to set environment variables required for Google Cloud.
import os


# Importing Google's automatic authentication function.
# This helps the application find available Google Cloud credentials.
# It uses:
# - Local gcloud credentials during development
# - Service account credentials when deployed on Cloud Run
from google.auth import default


# Importing the Agent class from Google ADK.
# This class is used to create our AI agent by connecting:
# Gemini model + instructions + external tools
from google.adk.agents.llm_agent import Agent


# Importing the MarketLens semantic layer instructions.
# These instructions tell Gemini:
# - Which Gold layer tables/views are available
# - Which data source to use for different questions
# - How to answer financial queries
from .prompts.semantic_layer import MARKETLENS_SEMANTIC_LAYER


# Importing BigQuery related classes.
# These classes help our AI agent connect with BigQuery
# and retrieve data from our Gold layer.
from google.adk.integrations.bigquery import (
    BigQueryCredentialsConfig,
    BigQueryToolset,
)


# Importing BigQuery configuration classes.
# These control what actions the AI agent can perform in BigQuery.
from google.adk.integrations.bigquery.config import (
    BigQueryToolConfig,
    WriteMode,
)



# -----------------------------------------
# Google Cloud configuration
# -----------------------------------------


# Setting the Google Cloud project ID.
# This tells Google which cloud project contains our resources.
# Our BigQuery datasets and Vertex AI services are inside this project.
os.environ["GOOGLE_CLOUD_PROJECT"] = "market-lens-506611"


# Setting the Google Cloud location.
# This tells Google where the AI services should run.
os.environ["GOOGLE_CLOUD_LOCATION"] = "global"


# Telling the application to use Gemini through Vertex AI.
# Vertex AI provides enterprise-level access to Gemini.
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


# Getting Google Cloud credentials automatically.
#
# If running locally:
# It uses credentials from your gcloud login.
#
# If running on Cloud Run:
# It uses the attached service account credentials.
#
# This gives the application an identity to access Google services.
credentials, project = default()



# -----------------------------------------
# BigQuery credentials configuration
# -----------------------------------------


# Creating a BigQuery credential configuration.
#
# This tells the BigQuery tool:
# "Use these credentials when accessing BigQuery."
#
# The credentials decide:
# - Who is accessing BigQuery
# - What permissions they have
credentials_config = BigQueryCredentialsConfig(
    credentials=credentials
)



# -----------------------------------------
# Read-only BigQuery access
# -----------------------------------------


# Configuring BigQuery tool permissions.
#
# WriteMode.BLOCKED means:
# The AI agent can only read data.
#
# Allowed:
# SELECT queries
# Reading tables/views
#
# Not allowed:
# INSERT
# UPDATE
# DELETE
# Changing database records
#
# This keeps financial data safe.
tool_config = BigQueryToolConfig(
    write_mode=WriteMode.BLOCKED
)



# -----------------------------------------
# BigQuery Tool
# -----------------------------------------


# Creating the BigQuery tool for the AI agent.
#
# This creates a connection between:
#
# Gemini Agent
#       |
#       ↓
# BigQuery Tool
#       |
#       ↓
# Gold Layer Tables and Views
#
# Through this tool Gemini can retrieve real market data.
bigquery_toolset = BigQueryToolset(
    credentials_config=credentials_config,
    bigquery_tool_config=tool_config,
)



# -----------------------------------------
# MarketLens Agent
# -----------------------------------------


# Creating the main MarketLens AI Agent.
#
# This combines:
# 1. Gemini model for reasoning
# 2. Semantic layer for business instructions
# 3. BigQuery tool for accessing financial data
#
# This object becomes the actual AI assistant.
root_agent = Agent(


    # Selecting the Gemini model that will power the agent.
    #
    # This model understands user questions,
    # generates SQL when required,
    # and creates financial explanations.
    model="gemini-3.1-flash-lite",


    # Giving a unique name to our agent.
    #
    # Google ADK uses this name internally
    # to identify this agent.
    name="market_lens_agent",


    # Giving a short description of what this agent does.
    #
    # It tells that this agent:
    # - analyzes financial data
    # - uses BigQuery Gold layer as the source of truth
    description=(
        "MarketLens financial data analysis agent. "
        "Always retrieves financial answers from BigQuery Gold layer."
    ),


    # Providing the semantic layer instructions to Gemini.
    #
    # These instructions teach Gemini:
    # - available datasets
    # - available tables/views
    # - correct data source selection
    # - financial interpretation rules
    instruction=MARKETLENS_SEMANTIC_LAYER,


    # Giving the agent access to the BigQuery tool.
    #
    # Without this:
    # Gemini can understand questions but cannot access MarketLens data.
    #
    # With this:
    # Gemini can fetch real data from the Gold layer.
    tools=[bigquery_toolset],
)

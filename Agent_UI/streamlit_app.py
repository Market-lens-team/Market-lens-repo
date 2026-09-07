# Importing Streamlit library.
# Streamlit is used to create our web-based chat interface.
# It allows us to create buttons, chat boxes, layouts, and display responses.
import streamlit as st


# Importing Python's system module.
# We use this to modify Python paths so that our app can find the agent code.
import sys


# Importing Python's operating system module.
# We use this for environment variables and handling file paths.
import os


# Importing asyncio.
# Our AI agent works asynchronously, meaning it can handle waiting for responses
# without blocking the complete application.
import asyncio


# Importing UUID library.
# UUID helps us create unique IDs for every user conversation session.
import uuid



# -----------------------------------
# Vertex AI configuration
# -----------------------------------


# Setting the Google Cloud project ID.
# This tells Google which cloud project contains our AI services.
os.environ["GOOGLE_CLOUD_PROJECT"] = "market-lens-506611"


# Setting the Google Cloud location.
# This tells Google where the Vertex AI services should run.
os.environ["GOOGLE_CLOUD_LOCATION"] = "global"


# Telling Google that Gemini should be accessed through Vertex AI.
# Vertex AI provides secure enterprise access to Gemini models.
os.environ["GOOGLE_GENAI_USE_VERTEXAI"] = "TRUE"



# -----------------------------------
# Connect Streamlit to Agent package
# -----------------------------------


# Finding the root folder where our agent package exists.
#
# Current file:
# streamlit_app.py
#
# We move one folder back and enter:
# genai/agents
#
# This allows Streamlit to import our AI agent code.
PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../genai/agents")
)


# Adding the agent folder path into Python's search locations.
#
# After this Python can find:
#
# market_lens_agent.agent
#
# without getting import errors.
sys.path.append(PROJECT_ROOT)


# Importing our main AI agent created in agent.py.
#
# root_agent contains:
# - Gemini model
# - semantic layer instructions
# - BigQuery tool
#
# This connects our UI with the actual AI brain.
from market_lens_agent.agent import root_agent



# Importing Runner from Google ADK.
#
# Runner is responsible for executing the agent.
#
# Flow:
#
# User Question
#       ↓
# Runner
#       ↓
# Agent
#       ↓
# Gemini + BigQuery
#
from google.adk.runners import Runner



# Importing session management service.
#
# This stores conversation history and maintains context
# between multiple user questions.
#
# Example:
#
# User:
# "Tell me about Apple"
#
# User:
# "Compare it with Microsoft"
#
# Agent remembers Apple from previous question.
from google.adk.sessions import InMemorySessionService



# Importing Google GenAI data types.
#
# We use this to format user messages
# before sending them to the ADK agent.
from google.genai import types




# -----------------------------------
# Page config + styling
# -----------------------------------


# Configuring the Streamlit webpage.
#
# This controls:
# - browser title
# - icon
# - page width
# - sidebar behaviour
st.set_page_config(
    page_title="MarketLens AI",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)



# Adding custom CSS styling to the Streamlit application.
#
# This improves the appearance of the AI interface.
#
# unsafe_allow_html=True allows us to use HTML and CSS.
st.markdown(
    """
    <style>


        # Main application background color.
        .stApp {
            background-color: #0e1117;
        }


        # Styling for the main heading.
        .main-header {
            font-size: 4.2rem;
            font-weight: 700;
            color: #f0f2f6;
            margin-bottom: 0;
        }


        # Styling for the subtitle below the heading.
        .sub-header {
            color: #8b949e;
            font-size: 0.95rem;
            margin-top: 0.2rem;
            margin-bottom: 1.5rem;
        }


        # Styling for example question buttons/cards.
        .example-chip {
            background-color: #1c2128;
            border: 1px solid #30363d;
            border-radius: 8px;
            padding: 10px 14px;
            margin-bottom: 8px;
            font-size: 0.85rem;
            color: #c9d1d9;
        }


        # Making chat messages have rounded corners.
        div[data-testid="stChatMessage"] {
            border-radius: 12px;
        }


        # Styling for small metric badges.
        .metric-badge {
            display: inline-block;
            background-color: #1c2128;
            border: 1px solid #30363d;
            border-radius: 999px;
            padding: 4px 12px;
            font-size: 0.8rem;
            color: #8b949e;
            margin-right: 6px;
        }


    </style>
    """,
    unsafe_allow_html=True,
)

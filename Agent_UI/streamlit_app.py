import streamlit as st
import sys
import os
import asyncio
import uuid

# -----------------------------------
# Vertex AI configuration
# -----------------------------------

os.environ["GOOGLE_CLOUD_PROJECT"] = "market-lens-506611"
os.environ["GOOGLE_CLOUD_LOCATION"] = "global"
os.environ["GOOGLE_GENAI_USE_VERTEXAI"] = "TRUE"

# -----------------------------------
# Connect Streamlit to Agent package
# -----------------------------------

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../genai/agents")
)
sys.path.append(PROJECT_ROOT)

from market_lens_agent.agent import root_agent

from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types


# -----------------------------------
# Page config + styling
# -----------------------------------

st.set_page_config(
    page_title="MarketLens AI",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
        .stApp {
            background-color: #0e1117;
        }
        .main-header {
            font-size: 4.2rem;
            font-weight: 700;
            color: #f0f2f6;
            margin-bottom: 0;
        }
        .sub-header {
            color: #8b949e;
            font-size: 0.95rem;
            margin-top: 0.2rem;
            margin-bottom: 1.5rem;
        }
        .example-chip {
            background-color: #1c2128;
            border: 1px solid #30363d;
            border-radius: 8px;
            padding: 10px 14px;
            margin-bottom: 8px;
            font-size: 0.85rem;
            color: #c9d1d9;
        }
        div[data-testid="stChatMessage"] {
            border-radius: 12px;
        }
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


# -----------------------------------
# Example questions (mirrors the problem statement's target queries)
# -----------------------------------

EXAMPLE_QUESTIONS = [
    "Was TSLA's volume unusual last month?",
    "Compare MSFT, GOOGL, and AAPL over the last 3 years, normalized",
    "Which ETFs had the lowest drawdown in 2020?",
    "Show me the top 10 stocks by 1-year return",
    "Compare AAPL against its market category peers",
    "Which mid-cap stocks had unusual volume spikes last quarter?",
]


# -----------------------------------
# Session state setup
# -----------------------------------

if "runner" not in st.session_state:
    st.session_state.session_service = InMemorySessionService()
    st.session_state.runner = Runner(
        agent=root_agent,
        app_name="market_lens_agent",
        session_service=st.session_state.session_service,
    )

if "adk_user_id" not in st.session_state:
    st.session_state.adk_user_id = "streamlit_user"

if "adk_session_id" not in st.session_state:
    st.session_state.adk_session_id = str(uuid.uuid4())
    st.session_state.adk_session_created = False

if "messages" not in st.session_state:
    st.session_state.messages = []

if "pending_question" not in st.session_state:
    st.session_state.pending_question = None


# -----------------------------------
# Agent call (async), reuses ONE session across the whole
# conversation so the agent retains context between questions -
# the original code created a brand-new session per question,
# which meant every question was answered with zero memory of
# anything asked before it.
# -----------------------------------

async def ensure_session():
    if not st.session_state.adk_session_created:
        await st.session_state.session_service.create_session(
            app_name="market_lens_agent",
            user_id=st.session_state.adk_user_id,
            session_id=st.session_state.adk_session_id,
        )
        st.session_state.adk_session_created = True


async def call_agent(question: str) -> str:
    await ensure_session()

    message = types.Content(
        role="user",
        parts=[types.Part(text=question)],
    )

    response_text = ""

    async for event in st.session_state.runner.run_async(
        user_id=st.session_state.adk_user_id,
        session_id=st.session_state.adk_session_id,
        new_message=message,
    ):
        if event.content and event.content.parts:
            for part in event.content.parts:
                if part.text:
                    response_text += part.text

    return response_text or "I couldn't generate a response for that. Try rephrasing your question."


# -----------------------------------
# Sidebar
# -----------------------------------

with st.sidebar:
    st.markdown("### 📊 MarketLens")
    st.caption("AI analyst over Bronze → Silver → Gold market data")

    st.markdown("---")
    st.markdown("**Try asking:**")

    for q in EXAMPLE_QUESTIONS:
        if st.button(q, key=f"example_{hash(q)}", use_container_width=True):
            st.session_state.pending_question = q

    st.markdown("---")

    if st.button("🗑️ Clear conversation", use_container_width=True):
        st.session_state.messages = []
        st.session_state.adk_session_id = str(uuid.uuid4())
        st.session_state.adk_session_created = False
        st.rerun()

    st.markdown("---")
    st.caption(
        "Data covers historical + incremental stock and ETF prices, "
        "returns, volume anomalies, and drawdown — sourced from the "
        "Gold layer (mart_screener, mart_unusual_volume, "
        "mart_normalized_prices, and related views)."
    )


# -----------------------------------
# Main header
# -----------------------------------

st.markdown('<p class="main-header">📈 MarketLens AI Analyst</p>', unsafe_allow_html=True)
st.markdown(
    '<p class="sub-header">Ask about stock &amp; ETF performance, returns, volume anomalies, '
    'drawdown, and comparisons — grounded in your Gold-layer data.</p>',
    unsafe_allow_html=True,
)


# -----------------------------------
# Chat history render
# -----------------------------------

for msg in st.session_state.messages:
    with st.chat_message(msg["role"], avatar="📈" if msg["role"] == "assistant" else None):
        st.markdown(msg["content"])


# -----------------------------------
# Chat input (bottom, sticky - native Streamlit chat UX)
# -----------------------------------

typed_question = st.chat_input("Ask your financial question...")

question_to_run = st.session_state.pending_question or typed_question
st.session_state.pending_question = None

if question_to_run:

    st.session_state.messages.append({"role": "user", "content": question_to_run})
    with st.chat_message("user"):
        st.markdown(question_to_run)

    with st.chat_message("assistant", avatar="📈"):
        with st.spinner("Analyzing market data..."):
            response = asyncio.run(call_agent(question_to_run))
        st.markdown(response)

    st.session_state.messages.append({"role": "assistant", "content": response})


# -----------------------------------
# Empty state
# -----------------------------------

if not st.session_state.messages:
    st.info(
        "👋 Start by asking a question above, or pick one of the example "
        "prompts in the sidebar."
    )
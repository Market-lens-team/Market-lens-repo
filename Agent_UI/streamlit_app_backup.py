import streamlit as st
import sys
import os
import asyncio
import uuid

# Vertex AI configuration
os.environ["GOOGLE_CLOUD_PROJECT"] = "market-lens-506611"
os.environ["GOOGLE_CLOUD_LOCATION"] = "global"
os.environ["GOOGLE_GENAI_USE_VERTEXAI"] = "TRUE"



# -----------------------------------
# Connect Streamlit to Agent package
# -----------------------------------

PROJECT_ROOT = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../genai/agents"
    )
)

sys.path.append(PROJECT_ROOT)


from market_lens_agent.agent import root_agent


# -----------------------------------
# ADK imports
# -----------------------------------

from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types


# -----------------------------------
# Streamlit Configuration
# -----------------------------------

st.set_page_config(
    page_title="MarketLens AI",
    page_icon="📈",
    layout="wide"
)


st.title("📈 MarketLens AI Analyst")


st.write(
    """
    Ask questions about stocks, sectors, returns,
    risk and market performance.
    """
)


# -----------------------------------
# Create ADK Runner
# -----------------------------------

if "runner" not in st.session_state:

    st.session_state.session_service = InMemorySessionService()

    st.session_state.runner = Runner(
        agent=root_agent,
        app_name="market_lens_agent",
        session_service=st.session_state.session_service
    )


# -----------------------------------
# User Input
# -----------------------------------

question = st.text_input(
    "Ask your financial question:"
)


# -----------------------------------
# Run Agent
# -----------------------------------

if st.button("Analyze"):

    if question:

        st.info("Processing your request...")


        async def call_agent():

            user_id = "streamlit_user"

            session_id = str(uuid.uuid4())


            # Create ADK session
            await st.session_state.session_service.create_session(
                app_name="market_lens_agent",
                user_id=user_id,
                session_id=session_id
            )


            # Convert question into ADK message
            message = types.Content(
                role="user",
                parts=[
                    types.Part(
                        text=question
                    )
                ]
            )


            response_text = ""


            # Run agent
            async for event in st.session_state.runner.run_async(
                user_id=user_id,
                session_id=session_id,
                new_message=message
            ):

                if event.content and event.content.parts:

                    for part in event.content.parts:

                        if part.text:
                            response_text += part.text


            return response_text



        response = asyncio.run(call_agent())


        st.success(response)


    else:

        st.warning(
            "Please enter a question"
        )
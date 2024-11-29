from datetime import datetime
import streamlit as st
import requests

# Constants
CHAT_API_URL = "http://0.0.0.0:8000/ask_the_bot"
HERD_API_URL = "http://0.0.0.0:8000/herd-access"

# Set page title
st.title("Chatbot")

# Initialize session state for messages
if "messages" not in st.session_state:
    st.session_state["messages"] = [
        {"role": "assistant", "content": "How can I help you?"}
    ]

# Display chat history
for msg in st.session_state["messages"]:
    st.chat_message(msg["role"]).write(msg["content"])

# User input
if prompt := st.chat_input():
    # Add user input to session state
    st.session_state["messages"].append({"role": "user", "content": prompt})
    st.chat_message("user").write(prompt)

    with st.spinner("Processing..."):
        # Fetch herd information
        try:
            herd_response = requests.get(HERD_API_URL)
            herd_response.raise_for_status()  # Raise error if status != 200
            herd_ids = herd_response.json().get("HerdIds", [])
        except requests.RequestException as e:
            st.error(f"Error fetching herd information: {e}")
            st.stop()
        except KeyError:
            st.error("Unexpected response format from herd-access API.")
            st.stop()

        # Log herd information
        st.write(f"Herd IDs: {herd_ids}")

        # Send question to chatbot API
        try:
            chat_response = requests.post(
                CHAT_API_URL,
                json={"question": prompt, "herds": herd_ids},
            )
            chat_response.raise_for_status()  # Raise error if status != 200
            assistant_reply = chat_response.json()
        except requests.RequestException as e:
            st.error(f"Error communicating with the chatbot API: {e}")
            st.stop()
        except ValueError:
            st.error("Invalid JSON response from chatbot API.")
            st.stop()

        # Log completion time
        st.write(f"Response received at {datetime.now()}")

        # Add assistant's reply to session state and display it
        st.session_state["messages"].append(
            {"role": "assistant", "content": assistant_reply}
        )
        st.chat_message("assistant").write(assistant_reply)

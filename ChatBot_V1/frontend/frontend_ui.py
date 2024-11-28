from datetime import datetime
import streamlit as st
import requests

st.title("Chatbot")

if "messages" not in st.session_state:
    st.session_state["messages"] = [
        {"role": "assistant", "content": "How can I help you?"}
    ]

for msg in st.session_state.messages:
    st.chat_message(msg["role"]).write(msg["content"])

if prompt := st.chat_input():

    st.session_state.messages.append({"role": "user", "content": prompt})
    st.chat_message("user").write(prompt)

    api_url_for_chat = "http://0.0.0.0:8000/ask_the_bot"
    api_url_for_herd = "http://0.0.0.0:8000/herd-access"

    with st.spinner("Processing..."):
        herds = requests.get(api_url_for_herd)
        if herds.status_code != 200:
            st.error("Bad token")
            st.stop()
        print(prompt)
        response = requests.post(
            api_url_for_chat,
            json={"question": prompt, "herds": herds.json()["HerdIds"]},
        )
        print(f"Finish with getting answer from llm{datetime.now()}")
        if response.status_code == 200:
            msg = response.json()
            st.session_state.messages.append(
                {"role": "assistant", "content": msg})
            st.chat_message("assistant").write(msg)
        else:
            st.error(
                "Failed to get response from the API, try again or try other question."
            )

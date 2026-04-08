import streamlit as st
import pandas as pd
import os


st.title("Dify Research Dashboard")

if os.path.exists("messages.csv") and os.path.exists("participants.csv"):
    messages = pd.read_csv("messages.csv")
    participants = pd.read_csv("participants.csv")
else:
    st.warning("CSV files not found. Run main.py first to generate them.")
    st.stop()

messages = pd.read_csv("messages.csv")
participants = pd.read_csv("participants.csv")

#search
search_id = st.text_input("Enter cr_connect_id")

if search_id:
    filtered = messages[messages["cr_connect_id"] == search_id]

    st.write(f"Showing {len(filtered)} messages")

    for _, row in filtered.iterrows():
        if row["role"] == "user":
            st.markdown(f"**User:** {row['content']}")
        else:
            st.markdown(f"**Assistant:** {row['content']}")
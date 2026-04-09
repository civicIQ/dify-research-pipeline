import os
import json
import requests
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

API_KEY = os.environ.get("DIFY_API_KEY")
creds_json = json.loads(os.environ.get("GOOGLE_CREDENTIALS_JSON"))

BASE_URL = "https://api.dify.ai/v1"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}


def get_chat_messages(limit=50):
    url = f"{BASE_URL}/chat-messages"
    params = {"limit": limit}
    all_messages = []

    while True:
        res = requests.get(url, headers=HEADERS, params=params)
        if res.status_code != 200:
            print(f"Error fetching chat messages: {res.status_code} {res.text}")
            break

        data = res.json()
        messages = data.get("data", [])
        all_messages.extend(messages)

        if not data.get("has_more"):
            break

        params["cursor"] = data.get("last_id")

    return all_messages


def run_pipeline():
    messages = get_chat_messages()
    print("TOTAL MESSAGES:", len(messages))

    messages_list = []
    participants_list = []

    for msg in messages:
        inputs = msg.get("inputs", {})
        outputs = msg.get("outputs", {})

        query = inputs.get("sys.query") or inputs.get("user_text")
        answer = outputs.get("answer") or outputs.get("text")
        user_id = inputs.get("sys.user_id") or msg.get("user")

        print("USER:", query)
        print("AI:", answer)

        if query and answer:
            messages_list.append({
                "user_id": user_id,
                "user_message": query,
                "ai_response": answer,
                "message_id": msg.get("id"),
                "created_at": msg.get("created_at")
            })

            participants_list.append({"user_id": user_id})

    messages_df = pd.DataFrame(messages_list)
    participants_df = pd.DataFrame(participants_list).drop_duplicates()

    return messages_df, participants_df


def upload_to_sheets(messages, participants):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)
    sheet = client.open("Dify Participant Data")

    try:
        sheet_messages = sheet.worksheet("messages")
        sheet_messages.clear()
    except gspread.WorksheetNotFound:
        sheet_messages = sheet.add_worksheet(title="messages", rows="1000", cols="20")

    sheet_messages.update([messages.columns.values.tolist()] + messages.values.tolist())

    try:
        sheet_participants = sheet.worksheet("participants")
        sheet_participants.clear()
    except gspread.WorksheetNotFound:
        sheet_participants = sheet.add_worksheet(title="participants", rows="1000", cols="20")

    sheet_participants.update([participants.columns.values.tolist()] + participants.values.tolist())


if __name__ == "__main__":
    messages_df, participants_df = run_pipeline()

    messages_df.to_csv("messages.csv", index=False)
    participants_df.to_csv("participants.csv", index=False)
    print(f"Saved {len(messages_df)} messages locally")

    upload_to_sheets(messages_df, participants_df)
    print("Uploaded data to Google Sheets")
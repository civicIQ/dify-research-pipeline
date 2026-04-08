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
headers = {
    "Authorization": f"Bearer {API_KEY}"
}

def get_conversations():
    url = f"{BASE_URL}/conversations"
    conversations = []
    params = {"limit": 100}

    while True:
        res = requests.get(url, headers=headers, params=params).json()
        conversations.extend(res.get("data", []))

        if not res.get("has_more"):
            break
        params["cursor"] = res.get("last_id")

    return conversations


def get_messages(conversation_id):
    url = f"{BASE_URL}/messages"
    params = {"conversation_id": conversation_id}
    res = requests.get(url, headers=headers, params=params).json()
    return res.get("data", [])

def run_pipeline():
    conversations = get_conversations()
    message_rows = []
    participant_rows = []

    for conv in conversations:
        conv_id = conv["id"]
        cr_id = conv.get("inputs", {}).get("cr_connect_id", "UNKNOWN")

        if cr_id == "UNKNOWN":
            continue

        messages = get_messages(conv_id)

        participant_rows.append({
            "cr_connect_id": cr_id,
            "conversation_id": conv_id,
            "first_seen": conv.get("created_at"),
            "message_count": len(messages)
        })

        for msg in messages:
            message_rows.append({
                "cr_connect_id": cr_id,
                "conversation_id": conv_id,
                "role": msg["role"],
                "content": msg["content"],
                "timestamp": msg["created_at"],
                "pulled_at": datetime.now().isoformat()
            })

    df_messages = pd.DataFrame(message_rows)
    df_participants = pd.DataFrame(participant_rows)

    df_messages = df_messages.drop_duplicates(
        subset=["conversation_id", "content", "timestamp"]
    )

    return df_messages, df_participants


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

    sheet_messages.update(
        [messages.columns.values.tolist()] + messages.values.tolist()
    )

    try:
        sheet_participants = sheet.worksheet("participants")
        sheet_participants.clear()
    except gspread.WorksheetNotFound:
        sheet_participants = sheet.add_worksheet(title="participants", rows="1000", cols="20")

    sheet_participants.update(
        [participants.columns.values.tolist()] + participants.values.tolist()
    )

if __name__ == "__main__":
    messages, participants = run_pipeline()

    messages.to_csv("messages.csv", index=False)
    participants.to_csv("participants.csv", index=False)
    print(f"Saved {len(messages)} messages locally")

    upload_to_sheets(messages, participants)
    print("Uploaded data to Google Sheets")
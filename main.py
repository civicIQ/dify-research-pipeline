import os
import json
import requests
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

API_KEY = os.environ.get("DIFY_API_KEY")
creds_json = json.loads(os.environ.get("GOOGLE_CREDENTIALS_JSON"))

BASE_URL = "https://api.dify.ai/v1"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}


def get_workflow_runs(limit=50):
    url = f"{BASE_URL}/workflows/runs"
    runs = []
    params = {"limit": limit}

    while True:
        try:
            res = requests.get(url, headers=HEADERS, params=params)
            res.raise_for_status()
            data = res.json()
        except Exception as e:
            print("Error fetching workflow runs:", e)
            break

        print("FULL RESPONSE:", data)  
        runs.extend(data.get("data", []))

        if not data.get("has_more"):
            break

        cursor = data.get("next_cursor") or data.get("last_id")
        if not cursor:
            break
        params["cursor"] = cursor

    return runs


def run_pipeline():
    runs = get_workflow_runs()
    print("TOTAL RUNS:", len(runs))

    messages_data = []
    participants_data = []

    for run in runs:
        inputs = run.get("inputs", {})
        outputs = run.get("outputs", {})

        query = inputs.get("sys.query")
        answer = outputs.get("answer")
        user_id = inputs.get("sys.user_id")
        workflow_id = run.get("workflow_id")
        run_created_at = run.get("created_at")

        if query and answer:
            messages_data.append({
                "user_id": user_id,
                "user_message": query,
                "ai_response": answer,
                "workflow_run_id": run.get("id"),
                "workflow_id": workflow_id,
                "run_created_at": run_created_at
            })
            participants_data.append({"user_id": user_id})
        else:
            print("Skipping run (missing query/answer):", run.get("id"))

        print("USER:", query)
        print("AI:", answer)

    messages_df = pd.DataFrame(messages_data)
    participants_df = pd.DataFrame(participants_data).drop_duplicates()

    return messages_df, participants_df


def upload_to_sheets(messages_df, participants_df, sheet_name="Dify Participant Data"):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)
    sheet = client.open(sheet_name)

    try:
        ws_messages = sheet.worksheet("messages")
        ws_messages.clear()
    except gspread.WorksheetNotFound:
        ws_messages = sheet.add_worksheet(title="messages", rows="1000", cols="20")

    set_with_dataframe(ws_messages, messages_df)

    try:
        ws_participants = sheet.worksheet("participants")
        ws_participants.clear()
    except gspread.WorksheetNotFound:
        ws_participants = sheet.add_worksheet(title="participants", rows="1000", cols="20")

    set_with_dataframe(ws_participants, participants_df)


if __name__ == "__main__":
    messages_df, participants_df = run_pipeline()

    messages_df.to_csv("messages.csv", index=False)
    participants_df.to_csv("participants.csv", index=False)
    print(f"Saved {len(messages_df)} messages locally")

    upload_to_sheets(messages_df, participants_df)
    print("Uploaded data to Google Sheets successfully")
import os
import json
import requests
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials


API_KEY = os.environ.get("DIFY_API_KEY")
creds_json = json.loads(os.environ.get("GOOGLE_CREDENTIALS_JSON"))

APP_ID = "cd6c78d7-57ed-46c7-aa71-4949ba48dd6b"

BASE_URL = "https://api.dify.ai/v1"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}



def get_workflow_runs():
    url = f"{BASE_URL}/apps/{APP_ID}/workflow-runs"
    params = {"limit": 100}

    all_runs = []

    while True:
        res = requests.get(url, headers=HEADERS, params=params)

        if res.status_code != 200:
            print("Error fetching workflow runs:", res.status_code, res.text)
            break

        data = res.json()
        runs = data.get("data", [])

        print(f"Fetched {len(runs)} runs")

        all_runs.extend(runs)

        if not data.get("has_more"):
            break

        params["cursor"] = data.get("last_id")

    print("TOTAL RUNS:", len(all_runs))
    return all_runs


def run_pipeline():
    runs = get_workflow_runs()

    messages_list = []
    participants_list = []

    for run in runs:
        inputs = run.get("inputs", {})
        outputs = run.get("outputs", {})

        cr_id = inputs.get("cr_connect_id")
        query = inputs.get("sys.query")
        answer = outputs.get("answer")

        print("CR_ID:", cr_id)
        print("QUERY:", query)
        print("ANSWER:", answer)
        print("------")

        if not cr_id:
            continue

        messages_list.append({
            "cr_connect_id": cr_id,
            "query": query,
            "response": answer,
            "workflow_run_id": run.get("id"),
            "timestamp": run.get("created_at"),
            "pulled_at": datetime.now().isoformat()
        })

        participants_list.append({
            "cr_connect_id": cr_id
        })

    messages_df = pd.DataFrame(messages_list)
    participants_df = pd.DataFrame(participants_list)

    if not messages_df.empty:
        messages_df = messages_df.drop_duplicates(subset=["workflow_run_id"])

    if not participants_df.empty:
        participants_df = participants_df.drop_duplicates()

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

    if not messages.empty:
        sheet_messages.update(
            [messages.columns.values.tolist()] + messages.values.tolist()
        )

    try:
        sheet_participants = sheet.worksheet("participants")
        sheet_participants.clear()
    except gspread.WorksheetNotFound:
        sheet_participants = sheet.add_worksheet(title="participants", rows="1000", cols="20")

    if not participants.empty:
        sheet_participants.update(
            [participants.columns.values.tolist()] + participants.values.tolist()
        )


if __name__ == "__main__":
    messages_df, participants_df = run_pipeline()

    messages_df.to_csv("messages.csv", index=False)
    participants_df.to_csv("participants.csv", index=False)

    print(f"Saved {len(messages_df)} messages locally")

    upload_to_sheets(messages_df, participants_df)

    print("Uploaded data to Google Sheets")
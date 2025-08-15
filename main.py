import os, json
from flask import Flask, request, jsonify
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def sheets_service():
    sa = json.loads(os.environ["GOOGLE_SA_JSON"])            # paste SA key JSON in env
    creds = Credentials.from_service_account_info(sa, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

SHEET_ID = os.environ["SHEET_ID"]                            # set in env
SHEET_NAME = os.environ.get("SHEET_NAME", "Matches")         # set in env (optional)
SHARED_SECRET = os.environ.get("SHARED_SECRET", "")          # set in env (optional)

app = Flask(__name__)

@app.get("/healthz")
def healthz():
    return "OK", 200

@app.post("/ingame")
def ingame():
    if SHARED_SECRET and request.headers.get("X-Secret") != SHARED_SECRET:
        return jsonify(ok=False, error="unauthorized"), 401

    ct = request.headers.get("Content-Type", "")
    raw = request.get_data(as_text=True)

    # Try JSON first
    data = request.get_json(silent=True)

    rows = None
    if isinstance(data, list):
        rows = data                                  # top-level array
    elif isinstance(data, dict):
        rows = data.get("rows")                      # {"rows":[...]}
    elif isinstance(raw, str):
        s = raw.strip()
        # Accept form-encoded rows=... or plain JSON string
        if "application/x-www-form-urlencoded" in ct and "rows=" in raw:
            try:
                rows = json.loads(request.form["rows"])
            except Exception:
                rows = None
        elif s.startswith("[") and s.endswith("]"):
            try:
                rows = json.loads(s)                 # stringified array
            except Exception:
                rows = None

    if not rows or not isinstance(rows, list) or not isinstance(rows[0], list):
        return jsonify(
            ok=False,
            error="expected JSON: {'rows': [[...],[...]]} or just [[...],[...]]",
            content_type=ct,
            body_preview=raw[:200]
        ), 400

    resp = sheets_service().spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_NAME}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows}
    ).execute()
    return jsonify(ok=True, updates=resp.get("updates", {})), 200

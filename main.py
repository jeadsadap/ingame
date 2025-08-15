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
SHEET_NAME = os.environ.get("SHEET_NAME", "")         # set in env (optional)
SHARED_SECRET = os.environ.get("SHARED_SECRET", "")          # set in env (optional)

app = Flask(__name__)

@app.get("/healthz")
def healthz():
    return "OK", 200

# main.py — replace only the /ingame route
import json, traceback
from flask import request, jsonify
from googleapiclient.errors import HttpError

@app.post("/ingame")
def ingame():
    try:
        if SHARED_SECRET and request.headers.get("X-Secret") != SHARED_SECRET:
            return jsonify(ok=False, error="unauthorized"), 401

        ct  = request.headers.get("Content-Type","")
        raw = request.get_data(as_text=True)

        data = request.get_json(silent=True)
        rows = None

        if isinstance(data, list):          # top-level array
            rows = data
        elif isinstance(data, dict):        # {"rows": ...}
            rows = data.get("rows")

        # If rows is a JSON string (common builder quirk), parse it
        if isinstance(rows, str):
            try:
                rows = json.loads(rows)
            except Exception:
                pass

        # Accept form-encoded rows=<json>
        if rows is None and "application/x-www-form-urlencoded" in ct and "rows" in request.form:
            try:
                rows = json.loads(request.form["rows"])
            except Exception:
                rows = None

        # Sanitize nulls
        if isinstance(rows, list):
            rows = [[("" if c is None else c) for c in (r if isinstance(r, list) else [r])] for r in rows]

        if not (isinstance(rows, list) and rows and isinstance(rows[0], list)):
            return jsonify(
                ok=False,
                error="expected {'rows': [[...],[...]]} or top-level [[...],[...]]",
                content_type=ct,
                kind=type(rows).__name__,
                body_preview=raw[:200]
            ), 400

        resp = sheets_service().spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=f"'{SHEET_NAME}'!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows}
        ).execute()
        return jsonify(ok=True, updates=resp.get("updates", {})), 200

    except HttpError as he:
        status = getattr(getattr(he, "resp", None), "status", 500)
        detail = he.content.decode() if getattr(he, "content", None) else str(he)
        return jsonify(ok=False, google_status=status, google_error=detail), status

    except Exception:
        print(traceback.format_exc())
        return jsonify(ok=False, error="server exception"), 500

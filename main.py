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

# main.py
import json, traceback
from flask import request, jsonify
from googleapiclient.errors import HttpError

@app.post("/ingame")
def ingame():
    try:
        if SHARED_SECRET and request.headers.get("X-Secret") != SHARED_SECRET:
            return jsonify(ok=False, error="unauthorized"), 401

        ct = request.headers.get("Content-Type","")
        raw = request.get_data(as_text=True)

        # Accept either {"rows":[...]} or top-level [...]
        data = request.get_json(silent=True)
        rows = None
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            rows = data.get("rows")

        # (Optional) accept form-encoded: rows=<json>
        if rows is None and "application/x-www-form-urlencoded" in ct and "rows" in request.form:
            try:
                rows = json.loads(request.form["rows"])
            except Exception:
                rows = None

        # Sanitize: replace None/null with ""
        if rows and isinstance(rows, list):
            rows = [[("" if c is None else c) for c in (r if isinstance(r, list) else [r])] for r in rows]

        if not rows or not isinstance(rows, list) or not isinstance(rows[0], list):
            return jsonify(ok=False,
                           error="expected JSON: {'rows': [[...],[...]]} or just [[...],[...]]",
                           content_type=ct, body_preview=raw[:200]), 400

        # Append
        svc = sheets_service()
        resp = svc.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows}
        ).execute()

        return jsonify(ok=True, updates=resp.get("updates", {})), 200

    except HttpError as he:
        # Surface Google error cleanly (403/404/400 etc.)
        status = getattr(he, "resp", None).status if getattr(he, "resp", None) else 500
        try:
            detail = json.loads(he.content.decode())
        except Exception:
            detail = {"raw": str(he)}
        return jsonify(ok=False, google_status=status, google_error=detail), status

    except KeyError as ke:
        # Missing env var like GOOGLE_SA_JSON or SHEET_ID
        return jsonify(ok=False, error=f"missing env var: {ke!s}"), 500

    except json.JSONDecodeError as je:
        return jsonify(ok=False, error="GOOGLE_SA_JSON is not valid JSON", detail=str(je)), 500

    except Exception:
        # Last-resort: print traceback to logs & return message
        print(traceback.format_exc())
        return jsonify(ok=False, error="server exception", trace=traceback.format_exc()[-500:]), 500


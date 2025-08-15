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

def to_rows_from_extractor_payload(data: dict) -> list[list]:
    """
    Accepts {"result": "...", "time_played": "...", "players":[{...},...]}
    and returns the 2-D rows array expected by Sheets.
    """
    result = (data or {}).get("result", "") or ""
    time_played = (data or {}).get("time_played", "") or ""
    players = (data or {}).get("players", []) or []

    def team_from_player(name: str) -> str:
        if not name: return ""
        # team abbrev = first token before a space, e.g., "FW NaiLiu" -> "FW"
        return (name.split(" ", 1)[0] or "").strip()

    rows = []
    for p in players:
        side  = p.get("side", "") or ""
        name  = p.get("player_name", "") or ""
        hero  = p.get("hero", "") or ""
        kills = p.get("kills", 0) or 0
        deaths= p.get("deaths", 0) or 0
        assists = p.get("assists", 0) or 0
        gold  = p.get("gold", 0) or 0
        mvp   = p.get("mvp_points", 0) or 0
        dealt = p.get("damage_dealt", 0) or 0
        recv  = p.get("damage_received", 0) or 0
        team  = team_from_player(name)

        rows.append([
            "", side, team, name, hero,
            kills, deaths, assists, gold, mvp, dealt, recv,
            time_played, result
        ])
    return rows


# main.py — replace only the /ingame route
import json, traceback
from flask import request, jsonify
from googleapiclient.errors import HttpError

@app.post("/ingame")
@app.post("/ingame")
def ingame():
    try:
        if SHARED_SECRET and request.headers.get("X-Secret") != SHARED_SECRET:
            return jsonify(ok=False, error="unauthorized"), 401

        ct  = request.headers.get("Content-Type","")
        raw = request.get_data(as_text=True)

        data = request.get_json(silent=True)
        rows = None

        # Case 1: already a 2-D array (top-level)
        if isinstance(data, list):
            rows = data

        # Case 2: {"rows":[...]} format
        elif isinstance(data, dict) and "rows" in data:
            rows = data.get("rows")

        # Case 3: Extractor payload {"result":...,"time_played":...,"players":[...]}
        elif isinstance(data, dict) and "players" in data:
            rows = to_rows_from_extractor_payload(data)

        # Case 4: form-encoded rows=<json>
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
                error="expected {'rows': [[...],[...]]} / top-level [[...]] / or Extractor payload with 'players'",
                content_type=ct,
                body_preview=raw[:200],
                kind=type(data).__name__
            ), 400

        # --- Append (quote the tab name) ---
        safe = SHEET_NAME.replace("'", "''")
        target_range = f"'{safe}'!A1"
        svc  = sheets_service()
        resp = svc.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=target_range,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows},
        ).execute()
        return jsonify(ok=True, updates=resp.get("updates", {})), 200

    # keep your HttpError/Exception handlers as we added earlier

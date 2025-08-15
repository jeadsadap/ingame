import os, json, traceback
from flask import Flask, request, jsonify
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError

# ---- Config via ENV ----
SCOPES        = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID      = os.environ["SHEET_ID"]                      # required
SHEET_NAME    = os.environ.get("SHEET_NAME", "Matches")     # optional
SHARED_SECRET = os.environ.get("SHARED_SECRET", "")         # optional
SA_JSON       = json.loads(os.environ["GOOGLE_SA_JSON"])    # required

# ---- Google client ----
def sheets_service():
    creds = Credentials.from_service_account_info(SA_JSON, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

# ---- Convert Extractor payload -> rows (2-D) ----
def to_rows_from_extractor_payload(data: dict) -> list[list]:
    """
    Accepts:
      {
        "result": "Blue" | "Red" | "Draw",
        "time_played": "MM:SS",
        "players": [
          {
            "side": "Blue"|"Red",
            "player_name": "FW NaiLiu",
            "hero": "Marja",
            "kills": 7, "deaths": 1, "assists": 0,
            "gold": 0, "mvp_points": 0,
            "damage_dealt": 140235, "damage_received": 108498
          }, ...
        ]
      }
    Returns 10 rows shaped for Sheets.
    """
    if not isinstance(data, dict):
        return []

    result = (data.get("result") or "")
    time_played = (data.get("time_played") or "")
    players = data.get("players") or []

    def team_from_player(name: str) -> str:
        if not name:
            return ""
        # team abbrev = first token before a space, e.g. "FW NaiLiu" -> "FW"
        return (name.split(" ", 1)[0] or "").strip()

    rows = []
    for p in players:
        side   = p.get("side") or ""
        name   = p.get("player_name") or ""
        hero   = p.get("hero") or ""
        kills  = p.get("kills") or 0
        deaths = p.get("deaths") or 0
        assists= p.get("assists") or 0
        gold   = p.get("gold") or 0
        mvp    = p.get("mvp_points") or 0
        dealt  = p.get("damage_dealt") or 0
        recv   = p.get("damage_received") or 0
        team   = team_from_player(name)

        rows.append([
            "",               # Game No. (let the sheet formula fill this)
            side, team, name, hero,
            kills, deaths, assists, gold, mvp, dealt, recv,
            time_played, result
        ])
    return rows

# ---- Flask app ----
app = Flask(__name__)

@app.get("/healthz")
def healthz():
    return "OK", 200

@app.post("/ingame")
def ingame():
    try:
        # Optional shared-secret header
        if SHARED_SECRET and request.headers.get("X-Secret") != SHARED_SECRET:
            return jsonify(ok=False, error="unauthorized"), 401

        ct  = request.headers.get("Content-Type", "")
        raw = request.get_data(as_text=True)

        # Accept JSON body only
        data = request.get_json(silent=True)
        rows = None

        # 1) Already a top-level array [[...]]
        if isinstance(data, list):
            rows = data

        # 2) {"rows":[...]}
        elif isinstance(data, dict) and "rows" in data:
            rows = data.get("rows")

        # 3) Extractor payload {"result","time_played","players":[...]}
        elif isinstance(data, dict) and "players" in data:
            rows = to_rows_from_extractor_payload(data)

        # If builder sent rows as a JSON string, parse it
        if isinstance(rows, str):
            try:
                rows = json.loads(rows)
            except Exception:
                rows = None

        # Sanitize nulls -> ""
        if isinstance(rows, list):
            rows = [[("" if c is None else c) for c in (r if isinstance(r, list) else [r])] for r in rows]

        if not (isinstance(rows, list) and rows and isinstance(rows[0], list)):
            return jsonify(
                ok=False,
                error="expected {'rows': [[...],[...]]} / top-level [[...]] / or Extractor payload with 'players'",
                content_type=ct,
                body_preview=raw[:200]
            ), 400

        # Append to Sheets (quote the tab name)
        safe_tab = SHEET_NAME.replace("'", "''")
        rng = f"'{safe_tab}'!A1"

        svc  = sheets_service()
        resp = svc.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=rng,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows}
        ).execute()

        return jsonify(ok=True, updates=resp.get("updates", {})), 200

    except KeyError as ke:
        return jsonify(ok=False, error=f"missing env var: {ke!s}"), 500

    except json.JSONDecodeError as je:
        return jsonify(ok=False, error="GOOGLE_SA_JSON is not valid JSON", detail=str(je)), 500

    except HttpError as he:
        status = getattr(getattr(he, "resp", None), "status", 500)
        try:
            detail = json.loads(he.content.decode())
        except Exception:
            detail = {"raw": str(he)}
        return jsonify(ok=False, google_status=status, google_error=detail), status

    except Exception:
        print(traceback.format_exc())
        return jsonify(ok=False, error="server exception"), 500

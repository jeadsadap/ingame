import os, json, base64, traceback
from flask import Flask, request, jsonify
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials

# ------------------ config ------------------
SCOPES        = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID      = os.environ["SHEET_ID"]                      # required
SHEET_NAME    = os.environ.get("SHEET_NAME", "Matches")     # tab name
SHARED_SECRET = os.environ.get("SHARED_SECRET", "")         # optional

def load_sa_json():
    """Load service-account JSON from env (plain/base64) or a secret file."""
    raw = os.environ.get("GOOGLE_SA_JSON", "").strip()
    if raw:
        return json.loads(raw)
    b64 = os.environ.get("GOOGLE_SA_JSON_B64", "").strip()
    if b64:
        return json.loads(base64.b64decode(b64).decode())
    path = os.environ.get("GOOGLE_SA_JSON_FILE", "").strip()
    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def sheets_service():
    sa = load_sa_json()
    if not sa:
        raise RuntimeError(
            "Service account JSON not found: set GOOGLE_SA_JSON or "
            "GOOGLE_SA_JSON_B64 or GOOGLE_SA_JSON_FILE"
        )
    creds = Credentials.from_service_account_info(sa, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

# --------- Extractor payload -> 2D rows ----------
def to_rows_from_extractor_payload(data: dict) -> list[list]:
    if not isinstance(data, dict):
        return []
    result = data.get("result") or ""
    time_played = data.get("time_played") or ""
    players = data.get("players") or []

    def team_from_player(name: str) -> str:
        return (name.split(" ", 1)[0] if name else "").strip()

    rows = []
    for p in players:
        side    = p.get("side") or ""
        name    = p.get("player_name") or ""
        hero    = p.get("hero") or ""
        kills   = p.get("kills") or 0
        deaths  = p.get("deaths") or 0
        assists = p.get("assists") or 0
        gold    = p.get("gold") or 0
        mvp     = p.get("mvp_points") or 0
        dealt   = p.get("damage_dealt") or 0
        recv    = p.get("damage_received") or 0
        team    = team_from_player(name)
        rows.append([
            "", side, team, name, hero,
            kills, deaths, assists, gold, mvp, dealt, recv,
            time_played, result
        ])
    return rows

# ------------------ app ------------------
app = Flask(__name__)

@app.get("/healthz")
def healthz():
    return "OK", 200

@app.post("/ingame")
def ingame():
    try:
        # optional shared-secret
        if SHARED_SECRET and request.headers.get("X-Secret") != SHARED_SECRET:
            return jsonify(ok=False, error="unauthorized"), 401

        ct  = request.headers.get("Content-Type", "")
        raw = request.get_data(as_text=True)

        def extract_rows(obj):
            if isinstance(obj, list):              # top-level [[...]]
                return obj
            if isinstance(obj, dict):
                if "rows" in obj:                  # {"rows":[...]}
                    return obj["rows"]
                if "players" in obj:               # Extractor payload
                    return to_rows_from_extractor_payload(obj)
            return None

        # 1) normal JSON
        data = request.get_json(silent=True)
        rows = extract_rows(data)

        # 2) parse raw body if needed (handles stringified/double-encoded JSON)
        if rows is None:
            s = (raw or "").strip()
            if s:
                # direct JSON
                try:
                    rows = extract_rows(json.loads(s))
                except Exception:
                    rows = None
                # double-encoded string: "\"{\\\"rows\\\":[...] }\""
                if rows is None and s.startswith('"') and s.endswith('"'):
                    try:
                        rows = extract_rows(json.loads(json.loads(s)))
                    except Exception:
                        rows = None

        # 3) form-encoded rows=<json>
        if rows is None and "application/x-www-form-urlencoded" in ct and "rows" in request.form:
            try:
                rows = json.loads(request.form["rows"])
            except Exception:
                rows = None

        # sanitize nulls -> ""
        if isinstance(rows, list):
            rows = [[("" if c is None else c) for c in (r if isinstance(r, list) else [r])] for r in rows]

        if not (isinstance(rows, list) and rows and isinstance(rows[0], list)):
            return jsonify(
                ok=False,
                error="expected {'rows': [[...],[...]]} / top-level [[...]] / Extractor payload (or stringified variants)",
                content_type=ct,
                body_preview=raw[:200],
                kind=type(data).__name__ if data is not None else "NoneType",
            ), 400

        # append to Sheets (quote tab name)
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

    except HttpError as he:
        status = getattr(getattr(he, "resp", None), "status", 500)
        try:
            detail = json.loads(he.content.decode())
        except Exception:
            detail = {"raw": str(he)}
        return jsonify(ok=False, google_status=status, google_error=detail), status

    except Exception as e:
        # last-resort error
        print(traceback.format_exc())
        return jsonify(ok=False, error="server exception", detail=str(e)), 500

"""
SOF Zoom → YouTube Pipeline
Receives Zoom recording.completed webhooks, downloads MP4, uploads to YouTube,
posts to Slack #tfc-recordings, and updates Notion Flow Code Call Archive.
"""

import os
import json
import hashlib
import hmac
import tempfile
import threading
import logging
import requests
from flask import Flask, request, jsonify

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

app = Flask(__name__)

# ── Env vars (loaded lazily so missing vars don't crash startup) ─────────────
def env(key, default=None):
    val = os.environ.get(key, default)
    if val is None:
        raise RuntimeError(f"Missing required env var: {key}")
    return val

PERSONAL_ROOM_TOPIC  = os.environ.get("PERSONAL_ROOM_TOPIC", "Jason Crouse's Personal Meeting Room")
MIN_DURATION_MINUTES = int(os.environ.get("MIN_DURATION_MINUTES", "90"))

# ── Zoom OAuth token ────────────────────────────────────────────────────────
def get_zoom_token():
    """Get a Server-to-Server OAuth token for Zoom API calls."""
    import base64
    account_id  = env("ZOOM_ACCOUNT_ID")
    client_id   = env("ZOOM_CLIENT_ID")
    client_secret = env("ZOOM_CLIENT_SECRET")
    credentials = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    r = requests.post(
        f"https://zoom.us/oauth/token?grant_type=account_credentials&account_id={account_id}",
        headers={"Authorization": f"Basic {credentials}"}
    )
    r.raise_for_status()
    return r.json()["access_token"]


# ── Zoom webhook verification ────────────────────────────────────────────────
def verify_zoom_signature(request):
    """Validate Zoom webhook signature."""
    timestamp = request.headers.get("x-zm-request-timestamp", "")
    signature = request.headers.get("x-zm-signature", "")
    body = request.get_data(as_text=True)
    message = f"v0:{timestamp}:{body}"
    expected = "v0=" + hmac.new(
        env("ZOOM_WEBHOOK_SECRET").encode(),
        message.encode(),
        hashlib.sha256
    ).hexdigest()

    return hmac.compare_digest(expected, signature)


# ── YouTube ──────────────────────────────────────────────────────────────────
def get_youtube_service():
    token_data = json.loads(env("YOUTUBE_TOKEN_JSON"))
    creds = Credentials.from_authorized_user_info(token_data)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build("youtube", "v3", credentials=creds)


def upload_to_youtube(file_path, title, description="", privacy="unlisted"):
    youtube = get_youtube_service()
    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": ["TFC", "Flow Code", "Group Coaching"],
            "categoryId": "27",
        },
        "status": {
            "privacyStatus": privacy,
            "selfDeclaredMadeForKids": False,
        },
    }
    media = MediaFileUpload(file_path, chunksize=-1, resumable=True)
    request_obj = youtube.videos().insert(
        part=",".join(body.keys()), body=body, media_body=media
    )
    response = None
    while response is None:
        _, response = request_obj.next_chunk()
    return f"https://www.youtube.com/watch?v={response['id']}"


# ── Timestamp / chapter helpers ─────────────────────────────────────────────
def parse_chapters(shorthand_bullet):
    """Parse Fireflies shorthand_bullet into YouTube-style chapter timestamps.

    Fireflies uses MM:SS for short calls and HH:MM for long calls (>1 hour).
    We detect which format by checking if the max parsed value makes sense:
    if treating as MM:SS yields < 10 minutes total for a multi-chapter call,
    it's almost certainly HH:MM.
    """
    import re
    if not shorthand_bullet:
        return []

    pattern = r'\*\*(.+?)\*\*\s*\((\d+:\d+(?::\d+)?)\s*-\s*\d+:\d+(?::\d+)?\)'
    raw = []

    for m in re.finditer(pattern, shorthand_bullet):
        title = m.group(1).strip()
        parts = m.group(2).split(":")
        if len(parts) == 2:
            # Could be MM:SS or HH:MM — resolve below
            raw.append((title, int(parts[0]), int(parts[1]), None))
        elif len(parts) == 3:
            total = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            raw.append((title, None, None, total))
        else:
            continue

    if not raw:
        return []

    # Detect HH:MM vs MM:SS for 2-part timestamps
    # If max MM:SS interpretation < 600 seconds (10 min) and we have 3+ chapters → HH:MM
    two_part = [(t, a, b) for t, a, b, fixed in raw if fixed is None]
    use_hhmm = False
    if two_part:
        max_mmss = max(a * 60 + b for _, a, b in two_part)
        if max_mmss < 600 and len(two_part) >= 3:
            use_hhmm = True

    chapters = []
    seen = set()
    for title, a, b, fixed in raw:
        if fixed is not None:
            total = fixed
        elif use_hhmm:
            total = a * 3600 + b * 60  # HH:MM → seconds
        else:
            total = a * 60 + b          # MM:SS → seconds
        if total in seen:
            continue
        seen.add(total)
        if total < 3600:
            yt = f"{total // 60}:{total % 60:02d}"
        else:
            h = total // 3600
            yt = f"{h}:{(total % 3600) // 60:02d}:{total % 60:02d}"
        chapters.append((yt, title, total))

    chapters.sort(key=lambda x: x[2])
    if chapters and chapters[0][2] > 0:
        chapters[0] = ("0:00", chapters[0][1], 0)
    return [(yt, title) for yt, title, _ in chapters]


def format_youtube_description(call_date_display, chapters):
    """Build a YouTube video description including chapter timestamps."""
    desc = f"TFC Flow Code — weekly group coaching call archived for community members.\n\nDate: {call_date_display}"
    if chapters:
        lines = "\n".join(f"{t} {title}" for t, title in chapters)
        desc += f"\n\nTIMESTAMPS\n{lines}"
    return desc


def update_youtube_description(youtube_url, description):
    """Update an existing YouTube video's description."""
    import re
    m = re.search(r"(?:v=|youtu\.be/)([A-Za-z0-9_-]+)", youtube_url)
    if not m:
        log.warning(f"Could not extract video ID from {youtube_url}")
        return
    video_id = m.group(1)
    try:
        youtube = get_youtube_service()
        current = youtube.videos().list(part="snippet", id=video_id).execute()
        items = current.get("items", [])
        if not items:
            log.warning(f"YouTube video {video_id} not found")
            return
        snippet = items[0]["snippet"]
        snippet["description"] = description
        youtube.videos().update(part="snippet", body={"id": video_id, "snippet": snippet}).execute()
        log.info(f"YouTube description updated for {video_id}")
    except Exception as e:
        log.warning(f"YouTube description update failed: {e}")


# ── GitHub wiki context ──────────────────────────────────────────────────────
GITHUB_RAW = "https://raw.githubusercontent.com/jcrouse26/ai-jason/main/wiki"
WIKI_CONTEXT_FILES = ["Framework-Definitions.md", "Jason-Voice-Patterns.md"]

_wiki_context_cache = None

def load_tfc_context():
    """Fetch TFC wiki context files from private GitHub repo. Cached per container lifetime."""
    global _wiki_context_cache
    if _wiki_context_cache:
        return _wiki_context_cache
    token = os.environ.get("GITHUB_TOKEN")
    headers_gh = {"Authorization": f"token {token}"} if token else {}
    chunks = []
    for filename in WIKI_CONTEXT_FILES:
        try:
            r = requests.get(f"{GITHUB_RAW}/{filename}", headers=headers_gh, timeout=15)
            if r.status_code == 200:
                content = r.text
                # Cap each file to avoid blowing context window
                if len(content) > 8000:
                    content = content[:8000] + "\n...[truncated]"
                chunks.append(f"=== {filename} ===\n{content}")
                log.info(f"Loaded wiki context: {filename} ({len(r.text):,} chars)")
            else:
                log.warning(f"Could not fetch {filename}: {r.status_code}")
        except Exception as e:
            log.warning(f"Wiki context fetch failed for {filename}: {e}")
    _wiki_context_cache = "\n\n".join(chunks)
    return _wiki_context_cache


# ── Claude summary generation ────────────────────────────────────────────────
def generate_tfc_summary(transcript_sentences, title, call_date, duration, tfc_context):
    """Generate a TFC-aware summary, keywords, and action items via Claude."""
    lines = [
        f"[{int(s.get('start_time', 0))}s] {s.get('speaker_name', '?')}: {s.get('text', '')}"
        for s in (transcript_sentences or [])
    ]
    transcript_text = "\n".join(lines)
    if len(transcript_text) > 80000:
        transcript_text = transcript_text[:80000] + "\n...[truncated]"

    prompt = f"""You work for Jason Crouse, founder of The Flow Code (TFC). Write an internal call summary for the Notion archive and Slack — used for coaching context and call prep.

WHAT THIS IS:
A brief handoff note from someone who was in the room. Not a corporate summary.
Something Jason could read and immediately remember the feel and substance of the call.

VOICE RULES:
- Do NOT use: explored, delved into, unpacked, covered, touched on, addressed, discussed, "the group reflected on," "participants shared," "insights were gained," "themes emerged"
- Short declarative sentences. Be specific. If a framework was named, use its exact TFC name.
- If Jason named something, say what it was. If a member had a breakthrough, name them.
- Write like you know what the Four Horsemen are. Write like you know what Flow Not Force means.
  Don't explain TFC concepts — just use the vocabulary.

TFC VOCABULARY REFERENCE:
{tfc_context}

---

CALL: {title}
DATE: {call_date}
DURATION: {duration:.0f} min

FULL TRANSCRIPT:
{transcript_text}

---

Produce exactly these two sections:

SUMMARY:
3-5 sentences. What actually happened — the arc, who showed up, what Jason did, the energy.
Name members by first name when they were central. No bullet points. Write it like a note.

KEYWORDS:
10-20 comma-separated tags. Include TFC framework names, tools, themes. No member names.
Example: Flow Not Force, Phase 3, 10-Minute Rule, Ideal Schedule, Four Horsemen, Accountability"""

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": env("ANTHROPIC_API_KEY"),
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 800,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=60,
        )
        r.raise_for_status()
        raw = r.json()["content"][0]["text"].strip()
    except Exception as e:
        log.warning(f"Claude summary generation failed: {e}")
        return None, []

    # Parse SUMMARY and KEYWORDS sections
    summary, keywords_raw = "", ""
    current = None
    for line in raw.splitlines():
        clean = line.strip().rstrip(":")
        if clean.upper() == "SUMMARY":
            current = "summary"
        elif clean.upper() == "KEYWORDS":
            current = "keywords"
        elif current == "summary":
            summary += (" " if summary else "") + line.strip()
        elif current == "keywords":
            keywords_raw += line.strip()

    keywords = [k.strip() for k in keywords_raw.split(",") if k.strip()]
    log.info(f"Claude summary generated ({len(keywords)} keywords)")
    return summary.strip() or None, keywords


# ── Fireflies ────────────────────────────────────────────────────────────────
def get_fireflies_summary(call_date):
    """Fetch transcript + generate Claude summary for the group coaching call on a given date."""
    query = """
    query($fromDate: String, $toDate: String) {
      transcripts(fromDate: $fromDate, toDate: $toDate) {
        id
        title
        duration
        sentences { speaker_name text start_time }
        summary { shorthand_bullet }
      }
    }
    """
    from datetime import datetime, timedelta
    dt = datetime.strptime(call_date, "%Y-%m-%d")
    from_date = (dt - timedelta(days=1)).strftime("%Y-%m-%d")
    to_date   = (dt + timedelta(days=1)).strftime("%Y-%m-%d")

    try:
        r = requests.post(
            "https://api.fireflies.ai/graphql",
            headers={"Authorization": f"Bearer {env('FIREFLIES_API_KEY')}",
                     "Content-Type": "application/json"},
            json={"query": query, "variables": {"fromDate": from_date, "toDate": to_date}},
            timeout=30
        )
    except Exception as e:
        log.warning(f"Fireflies request failed: {e}")
        return None, [], None, []
    if r.status_code != 200:
        log.warning(f"Fireflies API returned {r.status_code} — skipping summary")
        return None, [], None, []
    transcripts = r.json().get("data", {}).get("transcripts", [])

    # Find the group coaching call — prefer title match, fall back to longest duration
    group_call = None
    for t in transcripts:
        title = (t.get("title") or "").lower()
        if "group call" in title or "flow code" in title or "group coaching" in title or title == "tfc":
            group_call = t
            break
    if not group_call and transcripts:
        group_call = max(transcripts, key=lambda t: t.get("duration") or 0)

    if not group_call:
        log.warning("No Fireflies transcript found for this date")
        return None, [], None, []

    transcript_id = group_call["id"]
    title         = group_call.get("title", "TFC Group Call")
    duration      = group_call.get("duration") or 0
    sentences     = group_call.get("sentences") or []
    fireflies_url = f"https://app.fireflies.ai/view/{transcript_id}"
    chapters      = parse_chapters((group_call.get("summary") or {}).get("shorthand_bullet", ""))

    log.info(f"Found Fireflies transcript: {title} ({len(sentences)} sentences, {len(chapters)} chapters)")

    # Generate TFC-aware summary via Claude
    tfc_context = load_tfc_context()
    summary, keywords = generate_tfc_summary(sentences, title, call_date, duration, tfc_context)

    return summary, keywords, fireflies_url, chapters


def get_fireflies_summary_by_id(transcript_id):
    """Fetch transcript by ID and generate Claude summary."""
    query = """
    query($id: String!) {
      transcript(id: $id) {
        id
        title
        duration
        sentences { speaker_name text start_time }
        summary { shorthand_bullet }
      }
    }
    """
    try:
        r = requests.post(
            "https://api.fireflies.ai/graphql",
            headers={"Authorization": f"Bearer {env('FIREFLIES_API_KEY')}",
                     "Content-Type": "application/json"},
            json={"query": query, "variables": {"id": transcript_id}},
            timeout=30
        )
    except Exception as e:
        log.warning(f"Fireflies request failed: {e}")
        return None, [], None, []
    if r.status_code != 200:
        log.warning(f"Fireflies API returned {r.status_code}")
        return None, [], None, []
    t = r.json().get("data", {}).get("transcript", {})
    if not t:
        return None, [], None, []

    title     = t.get("title", "TFC Group Call")
    duration  = t.get("duration") or 0
    sentences = t.get("sentences") or []
    fireflies_url = f"https://app.fireflies.ai/view/{transcript_id}"
    chapters = parse_chapters((t.get("summary") or {}).get("shorthand_bullet", ""))

    log.info(f"Fetched Fireflies transcript by ID: {title} ({len(sentences)} sentences, {len(chapters)} chapters)")

    tfc_context = load_tfc_context()
    # Extract date from transcript_id if possible, else use today
    from datetime import date as _date
    call_date = _date.today().strftime("%Y-%m-%d")
    summary, keywords = generate_tfc_summary(sentences, title, call_date, duration, tfc_context)

    return summary, keywords, fireflies_url, chapters


# ── Slack ────────────────────────────────────────────────────────────────────
def post_to_slack(youtube_url, fireflies_url, call_date, summary, keywords):
    """Post a clean Slack notification. Skips sections when only fallback/no data available."""
    lines = [
        f"🎙️ *TFC Group Call — {call_date}*",
        youtube_url,
    ]

    if summary:
        lines.append(f"\n{summary}")

    if keywords:
        lines.append("  ·  ".join(keywords[:8]))

    has_real_ff_url = bool(fireflies_url and fireflies_url != "https://app.fireflies.ai")
    if has_real_ff_url:
        lines.append(f"\n🔥 Full transcript: {fireflies_url}")
    elif not summary:
        lines.append("_(Transcript will be linked once Fireflies finishes processing)_")

    message = "\n".join(lines)
    r = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {env('SLACK_BOT_TOKEN')}"},
        json={"channel": env("SLACK_CHANNEL_ID"), "text": message, "unfurl_links": False, "unfurl_media": False},
    )
    data = r.json()
    if not data.get("ok"):
        log.error(f"Slack post failed: {data}")
    else:
        log.info(f"Slack posted to {env('SLACK_CHANNEL_ID')}")


# ── Notion ───────────────────────────────────────────────────────────────────
def update_notion(youtube_url, call_date, summary, keywords, fireflies_url=None, call_date_display=None, chapters=None):
    from datetime import datetime as _dt
    if not call_date_display:
        try:
            call_date_display = _dt.strptime(call_date, "%Y-%m-%d").strftime("%B %-d, %Y")
        except Exception:
            call_date_display = call_date
    headers = {
        "Authorization": f"Bearer {env('NOTION_TOKEN')}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    kw_list = [k.strip() for k in (keywords or []) if k.strip()]
    properties = {
        "Call Title": {"title": [{"text": {"content": f"TFC Group Call — {call_date_display}"}}]},
        "Date": {"date": {"start": call_date}},
        "Theme / Topic": {"rich_text": [{"text": {"content": ", ".join(kw_list)}}]},
        "Recording Link": {"url": youtube_url},
        "Status": {"select": {"name": "Complete"}},
    }
    if kw_list:
        properties["Keywords"] = {"multi_select": [{"name": k[:100]} for k in kw_list]}
    if fireflies_url and fireflies_url != "https://app.fireflies.ai":
        properties["Fireflies Link"] = {"url": fireflies_url}

    children = [
        {"object": "block", "type": "heading_2", "heading_2": {"rich_text": [{"text": {"content": "Session Notes"}}]}},
        {"object": "block", "type": "paragraph", "paragraph": {"rich_text": [{"text": {"content": summary}}]}},
        {"object": "block", "type": "heading_2", "heading_2": {"rich_text": [{"text": {"content": "Key Themes"}}]}},
        *[
            {"object": "block", "type": "bulleted_list_item", "bulleted_list_item": {"rich_text": [{"text": {"content": kw}}]}}
            for kw in keywords
        ],
    ]
    if chapters:
        chapter_text = "\n".join(f"{t}  {title}" for t, title in chapters)
        children += [
            {"object": "block", "type": "heading_2", "heading_2": {"rich_text": [{"text": {"content": "Timestamps"}}]}},
            {"object": "block", "type": "paragraph", "paragraph": {"rich_text": [{"text": {"content": chapter_text}}]}},
        ]

    data = {
        "parent": {"database_id": env("NOTION_DB_ID")},
        "properties": properties,
        "children": children,
    }
    r = requests.post("https://api.notion.com/v1/pages", headers=headers, json=data)
    if r.status_code != 200:
        log.error(f"Notion update failed: {r.status_code} {r.text}")
    else:
        log.info("Notion Flow Code Call Archive updated")


# ── Core pipeline ────────────────────────────────────────────────────────────
def process_recording(payload):
    """Run in background thread after webhook is acknowledged."""
    try:
        meeting = payload.get("payload", {}).get("object", {})
        topic = meeting.get("topic", "")
        duration = meeting.get("duration", 0)

        # Only process group coaching calls
        if PERSONAL_ROOM_TOPIC.lower() not in topic.lower():
            log.info(f"Skipping: topic '{topic}' doesn't match personal room")
            return
        if duration < MIN_DURATION_MINUTES:
            log.info(f"Skipping: duration {duration}m < {MIN_DURATION_MINUTES}m minimum")
            return

        # Convert UTC start time to Pacific time for display
        from datetime import datetime, timezone
        from zoneinfo import ZoneInfo
        utc_start = meeting.get("start_time", "")
        try:
            dt_utc = datetime.strptime(utc_start.split(".")[0].rstrip("Z"), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            dt_pacific = dt_utc.astimezone(ZoneInfo("America/Los_Angeles"))
            start_time = dt_pacific.strftime("%Y-%m-%d")
            call_date_display = dt_pacific.strftime("%B %-d, %Y")  # e.g. "June 2, 2026"
        except Exception:
            start_time = utc_start[:10]
            call_date_display = utc_start[:10]

        # Find the MP4 recording file
        recording_files = meeting.get("recording_files", [])
        mp4_file = next(
            (f for f in recording_files
             if f.get("file_extension") == "MP4"
             and "speaker" in f.get("recording_type", "").lower()
             and f.get("status") == "completed"),
            None
        )
        if not mp4_file:
            log.warning("No MP4 file found in recording")
            return

        download_url = mp4_file["download_url"]

        # Zoom webhook_download URLs require the download_token from the webhook payload,
        # NOT a regular OAuth token. The token is at payload["download_token"].
        download_token = payload.get("download_token") or payload.get("payload", {}).get("download_token")
        if not download_token:
            # Fall back to OAuth token (works for /v2/meetings/... API download URLs)
            log.info("No download_token in payload — falling back to OAuth token")
            download_token = get_zoom_token()
        else:
            log.info("Using webhook download_token for recording download")

        # Download MP4 to temp file
        log.info(f"Downloading recording for {call_date_display}...")

        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
            tmp_path = tmp.name

        with requests.get(download_url, stream=True, timeout=600,
                          headers={"Authorization": f"Bearer {download_token}"}) as r:
            log.info(f"Download response: {r.status_code}, Content-Type: {r.headers.get('Content-Type')}, Content-Length: {r.headers.get('Content-Length')}")
            r.raise_for_status()
            content_type = r.headers.get('Content-Type', '')
            if 'html' in content_type.lower():
                log.error(f"Got HTML response instead of video — Zoom auth required. Content-Type: {content_type}")
                return
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    f.write(chunk)

        file_size = os.path.getsize(tmp_path)
        log.info(f"Downloaded {file_size} bytes to {tmp_path}")

        log.info(f"Downloaded to {tmp_path}, uploading to YouTube...")

        # Upload to YouTube
        title = f"TFC Group Call — {call_date_display}"
        description = "Weekly TFC Flow Code Group Coaching Call"
        youtube_url = upload_to_youtube(tmp_path, title, description)
        log.info(f"YouTube URL: {youtube_url}")

        # Clean up temp file
        os.unlink(tmp_path)

        # Pull Fireflies summary — wrapped so any network error never kills Slack/Notion
        try:
            summary, keywords, fireflies_url, chapters = get_fireflies_summary(start_time)
        except Exception as e:
            log.warning(f"Fireflies summary fetch failed (non-fatal): {e}")
            summary, keywords, fireflies_url, chapters = None, [], None, []

        # Update YouTube description with chapters now that we have Fireflies data
        if chapters:
            desc = format_youtube_description(call_date_display, chapters)
            update_youtube_description(youtube_url, desc)

        # Post to Slack — pass raw values so placeholder content isn't shown
        post_to_slack(youtube_url, fireflies_url, call_date_display, summary, keywords)

        # Apply fallbacks only for Notion (needs non-null values for properties)
        notion_summary  = summary  or "Weekly TFC Flow Code Group Coaching Call."
        notion_keywords = keywords or ["Group Coaching", "Flow Code"]
        notion_ff_url   = fireflies_url if (fireflies_url and fireflies_url != "https://app.fireflies.ai") else None
        update_notion(youtube_url, start_time, notion_summary, notion_keywords,
                      fireflies_url=notion_ff_url, call_date_display=call_date_display,
                      chapters=chapters)

        log.info(f"Pipeline complete for {call_date_display}: {youtube_url}")

    except Exception as e:
        log.error(f"Pipeline error: {e}", exc_info=True)
        raise


# ── Routes ───────────────────────────────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/test-notifications", methods=["POST"])
def test_notifications():
    """Test Slack + Notion without downloading or uploading anything."""
    youtube_url = "https://www.youtube.com/watch?v=TEST123"
    call_date = "2026-06-03"
    summary = "Test notification — verifying Slack and Notion integration."
    fireflies_url = "https://app.fireflies.ai/view/test"
    keywords = ["Test", "Community Support", "Flow Code"]
    try:
        update_notion(youtube_url, call_date, summary, keywords)
        return jsonify({"status": "ok", "youtube_url": youtube_url})
    except Exception as e:
        log.error(f"Test notification error: {e}", exc_info=True)
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/zoom-webhook", methods=["POST"])
def zoom_webhook():
    payload = request.get_json(force=True)

    # Handle Zoom's URL validation challenge
    if payload.get("event") == "endpoint.url_validation":
        token = payload["payload"]["plainToken"]
        hash_val = hmac.new(
            env("ZOOM_WEBHOOK_SECRET").encode(), token.encode(), hashlib.sha256
        ).hexdigest()
        return jsonify({"plainToken": token, "encryptedToken": hash_val})

    # Verify signature on all other events
    if not verify_zoom_signature(request):
        return jsonify({"error": "Invalid signature"}), 401

    if payload.get("event") == "recording.completed":
        # Acknowledge immediately, process in background
        thread = threading.Thread(target=process_recording, args=(payload,))
        thread.daemon = True
        thread.start()

    return jsonify({"status": "received"}), 200


@app.route("/post-notifications", methods=["POST"])
def post_notifications():
    """Post Slack + Notion for a recording that already uploaded to YouTube.
    Body: {"youtube_url": "...", "date": "YYYY-MM-DD", "transcript_id": "optional"}
    """
    body = request.get_json(force=True) or {}
    youtube_url = body.get("youtube_url")
    date = body.get("date")
    transcript_id = body.get("transcript_id")
    if not youtube_url or not date:
        return jsonify({"error": "youtube_url and date required"}), 400
    try:
        if transcript_id:
            summary, keywords, fireflies_url, chapters = get_fireflies_summary_by_id(transcript_id)
        else:
            summary, keywords, fireflies_url, chapters = get_fireflies_summary(date)
        from datetime import datetime
        try:
            call_date_display = datetime.strptime(date, "%Y-%m-%d").strftime("%B %-d, %Y")
        except Exception:
            call_date_display = date
        if chapters:
            update_youtube_description(youtube_url, format_youtube_description(call_date_display, chapters))
        # Slack gets raw values — no placeholder fallbacks
        post_to_slack(youtube_url, fireflies_url, call_date_display, summary, keywords)
        # Notion gets fallbacks so properties are never null
        notion_summary  = summary  or "Weekly TFC Flow Code Group Coaching Call."
        notion_keywords = keywords or ["Group Coaching", "Flow Code"]
        notion_ff_url   = fireflies_url if (fireflies_url and fireflies_url != "https://app.fireflies.ai") else None
        update_notion(youtube_url, date, notion_summary, notion_keywords,
                      fireflies_url=notion_ff_url, call_date_display=call_date_display,
                      chapters=chapters)
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        log.error(f"post-notifications error: {e}", exc_info=True)
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/update-notion", methods=["POST"])
def update_notion_only():
    """Update Notion only (no Slack) for a recording that already uploaded to YouTube.
    Body: {"youtube_url": "...", "date": "YYYY-MM-DD", "transcript_id": "optional"}
    """
    body = request.get_json(force=True) or {}
    youtube_url = body.get("youtube_url")
    date = body.get("date")
    transcript_id = body.get("transcript_id")
    if not youtube_url or not date:
        return jsonify({"error": "youtube_url and date required"}), 400
    try:
        if transcript_id:
            summary, keywords, fireflies_url, chapters = get_fireflies_summary_by_id(transcript_id)
        else:
            summary, keywords, fireflies_url, chapters = get_fireflies_summary(date)
        if not summary:
            summary = "Weekly TFC Flow Code Group Coaching Call."
        if not keywords:
            keywords = ["Community Support", "Group Coaching", "Flow Code"]
        from datetime import datetime
        try:
            call_date_display = datetime.strptime(date, "%Y-%m-%d").strftime("%B %-d, %Y")
        except Exception:
            call_date_display = date
        if chapters:
            update_youtube_description(youtube_url, format_youtube_description(call_date_display, chapters))
        update_notion(youtube_url, date, summary, keywords,
                      fireflies_url=fireflies_url, call_date_display=call_date_display,
                      chapters=chapters)
        return jsonify({"status": "ok", "title": f"TFC Group Call — {call_date_display}",
                        "chapters": len(chapters)}), 200
    except Exception as e:
        log.error(f"update-notion error: {e}", exc_info=True)
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/manual-trigger/<date>", methods=["POST"])
def manual_trigger(date):
    """Manually trigger pipeline for a given date (YYYY-MM-DD).
    Fetches the Zoom recording from that date and runs the full pipeline.
    Example: POST /manual-trigger/2026-06-09
    """
    try:
        zoom_token = get_zoom_token()
        r = requests.get(
            "https://api.zoom.us/v2/users/me/recordings",
            headers={"Authorization": f"Bearer {zoom_token}"},
            params={"from": date, "to": date, "page_size": 10},
            timeout=30,
        )
        r.raise_for_status()
        meetings = r.json().get("meetings", [])
        if not meetings:
            return jsonify({"error": f"No recordings found for {date}"}), 404

        # Find the longest meeting (most likely the group coaching call)
        meeting = max(meetings, key=lambda m: m.get("duration", 0))
        log.info(f"Manual trigger: found meeting '{meeting.get('topic')}' duration={meeting.get('duration')}m")

        # Reconstruct a webhook-compatible payload and run the pipeline
        payload = {"payload": {"object": meeting}}
        thread = threading.Thread(target=process_recording, args=(payload,))
        thread.daemon = True
        thread.start()

        return jsonify({
            "status": "triggered",
            "topic": meeting.get("topic"),
            "duration": meeting.get("duration"),
            "date": date,
        }), 200

    except Exception as e:
        log.error(f"Manual trigger error: {e}", exc_info=True)
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/update-youtube-description", methods=["POST"])
def update_yt_description_only():
    """Update only the YouTube description — no Slack, no Notion.
    Body: {"youtube_url": "...", "date": "YYYY-MM-DD", "transcript_id": "optional"}
    """
    body = request.get_json(force=True) or {}
    youtube_url = body.get("youtube_url")
    date = body.get("date")
    transcript_id = body.get("transcript_id")
    if not youtube_url or not date:
        return jsonify({"error": "youtube_url and date required"}), 400
    try:
        if transcript_id:
            _, _, _, chapters = get_fireflies_summary_by_id(transcript_id)
        else:
            _, _, _, chapters = get_fireflies_summary(date)
        from datetime import datetime
        try:
            call_date_display = datetime.strptime(date, "%Y-%m-%d").strftime("%B %-d, %Y")
        except Exception:
            call_date_display = date
        if not chapters:
            return jsonify({"status": "no_chapters", "message": "No chapters found for this transcript"}), 200
        desc = format_youtube_description(call_date_display, chapters)
        update_youtube_description(youtube_url, desc)
        return jsonify({"status": "ok", "chapters": len(chapters), "description": desc}), 200
    except Exception as e:
        log.error(f"update-youtube-description error: {e}", exc_info=True)
        return jsonify({"status": "error", "error": str(e)}), 500



if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

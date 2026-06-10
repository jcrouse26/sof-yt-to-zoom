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


# ── Fireflies ────────────────────────────────────────────────────────────────
def get_fireflies_summary(call_date):
    """Fetch summary and keywords for the group coaching call on a given date."""
    query = """
    query($fromDate: String, $toDate: String) {
      transcripts(fromDate: $fromDate, toDate: $toDate) {
        id
        title
        summary {
          short_summary
          keywords
          bullet_gist
        }
      }
    }
    """
    # Look in a 2-day window around the call date
    from datetime import datetime, timedelta
    dt = datetime.strptime(call_date, "%Y-%m-%d")
    from_date = (dt - timedelta(days=1)).strftime("%Y-%m-%d")
    to_date   = (dt + timedelta(days=1)).strftime("%Y-%m-%d")

    r = requests.post(
        "https://api.fireflies.ai/graphql",
        headers={"Authorization": f"Bearer {env('FIREFLIES_API_KEY')}",
                 "Content-Type": "application/json"},
        json={"query": query, "variables": {"fromDate": from_date, "toDate": to_date}},
        timeout=30
    )
    if r.status_code != 200:
        log.warning(f"Fireflies API returned {r.status_code} — skipping summary")
        return None, [], None
    transcripts = r.json().get("data", {}).get("transcripts", [])

    # Find the group coaching call (longest call or title match)
    group_call = None
    for t in transcripts:
        title = (t.get("title") or "").lower()
        if "group call" in title or "flow code" in title or "group coaching" in title:
            group_call = t
            break

    if not group_call and transcripts:
        group_call = transcripts[0]

    if not group_call or not group_call.get("summary"):
        log.warning("No Fireflies summary found for this date")
        return None, [], None

    summary_obj = group_call["summary"]
    short_summary = summary_obj.get("short_summary", "")
    keywords      = summary_obj.get("keywords", [])
    if isinstance(keywords, str):
        keywords = [k.strip() for k in keywords.split(",")]
    fireflies_url = f"https://app.fireflies.ai/view/{group_call['id']}"
    log.info(f"Found Fireflies transcript: {group_call['title']}")
    return short_summary, keywords, fireflies_url


# ── Slack ────────────────────────────────────────────────────────────────────
def post_to_slack(youtube_url, fireflies_url, call_date, summary, keywords):
    themes = ", ".join(keywords) if keywords else ""
    message = f"📹 Recording {call_date}:\n{youtube_url}"
    if themes:
        message += f"\n\nSome themes: {themes}"
    if summary:
        message += f"\n\n{summary}"
    if fireflies_url:
        message += f"\n\n🔥 Full transcript: {fireflies_url}"
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
def update_notion(youtube_url, call_date, summary, keywords):
    headers = {
        "Authorization": f"Bearer {env('NOTION_TOKEN')}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    data = {
        "parent": {"database_id": env("NOTION_DB_ID")},
        "properties": {
            "Call Title": {"title": [{"text": {"content": f"TFC Group Coaching — {call_date}"}}]},
            "Date": {"date": {"start": call_date}},
            "Theme / Topic": {"rich_text": [{"text": {"content": ", ".join(keywords)}}]},
            "Recording Link": {"url": youtube_url},
            "Status": {"select": {"name": "Complete"}},
        },
        "children": [
            {"object": "block", "type": "heading_2", "heading_2": {"rich_text": [{"text": {"content": "Session Notes"}}]}},
            {"object": "block", "type": "paragraph", "paragraph": {"rich_text": [{"text": {"content": summary}}]}},
            {"object": "block", "type": "heading_2", "heading_2": {"rich_text": [{"text": {"content": "Key Themes"}}]}},
            *[
                {"object": "block", "type": "bulleted_list_item", "bulleted_list_item": {"rich_text": [{"text": {"content": kw}}]}}
                for kw in keywords
            ],
        ],
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
        from datetime import timezone
        from zoneinfo import ZoneInfo
        utc_start = meeting.get("start_time", "")
        try:
            dt_utc = datetime.strptime(utc_start, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
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

        # Download MP4 to temp file
        log.info(f"Downloading recording for {call_date_display}...")
        zoom_token = get_zoom_token()
        log.info("Got Zoom OAuth token")

        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
            tmp_path = tmp.name

        with requests.get(download_url, stream=True, timeout=600,
                          headers={"Authorization": f"Bearer {zoom_token}"}) as r:
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
        title = f"TFC Group Coaching Call — {call_date_display}"
        description = "Weekly TFC Flow Code Group Coaching Call"
        youtube_url = upload_to_youtube(tmp_path, title, description)
        log.info(f"YouTube URL: {youtube_url}")

        # Clean up temp file
        os.unlink(tmp_path)

        # Pull Fireflies summary
        summary, keywords, fireflies_url = get_fireflies_summary(start_time)
        if not summary:
            summary = "Weekly TFC Flow Code Group Coaching Call."
        if not keywords:
            keywords = ["Community Support", "Group Coaching", "Flow Code"]
        if not fireflies_url:
            fireflies_url = "https://app.fireflies.ai"

        # Post to Slack
        post_to_slack(youtube_url, fireflies_url, call_date_display, summary, keywords)

        # Update Notion
        update_notion(youtube_url, start_time, summary, keywords)

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
    Body: {"youtube_url": "...", "date": "YYYY-MM-DD"}
    """
    body = request.get_json(force=True) or {}
    youtube_url = body.get("youtube_url")
    date = body.get("date")
    if not youtube_url or not date:
        return jsonify({"error": "youtube_url and date required"}), 400
    try:
        summary, keywords, fireflies_url = get_fireflies_summary(date)
        if not summary:
            summary = "Weekly TFC Flow Code Group Coaching Call."
        if not keywords:
            keywords = ["Community Support", "Group Coaching", "Flow Code"]
        if not fireflies_url:
            fireflies_url = "https://app.fireflies.ai"
        from zoneinfo import ZoneInfo
        from datetime import datetime
        try:
            call_date_display = datetime.strptime(date, "%Y-%m-%d").strftime("%B %-d, %Y")
        except Exception:
            call_date_display = date
        post_to_slack(youtube_url, fireflies_url, call_date_display, summary, keywords)
        update_notion(youtube_url, date, summary, keywords)
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        log.error(f"post-notifications error: {e}", exc_info=True)
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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

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


# ── Slack ────────────────────────────────────────────────────────────────────
def post_to_slack(youtube_url, fireflies_url, call_date, summary):
    message = (
        f"🎙️ *TFC Group Coaching Call — {call_date}* is now available\n\n"
        f"*Summary:* {summary}\n\n"
        f"📺 *Recording:* {youtube_url}\n"
        f"🔥 *Full Transcript:* {fireflies_url}"
    )
    r = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {env('SLACK_BOT_TOKEN')}"},
        json={"channel": env("SLACK_CHANNEL_ID"), "text": message},
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

        start_time = meeting.get("start_time", "")[:10]  # YYYY-MM-DD
        call_date_display = meeting.get("start_time", "")[:10]

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

        # Post to Slack
        summary = "Weekly TFC group coaching session. Members shared wins, challenges, and received live coaching."
        fireflies_url = "https://app.fireflies.ai"  # placeholder — Fireflies link added manually if needed
        post_to_slack(youtube_url, fireflies_url, call_date_display, summary)

        # Update Notion
        keywords = ["Community Support", "Group Coaching", "Flow Code"]
        update_notion(youtube_url, start_time, summary, keywords)

        log.info(f"Pipeline complete for {call_date_display}: {youtube_url}")

    except Exception as e:
        log.error(f"Pipeline error: {e}", exc_info=True)
        raise


# ── Routes ───────────────────────────────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

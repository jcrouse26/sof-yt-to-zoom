#!/usr/bin/env python3
"""
youtube_upload.py — Upload a video file to YouTube as unlisted
Usage: python3 youtube_upload.py --file /path/to/video.mp4 --title "Title" --description "Desc"

First-time setup:
  1. python3 youtube_upload.py --setup
  This opens a browser for Google OAuth. Credentials saved to youtube_token.json.

Subsequent runs are fully automated.
"""

import argparse
import json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TOKEN_FILE = os.path.join(SCRIPT_DIR, "youtube_token.json")
CREDENTIALS_FILE = os.path.join(SCRIPT_DIR, "youtube_client_secrets.json")

SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

def get_authenticated_service():
    """Return an authenticated YouTube API service, refreshing token if needed."""
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
    except ImportError:
        print("Missing dependencies. Run: pip3 install google-auth google-auth-oauthlib google-api-python-client --break-system-packages")
        sys.exit(1)

    creds = None

    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE) as f:
            creds = Credentials.from_authorized_user_info(json.load(f), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(CREDENTIALS_FILE):
                print(f"\nERROR: {CREDENTIALS_FILE} not found.")
                print("Download your OAuth client secrets from Google Cloud Console and save as:")
                print(f"  {CREDENTIALS_FILE}")
                print("\nSee README_YOUTUBE_SETUP.md for step-by-step instructions.")
                sys.exit(1)
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
        print(f"Token saved to {TOKEN_FILE}")

    return build("youtube", "v3", credentials=creds)


def upload_video(file_path, title, description="", privacy="unlisted"):
    """Upload a video to YouTube and return the video URL."""
    try:
        from googleapiclient.http import MediaFileUpload
        from googleapiclient.errors import HttpError
    except ImportError:
        print("Missing dependency: pip3 install google-api-python-client --break-system-packages")
        sys.exit(1)

    if not os.path.exists(file_path):
        print(f"ERROR: File not found: {file_path}")
        sys.exit(1)

    youtube = get_authenticated_service()

    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": ["TFC", "Flow Code", "Group Coaching"],
            "categoryId": "27",  # Education
        },
        "status": {
            "privacyStatus": privacy,
            "selfDeclaredMadeForKids": False,
        },
    }

    media = MediaFileUpload(file_path, chunksize=-1, resumable=True)

    print(f"Uploading: {file_path}")
    print(f"Title: {title}")
    print(f"Privacy: {privacy}")

    try:
        request = youtube.videos().insert(
            part=",".join(body.keys()),
            body=body,
            media_body=media,
        )

        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                pct = int(status.progress() * 100)
                print(f"  Upload progress: {pct}%", end="\r")

        video_id = response["id"]
        url = f"https://www.youtube.com/watch?v={video_id}"
        print(f"\nUpload complete: {url}")
        return url

    except Exception as e:
        print(f"Upload failed: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Upload a video to YouTube")
    parser.add_argument("--file", help="Path to video file")
    parser.add_argument("--title", help="Video title")
    parser.add_argument("--description", default="", help="Video description")
    parser.add_argument("--privacy", default="unlisted", choices=["public", "unlisted", "private"])
    parser.add_argument("--setup", action="store_true", help="Run OAuth setup only")
    args = parser.parse_args()

    if args.setup:
        print("Running YouTube OAuth setup...")
        get_authenticated_service()
        print("Setup complete. You can now upload videos.")
        return

    if not args.file or not args.title:
        parser.print_help()
        sys.exit(1)

    url = upload_video(args.file, args.title, args.description, args.privacy)
    print(url)


if __name__ == "__main__":
    main()

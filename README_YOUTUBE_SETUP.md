# YouTube Upload — One-Time Setup

You only need to do this once. After that, uploads are fully automated.

## Step 1: Install Dependencies

Open Terminal and run:
```
pip3 install google-auth google-auth-oauthlib google-api-python-client --break-system-packages
```

## Step 2: Authorize

Run this once — it opens a browser window for you to approve access:
```
python3 "/Users/jasoncrouse/Desktop/SOF YouTube Automation/youtube_upload.py" --setup
```

Approve in the browser. A `youtube_token.json` file gets saved here. That's it — future uploads are silent and automatic.

## Testing

```
python3 "/Users/jasoncrouse/Desktop/SOF YouTube Automation/youtube_upload.py" \
  --file "/path/to/test-video.mp4" \
  --title "Test Upload" \
  --privacy unlisted
```

# Pipedream Python Code Step
# REQUIREMENTS:
# Add youtube-transcript-api==0.6.2 (or latest) to your requirements
# in the step's configuration

# --- Imports ---
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound
import json
import os  # Import the 'os' module to access environment variables
import time
import random

# --- Handler Function ---
def handler(pd: "pipedream"):

    # --- Get Input Directly from Trigger Event ---
    # Access the videoId. Double-check this path matches your trigger data structure.
    # Using body.video_id based on previous discussion.
    video_id = pd.steps["get_document"]["$return_value"]["data"]["video_id"]
    print(f"Received Video ID from trigger body: {video_id}")

    # --- Input Validation ---
    if not video_id:
        print("Error: Missing 'video_id' in trigger's body.")
        pd.respond({
            "status": 400,
            "headers": { "Content-Type": "application/json" },
            "body": json.dumps({ "error": "Missing 'video_id' in request body" })
        })
        return

    # --- Get Proxy Info from Environment Variables ---
    # !! Verify these names EXACTLY match your Pipedream Environment Variable settings !!
    proxy_host = os.environ.get("PROXY_HOST")
    proxy_port = os.environ.get("PROXY_PORT")
    proxy_user = os.environ.get("PROXY_USER") # Optional depending on proxy
    proxy_pass = os.environ.get("PROXY_PASS") # Optional depending on proxy

    proxies = None
    # Check if HOST and PORT are present (most crucial)
    if proxy_host and proxy_port:
        proxy_url_base = f"{proxy_host}:{proxy_port}"
        # Include user/pass in URL only if both are present
        if proxy_user and proxy_pass:
             proxy_url = f"http://{proxy_user}:{proxy_pass}@{proxy_url_base}"
             print(f"Configuring proxy with credentials: http://***:***@{proxy_url_base}")
        else:
             proxy_url = f"http://{proxy_url_base}"
             print(f"Configuring proxy without credentials: {proxy_url}")

        proxies = {
            'http': proxy_url,
            'https': proxy_url,
        }
        print(f"Attempting request using configured proxy.")
    else:
        # Log clearly if proxies aren't being used
        print("Proxy environment variables PROXY_HOST and/or PROXY_PORT not set. Proceeding without proxy.")
        # Depending on requirements, you might want to raise an error here
        # if proxies are mandatory for your use case.

    # --- Retry Wrapper Function ---
    def fetch_transcript_with_retries(video_id, languages, proxies, max_retries=5, base_delay=2):
        for attempt in range(1, max_retries + 1):
            try:
                print(f"Fetch attempt {attempt} for videoId: {video_id}")
                transcript_list = YouTubeTranscriptApi.get_transcript(
                    video_id,
                    languages=languages,
                    proxies=proxies
                )
                return transcript_list
            except Exception as e:
                error_message = str(e)
                if ("no element found" in error_message or
                    "Could not retrieve a transcript" in error_message or
                    "timed out" in error_message or
                    "temporarily unavailable" in error_message):
                    wait = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                    print(f"Attempt {attempt} failed: {error_message}. Retrying in {wait:.1f} seconds...")
                    time.sleep(wait)
                else:
                    print(f"Non-retryable error on attempt {attempt}: {error_message}")
                    raise
        raise Exception(f"Failed to fetch transcript after {max_retries} attempts.")

    # --- Fetch Transcript Logic ---
    try:
        print(f"Attempting to fetch transcript for videoId: {video_id}, language: 'en'")
        transcript_list = fetch_transcript_with_retries(
            video_id,
            languages=['en'],
            proxies=proxies
        )

        full_transcript = ' '.join(segment['text'] for segment in transcript_list)
        print(f"Successfully fetched transcript for videoId: {video_id}. Length: {len(full_transcript)}")

        # --- Return data for subsequent steps ---
        success_data = {
            'videoId': video_id,
            'language': 'en',
            'transcript': full_transcript
        }
        # This return makes the data available as steps.<step_name>.return_value
        return success_data

    # --- Exception Handling ---
    except TranscriptsDisabled:
        print(f"TranscriptsDisabled error for videoId: {video_id}")
        pd.respond({ "status": 400, "headers": { "Content-Type": "application/json" }, "body": json.dumps({ "videoId": video_id, "error": "Transcripts are disabled for this video" }) })
        return

    except NoTranscriptFound:
        print(f"NoTranscriptFound error for videoId: {video_id}, language: 'en'")
        pd.respond({ "status": 404, "headers": { "Content-Type": "application/json" }, "body": json.dumps({ "videoId": video_id, "error": "No English transcript found for this video" }) })
        return

    except Exception as e:
        error_message = str(e)
        proxy_status = "with proxy" if proxies else "without proxy"
        # Check for specific errors like the IP block
        if "Could not retrieve a transcript" in error_message and "blocking requests" in error_message:
             print(f"IP block error occurred {proxy_status} for videoId: {video_id}.")
             pd.respond({ "status": 403, "headers": { "Content-Type": "application/json" }, "body": json.dumps({ "videoId": video_id, "error": f"YouTube blocked the request ({proxy_status}). IP likely flagged." }) })
        else:
             # Handle other generic errors
             print(f"Generic error fetching transcript ({proxy_status}) for videoId: {video_id}: {error_message}")
             pd.respond({ "status": 500, "headers": { "Content-Type": "application/json" }, "body": json.dumps({ "videoId": video_id, "error": f'Error fetching transcript: {error_message}' }) })
        return

# ---
# Google Cloud Logging code has been removed. All logging is now via print statements only.
# ---

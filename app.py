import os
import subprocess
import requests
import threading
import logging
import time
import json # ffprobe আউটপুট পার্স করার জন্য
from flask import Flask, render_template, send_from_directory, abort

# === Logging Configuration ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# === Flask App Initialization ===
app = Flask(__name__)

# === Configuration Constants ===
# মূল ভিডিও URL (ধরে নিলাম এতে ইংরেজি অডিও আছে - অনুগ্রহ করে আসল URL ব্যবহার করুন)
# *** গুরুত্বপূর্ণ: নিচের URL গুলো আপনার হিন্দি এবং জাপানিজ অডিওসহ ভিডিওর সঠিক URL দিয়ে প্রতিস্থাপন করুন ***
VIDEO_URL = "https://video-mxp1-1.xx.fbcdn.net/o1/v/t2/f2/m69/AQM8S3pFxa70tno6zop7jYr1U16B60EHmFPInE6TGBwoOaJnQOXYtCml3Qkpv-p01h-Mq8WY8cuwf4HDp-EFVkCJ.mp4?strext=1&_nc_cat=103&_nc_sid=5e9851&_nc_ht=video-mxp1-1.xx.fbcdn.net&_nc_ohc=PsDTzb3w2UsQ7kNvwEdobVT&efg=eyJ2ZW5jb2RlX3RhZyI6Inhwdl9wcm9ncmVzc2l2ZS5GQUNFQk9PSy4uQzMuNzIwLmRhc2hfaDI2NC1iYXNpYy1nZW4yXzcyMHAiLCJ4cHZfYXNzZXRfaWQiOjE0MDc5NDYzMjM1NTI4NTksInZpX3VzZWNhc2VfaWQiOjEwMTIyLCJkdXJhdGlvbl9zIjoyMDAsInVybGdlbl9zb3VyY2UiOiJ3d3cifQ%3D%3D&ccb=17-1&vs=f2a9875f9f41aa3d&_nc_vs=HBksFQIYOnBhc3N0aHJvdWdoX2V2ZXJzdG9yZS9HTmhqVUIzRVlmM3FMbU1DQVBzVVd5WFBEaXNHYm1kakFBQUYVAALIAQAVAhg6cGFzc3Rocm91Z2hfZXZlcnN0b3JlL0dJTkhVaDB6TFExRGFSOEZBR2tockdVaUNUdEdickZxQUFBRhUCAsgBACgAGAAbAogHdXNlX29pbAExEnByb2dyZXNzaXZlX3JlY2lwZQExFQAAJrap08ehoYAFFQIoAkMzLBdAaRiLQ5WBBhgZZGFzaF9oMjY0LWJhc2ljLWdlbjJfNzIwcBEAdQIA&_nc_zt=28&oh=00_AfEkU99vSfDRfVji51klRkyvAj5hml5FUlj3hFYozoLfGg&oe=68056F07&dl=1" # <--- এখানে সঠিক URL দিন
DOWNLOADED_FILENAME_BASE = "source_video" # ফাইলের নামের ভিত্তি

# অতিরিক্ত অডিও ট্র্যাকের সোর্স (ভাষা কোড -> {url, filename})
ADDITIONAL_AUDIO_SOURCES = {
    "hin": { # হিন্দি অডিওর জন্য
        "url": "https://video-lga3-1.xx.fbcdn.net/o1/v/t2/f2/m69/AQPMl8zJMnuo69uJZ2Vb5qA0zubB50NBwQXYxVaWgl5EhRxQerzsJsMZe-GK2ko7yKxeHwS9B41kbp0pAle1oSYE.mp4?strext=1&_nc_cat=108&_nc_sid=8bf8fe&_nc_ht=video-lga3-1.xx.fbcdn.net&_nc_ohc=xzuPd_k2hp8Q7kNvwH2ar5R&efg=eyJ2ZW5jb2RlX3RhZyI6Inhwdl9wcm9ncmVzc2l2ZS5GQUNFQk9PSy4uQzMuMzYwLnN2ZV9zZCIsInhwdl9hc3NldF9pZCI6MTA4Mjk0NzQ0Njg5MzYwMywidmlfdXNlY2FzZV9pZCI6MTAxMjIsImR1cmF0aW9uX3MiOjI0OSwidXJsZ2VuX3NvdXJjZSI6Ind3dyJ9&ccb=17-1&_nc_zt=28&oh=00_AfGvELNyDQqknfjjIeWGbpzBKq-JJG_xJSv1CM10lxqDMQ&oe=68064F92&dl=1", # <--- এখানে সঠিক URL দিন
        "filename": f"{DOWNLOADED_FILENAME_BASE}_hindi_audio.mp4"
    },
    "jpn": { # জাপানিজ অডিওর জন্য
        "url": "https://video-fra5-2.xx.fbcdn.net/o1/v/t2/f2/m69/AQPnpjAXNAHfjEG9CGWmu6SvHIGn8TnikG1T-wX7bBEK7YEU7O0U-6r5_S_AjjX-RJEwi7qkGZRX-ryxsW-I_K29.mp4?strext=1&_nc_cat=109&_nc_sid=8bf8fe&_nc_ht=video-fra5-2.xx.fbcdn.net&_nc_ohc=ruI9FINodn4Q7kNvwFWQb05&efg=eyJ2ZW5jb2RlX3RhZyI6Inhwdl9wcm9ncmVzc2l2ZS5GQUNFQk9PSy4uQzMuMzYwLnN2ZV9zZCIsInhwdl9hc3NldF9pZCI6OTI1MTc4NzgzMTQwOTE5LCJ2aV91c2VjYXNlX2lkIjoxMDEyMiwiZHVyYXRpb25fcyI6MjA1LCJ1cmxnZW5fc291cmNlIjoid3d3In0%3D&ccb=17-1&_nc_zt=28&oh=00_AfHADl463iCltfsTfM3yFX_eYe0AMiItht5QdPnLZ2AfUA&oe=680621F5&dl=1", # <--- এখানে সঠিক URL দিন
        "filename": f"{DOWNLOADED_FILENAME_BASE}_japanese_audio.mp4"
    }
    # প্রয়োজনে আরো ভাষা যোগ করতে পারেন
}

# ডাউনলোড করা ফাইলের তথ্য (মূল ভিডিও সহ)
# মূল ভিডিওর ভাষা কোড 'eng' ধরা হলো
DOWNLOAD_TARGETS = {
    "eng": { # ইংরেজি (মূল ভিডিও)
        "url": VIDEO_URL,
        "filename": f"{DOWNLOADED_FILENAME_BASE}_eng_video.mp4" # নাম পরিবর্তন করে ভাষা উল্লেখ করা হলো
    },
    **ADDITIONAL_AUDIO_SOURCES # ডিকশনারি দুটি মার্জ করা হলো
}

# অডিও ট্র্যাকের বিস্তারিত তথ্য (মাস্টার প্লেলিস্ট তৈরির জন্য)
# DOWNLOAD_TARGETS এর কী (key) গুলো এখানে ল্যাঙ্গুয়েজ কোড হিসেবে ব্যবহৃত হবে
AUDIO_TRACK_DETAILS = {
    "eng": {"name": "English", "default": True}, # কোনটি ডিফল্ট হবে
    "hin": {"name": "Hindi", "default": False},
    "jpn": {"name": "Japanese", "default": False},
    # DOWNLOAD_TARGETS এ যোগ করা অন্যান্য ভাষার জন্যও এখানে তথ্য যোগ করুন
}
# প্রক্রিয়াকরণের শুরুতে সক্রিয় ভাষার তালিকা (ডাউনলোড ব্যর্থ হলে এখান থেকে বাদ যাবে)
ACTIVE_AUDIO_LANGS = list(DOWNLOAD_TARGETS.keys())

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, 'downloads')
STATIC_DIR = os.path.join(BASE_DIR, 'static')
HLS_DIR = os.path.join(STATIC_DIR, 'hls')
MASTER_PLAYLIST_NAME = "master.m3u8"

# Define desired output resolutions and bitrates (height, video_bitrate, audio_bitrate)
RESOLUTIONS = [
    (360, '800k', '96k'),
    (480, '1400k', '128k'),
    (720, '2800k', '128k') # যদি মূল ভিডিও 720p বা তার বেশি হয়
]
FFMPEG_TIMEOUT = 3600 # Timeout for each ffmpeg command in seconds (60 minutes) - প্রয়োজনে বাড়ান

# === State Management Files ===
PROCESSING_LOCK_FILE = os.path.join(BASE_DIR, '.processing.lock')
HLS_READY_FILE = os.path.join(HLS_DIR, '.hls_ready')
ALL_DOWNLOADS_COMPLETE_MARKER = os.path.join(DOWNLOAD_DIR, '.all_downloads_complete')
PROCESSING_ERROR_FILE = os.path.join(BASE_DIR, '.processing.error')

# === Helper Functions ===

def ensure_dir(directory):
    """Creates a directory if it doesn't exist."""
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
            logging.info(f"Created directory: {directory}")
        except OSError as e:
            logging.error(f"Failed to create directory {directory}: {e}")
            raise # Re-raise exception as this is critical

def get_video_duration(file_path):
    """Gets the duration of a media file in seconds using ffprobe."""
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        logging.error(f"Cannot get duration: File not found or is empty at {file_path}")
        return None

    command = [
        'ffprobe',
        '-v', 'quiet',
        '-print_format', 'json',
        '-show_format',
        '-show_streams', # Include stream info (though format duration is usually sufficient)
        file_path
    ]
    logging.info(f"Running ffprobe to get duration for: {file_path}")
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=60) # Increased timeout for ffprobe
        data = json.loads(result.stdout)
        duration_str = data.get('format', {}).get('duration')

        if duration_str:
            duration = float(duration_str)
            logging.info(f"Duration found: {duration:.3f} seconds")
            if duration <= 0:
                 logging.warning(f"ffprobe reported non-positive duration ({duration}) for {file_path}. Check file integrity.")
                 return None # Treat non-positive duration as invalid
            return duration
        else:
            # Fallback: Check streams if format duration is missing (less common)
            first_video_stream = next((s for s in data.get('streams', []) if s.get('codec_type') == 'video'), None)
            if first_video_stream and 'duration' in first_video_stream:
                duration_str = first_video_stream['duration']
                duration = float(duration_str)
                logging.info(f"Duration found in video stream: {duration:.3f} seconds")
                if duration <= 0:
                    logging.warning(f"ffprobe reported non-positive stream duration ({duration}) for {file_path}.")
                    return None
                return duration
            else:
                 logging.error(f"Could not find duration in ffprobe output (format or stream) for {file_path}.")
                 return None

    except FileNotFoundError:
        logging.error("ffprobe command not found. Ensure ffmpeg (and ffprobe) is installed and in PATH.")
        return None
    except subprocess.CalledProcessError as e:
        logging.error(f"ffprobe failed for {file_path} with return code {e.returncode}. Error: {e.stderr}")
        return None
    except subprocess.TimeoutExpired:
        logging.error(f"ffprobe timed out after 60 seconds for {file_path}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse ffprobe JSON output for {file_path}: {e}")
        return None
    except ValueError as e:
         logging.error(f"Failed to convert duration to float for {file_path}: {e}")
         return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while getting duration for {file_path}: {e}", exc_info=True)
        return None

def check_ffmpeg_tools():
    """Checks if both ffmpeg and ffprobe are installed and accessible."""
    ffmpeg_ok = False
    ffprobe_ok = False
    error_details = ""

    # Check ffmpeg
    try:
        subprocess.run(['ffmpeg', '-version'], check=True, capture_output=True, text=True, timeout=10)
        logging.info("ffmpeg check successful.")
        ffmpeg_ok = True
    except Exception as e:
        error_details += f"ffmpeg check failed: {e}\n"
        logging.error(f"ffmpeg check failed: {e}")

    # Check ffprobe
    try:
        subprocess.run(['ffprobe', '-version'], check=True, capture_output=True, text=True, timeout=10)
        logging.info("ffprobe check successful.")
        ffprobe_ok = True
    except Exception as e:
        error_details += f"ffprobe check failed: {e}\n"
        logging.error(f"ffprobe check failed: {e}")

    if ffmpeg_ok and ffprobe_ok:
        return True
    else:
        error_msg = f"Required tool(s) missing or failed check:\n{error_details}"
        logging.error(error_msg)
        # Write error to file for user visibility in UI
        try:
            # Overwrite previous error file if tool check fails
            with open(PROCESSING_ERROR_FILE, 'w') as f:
                f.write(f"Fatal Error: ffmpeg and/or ffprobe are required but not found or not working.\nPlease install ffmpeg and ensure it's in the system PATH.\n\nDetails:\n{error_details}")
        except IOError as io_err:
             logging.error(f"Failed to write ffmpeg/ffprobe error to file: {io_err}")
        return False


def download_file(url, dest_path, lang_code):
    """Downloads a single file, handling potential issues."""
    logging.info(f"Attempting download for '{lang_code}' from {url} to {dest_path}...")
    # Check if file exists and is non-empty
    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
        logging.info(f"File for '{lang_code}' already exists and is not empty: {dest_path}. Skipping download.")
        return True
    elif os.path.exists(dest_path):
         logging.warning(f"File for '{lang_code}' exists but is empty, removing before download: {dest_path}")
         try:
             os.remove(dest_path)
         except OSError as e:
             logging.error(f"Could not remove empty file {dest_path}: {e}")
             # Continue to attempt download anyway

    try:
        with requests.get(url, stream=True, timeout=(15, 300)) as r: # (connect_timeout, read_timeout)
            r.raise_for_status() # Check for HTTP errors like 404, 500
            total_size = int(r.headers.get('content-length', 0))
            bytes_downloaded = 0
            start_time = time.time()

            log_prefix = f"Downloading '{lang_code}'"
            if total_size > 0:
                 logging.info(f"{log_prefix} ({total_size / (1024*1024):.2f} MB)...")
            else:
                 logging.info(f"{log_prefix} (size unknown)...")

            with open(dest_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192*4): # Use a larger chunk size
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)
                        bytes_downloaded += len(chunk)

            end_time = time.time()
            elapsed_time = end_time - start_time
            # Check if download was actually performed or skipped due to existing file
            if elapsed_time < 0.01 and bytes_downloaded == 0 and os.path.exists(dest_path):
                 # This likely means the file existed from a previous run; already logged above
                 pass
            else:
                if elapsed_time > 0:
                    download_speed = (bytes_downloaded / (1024*1024)) / elapsed_time
                    logging.info(f"Download for '{lang_code}' complete ({bytes_downloaded / (1024*1024):.2f} MB) in {elapsed_time:.2f}s ({download_speed:.2f} MB/s).")
                else:
                    logging.info(f"Download for '{lang_code}' complete ({bytes_downloaded / (1024*1024):.2f} MB) instantly (likely cached or very fast).")


        # Final check after download
        if not os.path.exists(dest_path) or os.path.getsize(dest_path) == 0:
             # This case might happen if the download was interrupted or server sent empty response
             raise ValueError(f"Downloaded file for '{lang_code}' is missing or empty after download attempt.")
        return True

    # Specific error handling
    except requests.exceptions.Timeout as e:
        error_msg = f"Download timed out for '{lang_code}' ({url}): {e}"
    except requests.exceptions.HTTPError as e:
         error_msg = f"HTTP error during download for '{lang_code}' ({url}): {e.response.status_code} {e.response.reason}"
    except requests.exceptions.ConnectionError as e:
        error_msg = f"Connection error during download for '{lang_code}' ({url}): {e}"
    except requests.exceptions.RequestException as e:
        error_msg = f"Download failed for '{lang_code}' ({url}) (Network/Request Error): {e}"
    except (IOError, ValueError, Exception) as e:
        error_msg = f"Download or file handling failed for '{lang_code}' ({url}): {e}"

    # Log the specific error
    logging.error(error_msg)

    # Clean up partially downloaded file on error
    if os.path.exists(dest_path):
        try:
            # Check size again before removing, might be valid from previous run
            if os.path.getsize(dest_path) == 0:
                os.remove(dest_path)
                logging.info(f"Removed empty/failed download artifact for '{lang_code}': {dest_path}")
            else:
                 logging.warning(f"Download failed, but existing file {dest_path} has size > 0. Leaving it intact.")
                 return True # Consider existing file usable if download fails
        except OSError as e_rem:
            logging.warning(f"Could not remove failed/empty download artifact {dest_path}: {e_rem}")

    # Append error message to the main error file for UI visibility
    try:
        with open(PROCESSING_ERROR_FILE, 'a') as f: f.write(error_msg + "\n")
    except IOError as io_err:
        logging.error(f"Failed to write download error to processing error file: {io_err}")

    return False


def download_all_videos(target_dict, download_dir):
    """Downloads all necessary video files specified in target_dict."""
    global ACTIVE_AUDIO_LANGS # Allow modification based on success/failure
    ensure_dir(download_dir)
    active_langs_on_entry = list(DOWNLOAD_TARGETS.keys()) # Use initial config keys

    if os.path.exists(ALL_DOWNLOADS_COMPLETE_MARKER):
        logging.info("Overall download marker found. Verifying individual files...")
        all_files_ok = True
        missing_or_empty_files = []
        verified_langs = [] # Track successfully verified languages
        for lang_code in active_langs_on_entry:
            details = target_dict.get(lang_code)
            if not details: continue # Should not happen if active_langs is correct
            path = os.path.join(download_dir, details['filename'])
            if os.path.exists(path) and os.path.getsize(path) > 0:
                 verified_langs.append(lang_code) # Add to verified list
            else:
                logging.warning(f"Marker exists, but file for '{lang_code}' ({path}) is missing or empty.")
                all_files_ok = False
                missing_or_empty_files.append(lang_code)

        if all_files_ok:
            logging.info("All required files verified based on target list. Skipping download phase.")
            ACTIVE_AUDIO_LANGS = verified_langs # Update global list to only verified ones
            return True
        else:
            logging.warning(f"Need to re-download or verify files for: {missing_or_empty_files}. Removing marker and starting download phase.")
            try:
                os.remove(ALL_DOWNLOADS_COMPLETE_MARKER)
            except OSError as e:
                logging.error(f"Could not remove stale download marker: {e}. Proceeding with downloads.")
            # Reset ACTIVE_AUDIO_LANGS, it will be rebuilt based on download success
            ACTIVE_AUDIO_LANGS = []
    else:
        logging.info("Overall download marker not found. Starting download phase.")
        ACTIVE_AUDIO_LANGS = [] # Reset, build based on success


    # --- Download Phase ---
    # Clear previous cumulative error file before starting fresh download attempt
    if os.path.exists(PROCESSING_ERROR_FILE):
        try:
            # Only remove if we are sure we are starting downloads, not just verifying
            if not os.path.exists(ALL_DOWNLOADS_COMPLETE_MARKER):
                 os.remove(PROCESSING_ERROR_FILE)
                 logging.info("Removed previous processing error file before download phase.")
        except OSError as e:
            logging.warning(f"Could not remove previous error file before download: {e}")

    successful_langs = []
    for lang_code, details in target_dict.items():
        url = details['url']
        filename = details['filename']
        dest_path = os.path.join(download_dir, filename)

        # Check for placeholder URLs
        if not url or "আপনার_" in url or url.strip() == "" or "এখানে_সঠিক_URL_দিন" in url:
             logging.warning(f"Skipping download for '{lang_code}': URL is missing or is a placeholder.")
             # Do not add to successful_langs, it won't be active
             continue

        logging.info(f"--- Downloading source for language: {lang_code} ---")
        if download_file(url, dest_path, lang_code):
            successful_langs.append(lang_code)
            logging.info(f"Successfully downloaded or verified file for '{lang_code}'.")
        else:
            logging.error(f"Failed to download or verify file for language '{lang_code}'. It will be excluded.")
            # Error details already logged and written to error file by download_file

    # --- Post-Download Validation ---
    ACTIVE_AUDIO_LANGS = successful_langs # Update global list based on success

    # Critical Check: Ensure the primary video ('eng') was successful
    primary_lang = 'eng'
    if primary_lang not in ACTIVE_AUDIO_LANGS:
         error_msg = f"CRITICAL FAILURE: Primary video ('{primary_lang}') could not be downloaded or verified. Cannot proceed."
         logging.error(error_msg)
         # Ensure this critical error is in the error file
         try:
             with open(PROCESSING_ERROR_FILE, 'a') as f: f.write("\n" + error_msg + "\n")
         except IOError as io_err: logging.error(f"Failed to write critical download error to file: {io_err}")
         return False # Cannot proceed

    # Check if *any* audio tracks are available
    if not ACTIVE_AUDIO_LANGS:
        # This case should be covered by the primary check above, but as a safeguard:
        error_msg = "CRITICAL FAILURE: No audio tracks (including primary) are available after download phase. Cannot proceed."
        logging.error(error_msg)
        with open(PROCESSING_ERROR_FILE, 'a') as f: f.write("\n" + error_msg + "\n")
        return False

    logging.info(f"Download phase complete. Active languages for processing: {ACTIVE_AUDIO_LANGS}")

    # Create the marker file now that downloads are verified/complete for active langs
    try:
        with open(ALL_DOWNLOADS_COMPLETE_MARKER, 'w') as f:
             f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
        logging.info(f"Created/Updated overall download marker: {ALL_DOWNLOADS_COMPLETE_MARKER}")
        return True
    except IOError as e:
        error_msg = f"Failed to write overall download marker: {e}"
        logging.error(error_msg)
        with open(PROCESSING_ERROR_FILE, 'a') as f: f.write("\n" + error_msg + "\n")
        # Proceeding might be okay, but state tracking is compromised
        return False # Treat failure to write marker as a failure state


def transcode_to_hls(download_targets, download_dir, output_base_dir, resolutions, active_langs):
    """Transcodes video with multiple audio tracks to HLS format, trimming to main video duration."""
    if os.path.exists(HLS_READY_FILE):
        logging.info("HLS ready marker file found. Skipping transcoding.")
        return True

    primary_lang = 'eng' # Define the primary language code

    # --- Get Primary Video Duration ---
    if primary_lang not in active_langs:
        # This should ideally be caught earlier, but double-check
        error_msg = f"Primary language '{primary_lang}' is not in the active list for transcoding. Cannot proceed."
        logging.error(error_msg)
        with open(PROCESSING_ERROR_FILE, 'a') as f: f.write("\n" + error_msg + "\n")
        return False

    primary_details = download_targets[primary_lang]
    primary_video_path = os.path.join(download_dir, primary_details['filename'])

    video_duration = get_video_duration(primary_video_path)
    if video_duration is None or video_duration <= 0: # Ensure duration is positive
        error_msg = f"FATAL: Could not determine a valid positive duration for primary video '{primary_video_path}'. Cannot proceed with transcoding."
        logging.error(error_msg)
        try:
            # Overwrite error file with this critical failure
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
        except IOError as io_err:
            logging.error(f"Failed to write critical duration error to file: {io_err}")
        return False
    logging.info(f"Determined primary video duration: {video_duration:.3f} seconds. Using this as the limit.")


    # --- Input Validation and Mapping Setup ---
    input_files = []
    input_paths = {} # lang_code -> path
    map_commands = []
    audio_metadata_commands = []
    output_audio_stream_index = 0 # Tracks the index of mapped *output* audio streams

    # Add primary video input and map its video and audio
    input_files.extend(['-i', primary_video_path])
    input_paths[primary_lang] = primary_video_path
    # Map first video stream from first input (0:v:0)
    map_commands.extend(['-map', '0:v:0'])
    # Map first audio stream from first input (0:a:0) - Assuming primary has audio
    # **** Important Assumption: Primary video MUST have an audio track for this simple mapping ****
    map_commands.extend(['-map', '0:a:0'])
    # Add language metadata for this first output audio stream (index 0)
    audio_metadata_commands.extend([f'-metadata:s:a:{output_audio_stream_index}', f'language={primary_lang}'])
    output_audio_stream_index += 1

    # Add other active language inputs and map their audio
    input_index = 1 # Start from 1 for subsequent -i inputs
    skipped_langs = [] # Track skipped languages during transcoding setup
    for lang_code in active_langs:
        if lang_code == primary_lang:
            continue # Already processed primary

        details = download_targets[lang_code]
        path = os.path.join(download_dir, details['filename'])
        # Basic file existence check (less robust than using ffprobe check)
        if not os.path.exists(path) or os.path.getsize(path) == 0:
             logging.warning(f"File for active language '{lang_code}' ({path}) missing or empty during transcoding setup. Skipping.")
             skipped_langs.append(lang_code)
             continue

        input_files.extend(['-i', path])
        input_paths[lang_code] = path
        # Map the first audio stream from this input (input_index : a : 0)
        # **** Important Assumption: Each additional file MUST have a valid audio track as its first audio stream ****
        map_commands.extend(['-map', f'{input_index}:a:0'])
        # Add language metadata for this output audio stream (index output_audio_stream_index)
        audio_metadata_commands.extend([f'-metadata:s:a:{output_audio_stream_index}', f'language={lang_code}'])
        input_index += 1
        output_audio_stream_index += 1

    # Check if we ended up with any audio streams to process
    if output_audio_stream_index == 0:
         error_msg = "No valid audio streams could be mapped for transcoding (not even primary). Cannot proceed."
         logging.error(error_msg)
         with open(PROCESSING_ERROR_FILE, 'a') as f: f.write("\n" + error_msg + "\n")
         return False

    # Log the final setup
    final_transcoding_langs = [lang for lang in active_langs if lang not in skipped_langs]
    logging.info(f"Attempting HLS transcoding for {len(final_transcoding_langs)} languages: {final_transcoding_langs} from {len(input_files)//2} input files.")
    logging.info(f"Output duration limited to: {video_duration:.3f}s")
    logging.info(f"Mapping commands: {map_commands}")
    logging.info(f"Audio metadata commands: {audio_metadata_commands}")

    ensure_dir(output_base_dir)
    resolution_details_for_master = []


    # --- Execute ffmpeg commands for each resolution ---
    start_time_total = time.time()
    transcoding_successful = True # Flag to track overall success

    for height, v_bitrate, a_bitrate in resolutions:
        res_output_dir = os.path.join(output_base_dir, str(height))
        ensure_dir(res_output_dir)
        segment_path_pattern = os.path.join(res_output_dir, 'segment_%03d.ts')
        absolute_playlist_path = os.path.join(res_output_dir, 'playlist.m3u8') # Resolution specific playlist

        # Base ffmpeg command
        cmd = ['ffmpeg', '-hide_banner'] # Basic command start
        cmd.extend(input_files)         # Add all -i inputs
        cmd.extend(map_commands)        # Add all -map commands

        # Add the crucial -t duration limit *before* output specifiers
        cmd.extend(['-t', f'{video_duration:.6f}']) # Use high precision

        # Video filters and encoding options
        cmd.extend(['-vf', f'scale=-2:{height}']) # Scale video
        cmd.extend(['-c:v', 'libx264', '-crf', '23', '-preset', 'veryfast']) # H.264 encoding
        cmd.extend(['-b:v', v_bitrate, '-maxrate', v_bitrate, '-bufsize', f'{int(v_bitrate[:-1])*2}k']) # Bitrate control

        # Audio encoding options (applied to all mapped output audio streams)
        cmd.extend(['-c:a', 'aac', '-ar', '48000']) # AAC codec, standard sample rate
        cmd.extend(['-b:a', a_bitrate]) # Apply the same audio bitrate to all output tracks

        # Add language metadata to each output audio stream
        cmd.extend(audio_metadata_commands)

        # HLS output options
        cmd.extend(['-f', 'hls'])
        cmd.extend(['-hls_time', '6'])          # Segment duration
        cmd.extend(['-hls_list_size', '0'])     # Keep all segments in playlist
        cmd.extend(['-hls_segment_filename', segment_path_pattern]) # Segment naming pattern
        cmd.extend(['-hls_flags', 'delete_segments+append_list']) # Overwrite existing segments cleanly
        cmd.append(absolute_playlist_path)      # Output playlist for this resolution

        logging.info(f"Running ffmpeg for {height}p (limited to {video_duration:.3f}s)...")
        # logging.debug(f"Command: {' '.join(cmd)}") # Uncomment for full command log

        try:
            # Run ffmpeg command
            result = subprocess.run(
                cmd, check=True, capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=FFMPEG_TIMEOUT
            )
            end_time_res = time.time()
            logging.info(f"ffmpeg finished successfully for {height}p in {end_time_res - start_time_res:.2f}s.")
            # logging.debug(f"FFMPEG STDERR (Success {height}p):\n{result.stderr}") # Log stderr even on success

            # Store details for master playlist generation
            audio_bitrate_numeric = int(a_bitrate[:-1]) * 1000
            total_audio_bitrate = output_audio_stream_index * audio_bitrate_numeric # Use count based on initial mapping
            video_bitrate_numeric = int(v_bitrate[:-1]) * 1000
            total_bandwidth = video_bitrate_numeric + total_audio_bitrate
            video_codec_str = "avc1.4D401F"; audio_codec_str = "mp4a.40.2" # Example codecs
            combined_codecs = f'{video_codec_str},{audio_codec_str}'

            resolution_details_for_master.append({
                'bandwidth': total_bandwidth, 'height': height,
                'playlist_path': os.path.join(str(height), 'playlist.m3u8'),
                'codecs': combined_codecs
            })

        # Handle potential errors during ffmpeg execution
        except subprocess.CalledProcessError as e:
            # Log more stderr on failure
            error_msg = (f"Transcoding failed for {height}p (rc={e.returncode}).\n"
                         f"CMD: {' '.join(e.cmd)}\n-- STDERR --\n{e.stderr}\n------------")
            logging.error(error_msg)
            with open(PROCESSING_ERROR_FILE, 'a') as f: f.write("\n" + error_msg + "\n")
            transcoding_successful = False; break
        except subprocess.TimeoutExpired as e:
            error_msg = (f"Transcoding timed out for {height}p after {FFMPEG_TIMEOUT}s.\nCMD: {' '.join(e.cmd)}\n"
                         f"-- STDERR (partial) --\n{e.stderr or 'N/A'}\n------------")
            logging.error(error_msg)
            with open(PROCESSING_ERROR_FILE, 'a') as f: f.write("\n" + error_msg + "\n")
            transcoding_successful = False; break
        except Exception as e:
            error_msg = f"Unexpected error during transcoding for {height}p: {e}"
            logging.error(error_msg, exc_info=True)
            with open(PROCESSING_ERROR_FILE, 'a') as f: f.write("\n" + error_msg + "\n")
            transcoding_successful = False; break

    # --- Create Master Playlist (only if all resolutions succeeded) ---
    if not transcoding_successful:
        logging.error("Transcoding failed for one or more resolutions. Master playlist will not be generated.")
        return False # Indicate failure

    if not resolution_details_for_master:
        error_msg = "Transcoding seemed to finish, but no resolution details were collected. Cannot generate master playlist."
        logging.error(error_msg)
        with open(PROCESSING_ERROR_FILE, 'a') as f: f.write("\n" + error_msg + "\n")
        return False

    logging.info("All resolutions transcoded successfully. Creating master playlist...")
    master_playlist_content = "#EXTM3U\n#EXT-X-VERSION:3\n"
    audio_group_id = "aac_multi_audio"

    # Generate EXT-X-MEDIA tags for each *originally active* language (might include skipped ones)
    # *** This could lead to EXT-X-MEDIA tags for tracks not actually in the segments if skipping occurred ***
    has_default_track_set = False
    for lang_code in active_langs: # Uses the list from download phase
        if lang_code in AUDIO_TRACK_DETAILS:
            track_info = AUDIO_TRACK_DETAILS[lang_code]
            is_default = track_info.get('default', False) and not has_default_track_set
            if is_default: has_default_track_set = True
            default_flag = "YES" if is_default else "NO"
            autoselect_flag = "YES"
            master_playlist_content += (
                f'#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="{audio_group_id}",'
                f'NAME="{track_info["name"]}",DEFAULT={default_flag},AUTOSELECT={autoselect_flag},'
                f'LANGUAGE="{lang_code}"\n'
            )
        else:
            logging.warning(f"Details not found for active language '{lang_code}' in AUDIO_TRACK_DETAILS. Skipping EXT-X-MEDIA tag.")

    # Ensure at least one default track (simple approach)
    if not has_default_track_set and active_langs:
        master_playlist_content = master_playlist_content.replace('DEFAULT=NO', 'DEFAULT=YES', 1)
        logging.warning("No default track set in config; marking first available track as default in playlist.")


    resolution_details_for_master.sort(key=lambda x: x['bandwidth'])
    for detail in resolution_details_for_master:
         codecs_str = f'CODECS="{detail["codecs"]}"'
         resolution_str = f'RESOLUTION={detail["height"]}x{detail["height"]}' # Placeholder width
         master_playlist_content += (
             f'#EXT-X-STREAM-INF:BANDWIDTH={detail["bandwidth"]},'
             f'{codecs_str},{resolution_str},AUDIO="{audio_group_id}"\n'
         )
         master_playlist_content += f'{detail["playlist_path"]}\n'

    master_playlist_path = os.path.join(output_base_dir, MASTER_PLAYLIST_NAME)
    try:
        with open(master_playlist_path, 'w', encoding='utf-8') as f:
            f.write(master_playlist_content)
        logging.info(f"Master playlist created successfully at {master_playlist_path}")

        with open(HLS_READY_FILE, 'w') as f:
             f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
        logging.info("HLS processing complete. Ready marker created.")
        end_time_total = time.time()
        logging.info(f"Total transcoding job time: {end_time_total - start_time_total:.2f}s")
        return True
    except IOError as e:
        error_msg = f"Failed to write master playlist or ready marker: {e}"
        logging.error(error_msg)
        with open(PROCESSING_ERROR_FILE, 'a') as f: f.write("\n" + error_msg + "\n")
        return False


def run_processing_job():
    """The main job function to download and transcode, run in a background thread."""
    global ACTIVE_AUDIO_LANGS # Allow modification by download_all_videos

    # Prevent multiple simultaneous runs using lock file
    if os.path.exists(PROCESSING_LOCK_FILE):
        logging.warning("Lock file found. Processing might be running, finished, or failed previously.")
        if os.path.exists(HLS_READY_FILE):
             logging.info("HLS already ready, existing lock file found. Exiting processing thread.")
             return
        logging.warning("HLS not ready, but lock file exists. Assuming another process is active or failed previously. Exiting thread.")
        return

    logging.info("Starting video processing job...")
    job_start_time = time.time()
    try:
        # Create lock file immediately to signal start
        ensure_dir(BASE_DIR) # Ensure base directory exists
        with open(PROCESSING_LOCK_FILE, 'w') as f:
            f.write(f'Processing started at: {time.strftime("%Y-%m-%d %H:%M:%S")}')
        logging.info(f"Created processing lock file: {PROCESSING_LOCK_FILE}")

        # Clear previous error file only if lock was just created (i.e., starting fresh)
        if os.path.exists(PROCESSING_ERROR_FILE):
            logging.warning("Removing previous error file before starting new job.")
            try: os.remove(PROCESSING_ERROR_FILE)
            except OSError as e: logging.error(f"Could not remove previous error file: {e}")

        # Ensure necessary directories exist
        ensure_dir(STATIC_DIR)
        ensure_dir(HLS_DIR)
        ensure_dir(DOWNLOAD_DIR)

        # --- Step 1: Check Prerequisites (ffmpeg, ffprobe) ---
        if not check_ffmpeg_tools():
            logging.error("ffmpeg/ffprobe check failed. Aborting processing.")
            # Error file written by check_ffmpeg_tools. Lock file remains to indicate failed state.
            return # Stop processing

        # --- Step 2: Download all source files ---
        logging.info("=== Starting Download Phase ===")
        if not download_all_videos(DOWNLOAD_TARGETS, DOWNLOAD_DIR):
            logging.error("Download step failed for one or more critical files. Aborting processing.")
            # Error file should contain details. Lock file remains.
            return # Stop processing
        logging.info("=== Download Phase Complete ===")


        # --- Step 3: Transcode to HLS ---
        logging.info("=== Starting Transcoding Phase ===")
        # ACTIVE_AUDIO_LANGS should be correctly set by download_all_videos now
        if not transcode_to_hls(DOWNLOAD_TARGETS, DOWNLOAD_DIR, HLS_DIR, RESOLUTIONS, ACTIVE_AUDIO_LANGS):
            logging.error("Transcoding step failed.")
            # Error file should contain details. Lock file remains.
            return # Stop processing
        logging.info("=== Transcoding Phase Complete ===")


        # --- Success ---
        job_end_time = time.time()
        logging.info(f"Processing job completed successfully in {job_end_time - job_start_time:.2f} seconds.")
        # Both HLS_READY_FILE and PROCESSING_LOCK_FILE exist, indicating successful completion.

    except Exception as e:
        # Catch any unexpected critical errors during the job sequence
        error_msg = f"CRITICAL UNEXPECTED ERROR in processing job: {e}"
        logging.error(error_msg, exc_info=True)
        try:
            # Ensure this critical error is logged to the file
            with open(PROCESSING_ERROR_FILE, 'a') as f: f.write(f"\n--- Critical Job Failure ---\n{error_msg}\n")
        except IOError as io_err:
            logging.error(f"Failed to write critical job error to file: {io_err}")
        # Keep lock file to indicate failure state.

    # finally:
        # Current logic keeps the lock file regardless of success or failure.
        # Status check logic in index() route uses combination of lock, ready, and error files.


# === Flask Routes ===

@app.route('/')
def index():
    """Serves the main HTML page displaying status and video player."""
    error_message = None
    is_processing = False
    is_hls_ready = os.path.exists(HLS_READY_FILE)
    processing_status_message = "Initializing..." # Default message

    # Check for errors first
    if os.path.exists(PROCESSING_ERROR_FILE):
        try:
            with open(PROCESSING_ERROR_FILE, 'r', encoding='utf-8') as f:
                error_message = f.read().strip()
            if error_message:
                 logging.warning(f"Found error file with content: {PROCESSING_ERROR_FILE}")
                 is_hls_ready = False # Cannot be ready if error occurred
                 is_processing = False # Processing stopped due to error
                 processing_status_message = "Error Occurred"
            else:
                 # Error file exists but is empty, might be leftover, ignore for now
                 error_message = None # Treat as no error
                 logging.warning(f"Found empty error file: {PROCESSING_ERROR_FILE}, ignoring.")

        except Exception as e:
            error_message = f"Could not read error file ({PROCESSING_ERROR_FILE}): {e}"
            logging.error(error_message)
            is_hls_ready = False
            is_processing = False

    # If no error, check HLS status
    if not error_message:
        if is_hls_ready:
            is_processing = False # If ready, it's not processing
            processing_status_message = "Ready"
        elif os.path.exists(PROCESSING_LOCK_FILE):
            is_processing = True # Lock exists, HLS not ready -> Processing
            processing_status_message = "Processing"
        else:
            # No lock, not ready, no error -> Should initiate processing
            is_processing = False # Not currently processing, but should start
            processing_status_message = "Idle / Starting"
            # Attempt to start the background thread if it's not running
            if not (processing_thread and processing_thread.is_alive()):
                 logging.warning("State: Idle/Not Ready. Attempting to start processing thread.")
                 start_processing_thread()
                 # Check if lock file appeared immediately after starting
                 if os.path.exists(PROCESSING_LOCK_FILE):
                      is_processing = True # Set state to processing for the UI
                      processing_status_message = "Processing (Just Started)"
                 else:
                      # Unusual state, thread started but lock not created instantly?
                      logging.warning("Attempted to start thread, but lock file did not appear immediately.")
                      # Keep is_processing false for now, UI will show Idle/Starting
            else:
                 # Thread is alive, but no lock and not ready? Inconsistent state.
                 logging.error("Inconsistent State: Processing thread alive, but no lock file and HLS not ready.")
                 error_message = "Server state is inconsistent (Thread active, no lock). Please check logs."
                 processing_status_message = "Inconsistent State"


    logging.info(f"Rendering index: Status='{processing_status_message}', HLS Ready={is_hls_ready}, Processing={is_processing}, Error Present={bool(error_message)}")
    return render_template('index.html',
                           hls_ready=is_hls_ready,
                           processing=is_processing,
                           error=error_message)

@app.route('/hls/<path:filename>')
def serve_hls_files(filename):
    """Serves HLS playlist and segment files (.m3u8, .ts) from the HLS directory structure."""
    hls_directory = HLS_DIR
    # Sanitize filename path to prevent directory traversal
    if '..' in filename or filename.startswith('/'):
        logging.warning(f"Directory traversal attempt blocked for HLS file: {filename}")
        abort(403) # Forbidden

    logging.debug(f"Request for HLS file: {filename} within {hls_directory}")
    try:
        # send_from_directory handles the full path construction and security checks
        return send_from_directory(hls_directory, filename, conditional=True) # Enable caching headers
    except FileNotFoundError:
        logging.warning(f"HLS file not found: {os.path.join(hls_directory, filename)}")
        abort(404) # Not Found
    except Exception as e:
        # Catch potential errors like permission issues
        logging.error(f"Error serving HLS file {filename}: {e}", exc_info=True)
        abort(500) # Internal Server Error


# === Application Startup & Background Thread ===
processing_thread = None

def start_processing_thread():
    """Starts the video processing in a background thread if conditions are met."""
    global processing_thread

    # Condition 1: Don't start if already running
    if processing_thread and processing_thread.is_alive():
        logging.info("Processing thread is already running.")
        return

    # Condition 2: Don't start if HLS is already successfully generated
    if os.path.exists(HLS_READY_FILE):
        logging.info("HLS already ready. No need to start processing thread.")
        return

    # Condition 3: Don't start if lock file exists (means running or failed)
    # Let the existing lock prevent a new start until manually cleared if failed.
    if os.path.exists(PROCESSING_LOCK_FILE):
         logging.warning("Lock file exists, but HLS not ready. Not starting new thread (might be running or failed previously).")
         return

    # Condition 4: Don't automatically restart if a previous error occurred (requires manual intervention)
    if os.path.exists(PROCESSING_ERROR_FILE) and os.path.getsize(PROCESSING_ERROR_FILE) > 0:
        logging.warning("Error file exists, indicating a previous failure. Manual intervention (e.g., clearing .processing.error and .processing.lock) might be required to retry. Not starting thread automatically.")
        return

    # All conditions met: Start the background processing thread
    logging.info("Starting background processing thread...")
    processing_thread = threading.Thread(target=run_processing_job, name="ProcessingThread", daemon=True)
    processing_thread.start()

def initial_start():
    """Function to initiate the first check/start of the processing thread."""
    logging.info("Application starting up. Attempting to initiate processing if needed.")
    start_processing_thread()

# --- Ensure processing starts/checks when the application boots ---
# Run this after app is defined and before app.run()
initial_start()


# === Main Execution Block ===
if __name__ == '__main__':
    # Use 0.0.0.0 to be accessible externally (like on Replit/Cloud Run)
    # Use a suitable port (e.g., 8000, 8080)
    # Set debug=False for production or when using background threads, as debug mode can interfere.
    app.run(host='0.0.0.0', port=8000, debug=False)

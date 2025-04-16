import os
import subprocess
import requests
import threading
import logging
import time
import dropbox
import shutil # For cache deletion
import schedule # For periodic cache check (install: pip install schedule)
from urllib.parse import urlparse, urlunparse
from dropbox.exceptions import ApiError, AuthError
from dropbox.files import WriteMode
from flask import Flask, render_template, send_from_directory, abort, Response, stream_with_context

# === Logging Configuration ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# === Flask App Initialization ===
app = Flask(__name__)

# === Configuration Constants ===
VIDEO_URL = "https://www.dropbox.com/scl/fi/qrzcox70ca91sb0kvs3e6/AQMjFm0PTBsLYCQ5zjKeCNSDa5bcmSWIGn_NYwUdErAVoCos5otAlo6NY8ZPSzF3Tq0epd8y_GX1mBMllyHtrCTY.mp4?rlkey=ftkmjlu69k1f32r2hw0x2jvk2&st=timxwsta&raw=1"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, 'downloads')
STATIC_DIR = os.path.join(BASE_DIR, 'static')
HLS_TRANSCODE_DIR = os.path.join(STATIC_DIR, 'hls_temp') # Temporary dir for transcoding output
HLS_CACHE_DIR = os.path.join(BASE_DIR, 'hls_cache')   # Directory for caching files from Dropbox
DOWNLOADED_FILENAME = "source_video.mp4"
MASTER_PLAYLIST_NAME = "master.m3u8"

# --- Cache Configuration ---
CACHE_MAX_AGE_SECONDS = 24 * 60 * 60  # Keep cache for 24 hours of inactivity
CACHE_CHECK_INTERVAL_SECONDS = 60 * 60 # Check cache every 1 hour
CACHE_ACCESS_MARKER = '.last_accessed' # File to track access time per resolution dir

# --- Dropbox Configuration ---
DROPBOX_ACCESS_TOKEN = "sl.u.AFoXkairOjvTRvgcYqJr5ZU4nt6lI9SqUO5exYo45vNQPSVERcpAAMXe6jMWKqmrSER1DX2XvXofYZGmD1TfZ9M9CR3MrMjkoFPXJf9TJk1GZCkvVZrM-lzCFs9p_U1_yTKIroo0zFII5tq2B2jyhekGJfkC8LGDM_SX-i_SmHm5qbGVN-vDxtcNk1iWJtUU0wi2yoPNmSHMTKMNFztttNSzleMYEi4lIdiwXzRPnDXRcbfyqviA2dbKriSjfBTROFPMf52Uodzyt8aWWM4CS9GEXhz2QsffIjewsVlZh1Mv3xt94DhgJNgI6gUKhhmkNFk5TxoLFS6qqaYa72mpzhxd3p1ZnzJXcw1sWyuflW5JseTBTp8rLsTh8raB_d9gRxM3gbpcD5jIHopJf2YwBHpnEVVog6WPJiwJCIJWtIFdTIBJ9uBwoLODbm41X2nMAQQLkXmRZtHJNrneS7eTUiNY3MYU2aXojMTWQ3_PS28bW645KB3zqHNAeqd8ZLf3OGylkkazcfK--7fWsbE5yKVeqTbpHVIyBl44KWWlY4OeKUVCpKpbjZD26M3ec_PfFeSb6CEkcIscx65VRTWDcsO8KonmJF89L1Xayo7hjzsKKL-n41tQSswt5hCkbckfZTD1jAwsDGdSSbiUzFlyQbNVY-6k9Jk2x2uqRUzOXMclfNAIaLCgtoIk9BW_cKfsl6kgqRTAPg6aDWNkqOmOe68SN28zwBGteXUFnqUnZTYWZMdU9Cgs8-KUABhqnLyUb0Xd-cVCcrapbjoJFxs_aJ4lJSm8sutBtFFzcHwbjnJi_05esuOFvGn4UQVF9xN1o32KHaeQRytuGqqup6jGcrhZoQFkZ2Fih6Ai4daCrojtjn_lNUnzBMFwlk8JJnasWONX_cvM1gjcI0qcY_lQRM0pZ58XNcgBBk5PiCCPfk5fOVi-oqODaZ_BKVBIqyZ-qn_ltd0SCRiw-yDUJ4Rg99A4shKXgOMiK4rL7UAi-RXpOmWNgAMMwOzRcTaINLKT5jfiGUAYt1b3Cued6JZhWMImq0Fbxf9AwAjgmtZpNK7CgWA46j1XJeSumM18o_MvbM0kf5W1HJTr43-NlE0nBbHVC0mZiWh7SLWM7S4qNj8WRnlEAYlJM7OaDp589jXe3GTxJhSvbYpr0PdKXRODZ9mU-ZYv7-U0i94YpEkenGJxIOOhPlRB25MdNaqLut6W_-_h6N5cpHkzzOtlT_BK0WHYDUngAtYAmHHkxz2BRhurtRxTEkZE3xwi0X5X5b5X_a0vR-1Quwfj5fsaNmy35U6oxJwB6ZsOfKIaeBlxsCiIi0NsaPCWA5_mZEH-6eoZR_wDCBKIzWuajkwwX5WrkTjirNfnGlLiduNh0ZGoiptCJ-3D1m56cXYGhmVrnBjjJPjD0S0F9AohIH9lmzq9WHui"
DROPBOX_HLS_FOLDER_PATH = "/HLS_Streams"

if not DROPBOX_ACCESS_TOKEN:
    logging.warning("DROPBOX_ACCESS_TOKEN environment variable not set.")
if DROPBOX_HLS_FOLDER_PATH and not DROPBOX_HLS_FOLDER_PATH.startswith('/'):
     corrected_path = '/' + DROPBOX_HLS_FOLDER_PATH.lstrip('/')
     logging.warning(f"Corrected DROPBOX_HLS_FOLDER_PATH to '{corrected_path}'")
     DROPBOX_HLS_FOLDER_PATH = corrected_path
elif not DROPBOX_HLS_FOLDER_PATH:
     logging.error("DROPBOX_HLS_FOLDER_PATH is not set. Using default '/HLS_Streams'.")
     DROPBOX_HLS_FOLDER_PATH = '/HLS_Streams'

RESOLUTIONS = [(360, '800k', '96k'), (480, '1400k', '128k'), (720, '2800k', '128k')]
FFMPEG_TIMEOUT = 1800

# === State Management Files ===
PROCESSING_LOCK_FILE = os.path.join(BASE_DIR, '.processing.lock')
DROPBOX_UPLOAD_COMPLETE_FILE = os.path.join(BASE_DIR, '.dropbox_upload_complete') # Marker for successful upload
DOWNLOAD_COMPLETE_FILE = os.path.join(DOWNLOAD_DIR, '.download_complete')
PROCESSING_ERROR_FILE = os.path.join(BASE_DIR, '.processing.error')
DROPBOX_BASE_URL_FILE = os.path.join(BASE_DIR, '.dropbox_base_url') # Stores the base URL for server download

# === Helper Functions ===

def ensure_dir(directory):
    """Creates a directory if it doesn't exist."""
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
            logging.info(f"Created directory: {directory}")
        except OSError as e:
            logging.error(f"Failed to create directory {directory}: {e}")
            raise

def check_ffmpeg():
    """Checks if ffmpeg is installed and accessible."""
    try:
        result = subprocess.run(['ffmpeg', '-version'], check=True, capture_output=True, text=True, timeout=10)
        logging.info("ffmpeg check successful.")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
        error_msg = f"ffmpeg not found or failed check: {e}"
        logging.error(error_msg)
        # Try to write error file only if check failed, not if error file already exists
        if not os.path.exists(PROCESSING_ERROR_FILE):
            try:
                with open(PROCESSING_ERROR_FILE, 'w') as f:
                    f.write(f"Fatal Error: ffmpeg is required but not found or not working.\nDetails: {e}")
            except IOError as io_err:
                 logging.error(f"Failed to write ffmpeg error to file: {io_err}")
        return False

def download_video(url, dest_path):
    """Downloads the video file if marker doesn't exist."""
    dest_dir = os.path.dirname(dest_path)
    ensure_dir(dest_dir)

    if os.path.exists(DOWNLOAD_COMPLETE_FILE):
        logging.info(f"Download marker file found. Checking if video file exists at {dest_path}...")
        if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
             logging.info("Video file exists. Skipping download.")
             return True
        else:
             logging.warning(f"Marker file exists but video file '{dest_path}' is missing or empty. Attempting re-download.")
             try:
                  if os.path.exists(DOWNLOAD_COMPLETE_FILE): os.remove(DOWNLOAD_COMPLETE_FILE)
             except OSError as e:
                  logging.error(f"Could not remove stale download marker: {e}")
    else:
         logging.info("Download marker not found.")

    logging.info(f"Starting download from {url} to {dest_path}...")
    try:
        if os.path.exists(dest_path):
            logging.warning(f"Removing existing file before download: {dest_path}")
            os.remove(dest_path)

        # Use requests library to download
        with requests.get(url, stream=True, timeout=300) as r: # Increased timeout to 5 minutes
            r.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)
            total_size = int(r.headers.get('content-length', 0))
            bytes_downloaded = 0
            start_time = time.time()

            if total_size > 0:
                 logging.info(f"Downloading {total_size / (1024*1024):.2f} MB...")
            else:
                 logging.info("Downloading (size unknown)...")

            with open(dest_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192*4): # Use a reasonably large chunk size
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)
                        bytes_downloaded += len(chunk)

            end_time = time.time()
            duration = end_time - start_time

            if bytes_downloaded > 0 and duration > 0:
                 download_speed = (bytes_downloaded / (1024*1024)) / duration
                 logging.info(f"Download complete ({bytes_downloaded / (1024*1024):.2f} MB) in {duration:.2f}s ({download_speed:.2f} MB/s).")
            else:
                 logging.info(f"Download complete ({bytes_downloaded / (1024*1024):.2f} MB).")

        if os.path.getsize(dest_path) == 0:
             try: os.remove(dest_path)
             except OSError as e: logging.warning(f"Could not remove empty download artifact {dest_path}: {e}")
             raise ValueError("Downloaded file is empty.")

        with open(DOWNLOAD_COMPLETE_FILE, 'w') as f:
             f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
        return True

    except requests.exceptions.Timeout as e:
        error_msg = f"Download failed (Timeout Error after 300s): {e}"
        logging.error(error_msg)
    except requests.exceptions.RequestException as e:
        error_msg = f"Download failed (Network/HTTP Error): {e}"
        logging.error(error_msg)
    except (IOError, ValueError, Exception) as e:
        error_msg = f"Download failed (File/System Error or Other): {e}"
        logging.error(error_msg)

    if not os.path.exists(PROCESSING_ERROR_FILE):
        try:
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
        except IOError as io_err: logging.error(f"Failed to write download error to file: {io_err}")
    if os.path.exists(dest_path):
        try: os.remove(dest_path)
        except OSError as e: logging.warning(f"Could not remove failed download artifact {dest_path}: {e}")
    return False

def initialize_dropbox_client():
    """Initializes and returns a Dropbox client instance."""
    if not DROPBOX_ACCESS_TOKEN:
        logging.error("Dropbox Access Token is not configured.")
        return None
    try:
        dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
        dbx.users_get_current_account() # Test authentication
        logging.info("Successfully connected to Dropbox.")
        return dbx
    except AuthError as e:
        logging.error(f"Dropbox authentication failed: {e}. Check your access token.")
        return None
    except Exception as e:
        logging.error(f"Failed to initialize Dropbox client: {e}")
        return None

def upload_to_dropbox(dbx, local_path, dropbox_path):
    """Uploads a single file to Dropbox with retries and chunking."""
    max_retries = 3
    retry_delay = 5 # seconds
    chunk_size = 100 * 1024 * 1024 # 100MB chunks

    if not os.path.exists(local_path):
        logging.error(f"Local file not found for upload: {local_path}")
        return False
    if os.path.getsize(local_path) == 0:
        logging.warning(f"Local file is empty, skipping upload: {local_path}")
        return True

    file_size = os.path.getsize(local_path)
    logging.info(f"Attempting to upload {local_path} ({file_size / (1024*1024):.2f} MB) to Dropbox path: {dropbox_path}")

    for attempt in range(max_retries):
        try:
            with open(local_path, 'rb') as f:
                if file_size <= chunk_size:
                    dbx.files_upload(f.read(), dropbox_path, mode=WriteMode('overwrite'))
                    logging.info(f"Successfully uploaded small file to {dropbox_path}")
                else:
                    upload_session_start_result = dbx.files_upload_session_start(f.read(chunk_size))
                    cursor = dropbox.files.UploadSessionCursor(session_id=upload_session_start_result.session_id, offset=f.tell())
                    commit = dropbox.files.CommitInfo(path=dropbox_path, mode=WriteMode('overwrite'))
                    logging.info(f"Started upload session {cursor.session_id}, offset {cursor.offset}")
                    while f.tell() < file_size:
                        bytes_to_read = min(chunk_size, file_size - f.tell())
                        chunk_data = f.read(bytes_to_read)
                        if not chunk_data: break
                        # Check if this is the last chunk
                        if (f.tell()) >= file_size:
                             logging.info(f"Uploading final chunk ({len(chunk_data)} bytes) for session {cursor.session_id}...")
                             dbx.files_upload_session_finish(chunk_data, cursor, commit)
                             logging.info(f"Finished upload session for large file to {dropbox_path}")
                        else:
                             logging.info(f"Uploading next chunk ({len(chunk_data)} bytes) for session {cursor.session_id}, offset {cursor.offset}...")
                             dbx.files_upload_session_append_v2(chunk_data, cursor)
                             cursor.offset = f.tell()
                             logging.info(f"Chunk uploaded, new offset: {cursor.offset}")
            return True # Upload successful

        except ApiError as e:
            logging.error(f"Dropbox API error on attempt {attempt+1} uploading {local_path} to {dropbox_path}: {e}")
        except IOError as e:
            logging.error(f"File I/O error reading {local_path} for Dropbox upload: {e}")
            return False
        except Exception as e:
             logging.error(f"Unexpected error on attempt {attempt+1} uploading {local_path} to {dropbox_path}: {e}", exc_info=True)

        if attempt < max_retries - 1:
            logging.warning(f"Retrying Dropbox upload for {local_path} in {retry_delay}s...")
            time.sleep(retry_delay)
        else:
            logging.error(f"Dropbox upload failed for {local_path} after {max_retries} attempts.")
            return False
    return False

def get_or_create_direct_shareable_link(dbx, dropbox_path):
    """Gets or creates a direct shareable link for a Dropbox file."""
    try:
        logging.info(f"Attempting to create/get shared link for: {dropbox_path}")
        try:
            settings = dropbox.sharing.SharedLinkSettings(requested_visibility=dropbox.sharing.RequestedVisibility.public)
            link_metadata = dbx.sharing_create_shared_link_with_settings(dropbox_path, settings=settings)
            link = link_metadata.url
            logging.info(f"Created new shared link: {link}")
        except ApiError as e:
            if e.error.is_shared_link_already_exists():
                logging.info(f"Shared link already exists for {dropbox_path}. Fetching existing link.")
                shared_links = dbx.sharing_list_shared_links(path=dropbox_path, direct_only=False).links
                if shared_links:
                    public_links = [l for l in shared_links if isinstance(l.link_permissions.resolved_visibility, dropbox.sharing.ResolvedVisibility.public)]
                    link = (public_links or shared_links)[0].url # Prefer public
                    logging.info(f"Found existing {'public ' if public_links else ''}shared link: {link}")
                else:
                    logging.error(f"Could not fetch existing shared link for {dropbox_path}.")
                    return None
            else:
                raise e

        # Convert to direct link format
        parsed_link = urlparse(link)
        direct_link = link # Default
        if "dropbox.com" in parsed_link.netloc:
             if parsed_link.path.startswith(('/scl/', '/s/')):
                  query = 'raw=1'
                  direct_link = urlunparse(('https', 'dl.dropboxusercontent.com', parsed_link.path, '', query, ''))
             else:
                  logging.warning(f"Unknown Dropbox path for direct link: {link}. Trying basic replacement.")
                  direct_link = link.replace("www.dropbox.com", "dl.dropboxusercontent.com").split('?')[0] + "?raw=1"
        elif "dl.dropboxusercontent.com" in parsed_link.netloc:
             if '?raw=1' not in link: direct_link = link.split('?')[0] + '?raw=1'

        logging.info(f"Direct shareable link for {dropbox_path}: {direct_link}")
        return direct_link

    except ApiError as e:
        logging.error(f"Dropbox API error getting/creating share link for {dropbox_path}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error getting/creating share link for {dropbox_path}: {e}", exc_info=True)
    return None

def upload_hls_to_dropbox(dbx, local_transcode_dir, dropbox_base_path):
    """Uploads HLS files from local_transcode_dir to Dropbox and returns base URL."""
    master_playlist_local_path = os.path.join(local_transcode_dir, MASTER_PLAYLIST_NAME)
    master_playlist_dropbox_path = f"{dropbox_base_path}/{MASTER_PLAYLIST_NAME}"
    all_uploads_successful = True

    if not os.path.exists(master_playlist_local_path):
        logging.error(f"Master playlist not found locally for upload: {master_playlist_local_path}")
        return None

    if not upload_to_dropbox(dbx, master_playlist_local_path, master_playlist_dropbox_path):
        logging.error("Failed to upload master playlist to Dropbox.")
        return None

    for item in os.listdir(local_transcode_dir):
        local_item_path = os.path.join(local_transcode_dir, item)
        if os.path.isdir(local_item_path) and item.isdigit():
            resolution = item
            dropbox_res_dir = f"{dropbox_base_path}/{resolution}"
            try:
                segment_files = [f for f in os.listdir(local_item_path) if os.path.isfile(os.path.join(local_item_path, f))]
            except OSError as e:
                 logging.error(f"Could not list files in {local_item_path}: {e}")
                 all_uploads_successful = False; continue
            for filename in segment_files:
                local_file = os.path.join(local_item_path, filename)
                dropbox_file_path = f"{dropbox_res_dir}/{filename}"
                if not upload_to_dropbox(dbx, local_file, dropbox_file_path):
                    logging.error(f"Failed to upload HLS file {local_file}.")
                    all_uploads_successful = False

    if not all_uploads_successful:
        logging.error("One or more HLS files failed to upload to Dropbox.")
        # Decide if partial upload is acceptable or should fail
        # return None # Fail if any part fails

    logging.info("Finished uploading HLS files to Dropbox.")

    master_playlist_url = get_or_create_direct_shareable_link(dbx, master_playlist_dropbox_path)
    if not master_playlist_url:
         logging.error("Failed to get shareable link for the master playlist.")
         return None

    base_url = master_playlist_url.rsplit('/', 1)[0] + '/'
    try:
        with open(DROPBOX_BASE_URL_FILE, 'w') as f: f.write(base_url)
        logging.info(f"Saved Dropbox base URL: {base_url}")
        with open(DROPBOX_UPLOAD_COMPLETE_FILE, 'w') as f: f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
    except IOError as e:
        logging.error(f"Failed to save Dropbox base URL to file: {e}")
        return None

    return base_url

def transcode_to_hls(input_path, output_base_dir, resolutions):
    """Transcodes video to HLS format locally."""
    if not os.path.exists(input_path) or os.path.getsize(input_path) == 0:
        error_msg = f"Input video file not found or is empty: {input_path}"; logging.error(error_msg)
        return False

    logging.info(f"Starting HLS transcoding from {input_path} into {output_base_dir}...")
    ensure_dir(output_base_dir)
    master_playlist_content = "#EXTM3U\n#EXT-X-VERSION:3\n"
    ffmpeg_commands = []
    resolution_details_for_master = []

    try: # Clean output dir
        if os.path.exists(output_base_dir): shutil.rmtree(output_base_dir)
        os.makedirs(output_base_dir)
    except Exception as e:
         logging.error(f"Failed to clean/create transcoding output directory {output_base_dir}: {e}"); return False

    for height, v_bitrate, a_bitrate in resolutions:
        res_output_dir = os.path.join(output_base_dir, str(height)); ensure_dir(res_output_dir)
        relative_playlist_path = f"{height}/playlist.m3u8"
        segment_path_pattern = os.path.join(res_output_dir, 'segment%03d.ts')
        absolute_playlist_path = os.path.join(res_output_dir, 'playlist.m3u8')
        cmd = ['ffmpeg', '-y', '-i', input_path, '-vf', f'scale=-2:{height}', '-c:v', 'libx264', '-crf', '23', '-preset', 'fast', '-b:v', v_bitrate, '-maxrate', v_bitrate, '-bufsize', f'{int(v_bitrate[:-1])*2}k', '-c:a', 'aac', '-ar', '48000', '-b:a', a_bitrate, '-f', 'hls', '-hls_time', '6', '-hls_list_size', '0', '-hls_segment_filename', segment_path_pattern, '-hls_flags', 'delete_segments+append_list', '-start_number', '0', absolute_playlist_path]
        ffmpeg_commands.append({'cmd': cmd, 'height': height})
        bandwidth = int(v_bitrate[:-1]) * 1000 + int(a_bitrate[:-1]) * 1000
        resolution_details_for_master.append({'bandwidth': bandwidth, 'height': height, 'playlist_path': relative_playlist_path})

    start_time_total = time.time(); transcoding_successful = True; error_msg = "Transcoding failed."
    for item in ffmpeg_commands:
        cmd, height = item['cmd'], item['height']
        logging.info(f"Running ffmpeg for {height}p..."); start_time_res = time.time()
        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=FFMPEG_TIMEOUT)
            logging.info(f"ffmpeg OK for {height}p in {time.time() - start_time_res:.2f}s.")
        except subprocess.CalledProcessError as e:
            error_msg = f"Transcoding failed {height}p (code {e.returncode}).\nSTDERR: ...{e.stderr[-1000:]}"; logging.error(error_msg); transcoding_successful = False; break
        except subprocess.TimeoutExpired as e:
            stderr_output = e.stderr.decode('utf-8', errors='ignore') if e.stderr else "N/A"
            error_msg = f"Transcoding timed out {height}p ({FFMPEG_TIMEOUT}s).\nSTDERR: ...{stderr_output[-1000:]}"; logging.error(error_msg); transcoding_successful = False; break
        except Exception as e:
            error_msg = f"Unexpected error transcoding {height}p: {e}"; logging.error(error_msg, exc_info=True); transcoding_successful = False; break

    if not transcoding_successful:
         if not os.path.exists(PROCESSING_ERROR_FILE):
              try:
                   with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
              except IOError as io_err: logging.error(f"Failed to write ffmpeg error file: {io_err}")
         return False

    for detail in resolution_details_for_master:
        master_playlist_content += f'#EXT-X-STREAM-INF:BANDWIDTH={detail["bandwidth"]},RESOLUTION=x{detail["height"]}\n{detail["playlist_path"]}\n'
    master_playlist_path = os.path.join(output_base_dir, MASTER_PLAYLIST_NAME)
    try:
        with open(master_playlist_path, 'w') as f: f.write(master_playlist_content)
        logging.info(f"Local master playlist created. Total transcode time: {time.time() - start_time_total:.2f}s")
        return True
    except IOError as e:
        error_msg = f"Failed to write local master playlist: {e}"; logging.error(error_msg)
        if not os.path.exists(PROCESSING_ERROR_FILE):
             try:
                  with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
             except IOError as io_err: logging.error(f"Failed to write playlist error file: {io_err}")
        return False

def run_processing_job():
    """Main background job: Download -> Transcode -> Upload."""
    # --- Check Locks and Completion ---
    if os.path.exists(PROCESSING_LOCK_FILE):
        if os.path.exists(DROPBOX_UPLOAD_COMPLETE_FILE): logging.info("Processing already completed."); return
        if os.path.exists(PROCESSING_ERROR_FILE): logging.warning("Previous run failed. Manual cleanup needed."); return
        try: # Stale lock check
              lock_age = time.time() - os.path.getmtime(PROCESSING_LOCK_FILE); max_lock_age = 7200
              if lock_age <= max_lock_age: logging.warning(f"Active lock file found (age: {lock_age:.0f}s). Exiting."); return
              logging.warning(f"Stale lock file found (age: {lock_age:.0f}s). Removing and proceeding.")
              os.remove(PROCESSING_LOCK_FILE)
        except OSError as e: logging.error(f"Error checking/removing lock file: {e}."); return

    logging.info("Starting video processing job...")
    lock_acquired = False
    try:
        ensure_dir(BASE_DIR)
        with open(PROCESSING_LOCK_FILE, 'w') as f: f.write(time.strftime("%Y-%m-%d %H:%M:%S")); lock_acquired = True
        logging.info("Created processing lock file.")

        # --- Cleanup ---
        files_to_clean = [PROCESSING_ERROR_FILE, DROPBOX_BASE_URL_FILE, DROPBOX_UPLOAD_COMPLETE_FILE, DOWNLOAD_COMPLETE_FILE]
        for file_path in files_to_clean:
             if os.path.exists(file_path):
                  try: os.remove(file_path); logging.info(f"Removed previous state file: {file_path}")
                  except OSError as e: logging.error(f"Could not remove {file_path}: {e}")
        for dir_path in [HLS_TRANSCODE_DIR, HLS_CACHE_DIR]:
             if os.path.exists(dir_path):
                  try: shutil.rmtree(dir_path); logging.info(f"Removed previous directory: {dir_path}")
                  except Exception as e: logging.error(f"Could not remove {dir_path}: {e}")
        ensure_dir(STATIC_DIR); ensure_dir(HLS_TRANSCODE_DIR); ensure_dir(HLS_CACHE_DIR); ensure_dir(DOWNLOAD_DIR)
        # --- End Cleanup ---

        if not check_ffmpeg(): return
        dbx = initialize_dropbox_client()
        if not dbx:
             if not os.path.exists(PROCESSING_ERROR_FILE):
                  with open(PROCESSING_ERROR_FILE, 'w') as f: f.write("Failed to initialize Dropbox client.")
             return
        download_path = os.path.join(DOWNLOAD_DIR, DOWNLOADED_FILENAME)
        if not download_video(VIDEO_URL, download_path): return
        if not transcode_to_hls(download_path, HLS_TRANSCODE_DIR, RESOLUTIONS): return
        base_url = upload_hls_to_dropbox(dbx, HLS_TRANSCODE_DIR, DROPBOX_HLS_FOLDER_PATH)
        if not base_url:
             if not os.path.exists(PROCESSING_ERROR_FILE):
                  with open(PROCESSING_ERROR_FILE, 'w') as f: f.write("Dropbox upload step failed.")
             return

        logging.info(f"Processing job completed successfully. Dropbox Base URL: {base_url}")
        # Optional: Cleanup local transcoded files
        try: shutil.rmtree(HLS_TRANSCODE_DIR); logging.info("Removed local transcoding directory.")
        except Exception as e: logging.warning(f"Could not remove local transcoding directory: {e}")

    except Exception as e:
        error_msg = f"Critical error in processing job: {e}"; logging.error(error_msg, exc_info=True)
        if not os.path.exists(PROCESSING_ERROR_FILE):
             try:
                  with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
             except IOError as io_err: logging.error(f"Failed to write critical error file: {io_err}")
    # finally:
         # Keep lock file on success, remove on failure? Requires careful check if failure was due to external lock.
         # Current logic: Keep lock file. Requires manual delete of lock+error to retry failed job.

# === Cache Management ===

def touch_cache_timestamp(cache_file_path):
    """Updates the .last_accessed timestamp file."""
    try:
        cache_dir = os.path.dirname(cache_file_path)
        if not cache_dir or cache_dir == HLS_CACHE_DIR: return # Avoid marker in root
        marker_path = os.path.join(cache_dir, CACHE_ACCESS_MARKER)
        ensure_dir(cache_dir)
        with open(marker_path, 'a'): os.utime(marker_path, None)
    except Exception as e: logging.warning(f"Failed to update cache timestamp for {cache_file_path}: {e}")

def download_from_dropbox_to_cache(relative_path):
    """Downloads a file from Dropbox to the local cache if not locked."""
    cache_path = os.path.join(HLS_CACHE_DIR, relative_path)
    cache_dir = os.path.dirname(cache_path); ensure_dir(cache_dir)
    lock_path = cache_path + '.downloading'

    if os.path.exists(lock_path): # Check if download is already in progress
        logging.warning(f"Download lock exists for {relative_path}. Waiting briefly.")
        time.sleep(0.5 + (os.getpid() % 10) * 0.1) # Basic jittered wait
        if os.path.exists(cache_path) and os.path.getsize(cache_path) > 0: return cache_path # Now cached
        logging.error(f"Download lock still present for {relative_path}, file not cached. Aborting redundant download.")
        return None

    try: # Attempt to acquire lock
        with open(lock_path, 'w') as f: f.write(str(os.getpid()))
        logging.debug(f"Acquired download lock for {relative_path}")

        if not os.path.exists(DROPBOX_BASE_URL_FILE): logging.error("Base URL file missing."); return None
        with open(DROPBOX_BASE_URL_FILE, 'r') as f: base_url = f.read().strip()
        if not base_url: logging.error("Base URL is empty."); return None

        dropbox_url = base_url.rstrip('/') + '/' + relative_path.lstrip('/')
        if relative_path.endswith('.m3u8') and '?raw=1' not in dropbox_url: dropbox_url += '?raw=1'
        logging.info(f"Cache miss: Downloading {dropbox_url} -> {cache_path}")

        with requests.get(dropbox_url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(cache_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): f.write(chunk)

        if os.path.exists(cache_path) and os.path.getsize(cache_path) > 0:
            logging.info(f"Cached successfully: {cache_path}")
            touch_cache_timestamp(cache_path)
            return cache_path
        else:
            logging.warning(f"Downloaded file is empty: {cache_path}.");
            if os.path.exists(cache_path): os.remove(cache_path)
            return None

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed download {relative_path} from {dropbox_url}: {e}")
    except Exception as e: logging.error(f"Error downloading {relative_path} to cache: {e}", exc_info=True)
    finally: # Release lock
        if os.path.exists(lock_path):
             try: os.remove(lock_path); logging.debug(f"Released download lock for {relative_path}")
             except OSError: pass
    # Cleanup empty file on error
    if os.path.exists(cache_path) and os.path.getsize(cache_path) == 0:
        try: os.remove(cache_path)
        except OSError: pass
    return None

def run_cache_eviction():
    """Scans cache directory and removes expired items."""
    logging.info("Running cache eviction check...")
    now = time.time()
    try:
        if not os.path.exists(HLS_CACHE_DIR): return
        for item_name in os.listdir(HLS_CACHE_DIR):
            item_path = os.path.join(HLS_CACHE_DIR, item_name)
            if os.path.isdir(item_path): # Focus on resolution directories
                marker_path = os.path.join(item_path, CACHE_ACCESS_MARKER)
                dir_last_modified = os.path.getmtime(item_path) # Check dir modified time as well
                marker_last_access = 0
                if os.path.exists(marker_path):
                    try: marker_last_access = os.path.getmtime(marker_path)
                    except OSError: pass # Handle case where marker exists but cannot be read

                # Use the most recent timestamp (marker or directory itself)
                last_activity_time = max(dir_last_modified, marker_last_access)
                age = now - last_activity_time

                if age > CACHE_MAX_AGE_SECONDS:
                    logging.info(f"Cache directory {item_path} expired (age: {age:.0f}s > {CACHE_MAX_AGE_SECONDS}s). Removing.")
                    try: shutil.rmtree(item_path)
                    except Exception as e: logging.error(f"Failed to remove expired cache dir {item_path}: {e}")
                # else: logging.debug(f"Cache dir {item_path} fresh (age: {age:.0f}s).")

    except Exception as e: logging.error(f"Error during cache eviction: {e}", exc_info=True)
    logging.info("Cache eviction check finished.")

def cache_eviction_scheduler():
    """Runs the cache eviction function periodically."""
    logging.info(f"Starting cache eviction scheduler (Interval: {CACHE_CHECK_INTERVAL_SECONDS}s)")
    schedule.every(CACHE_CHECK_INTERVAL_SECONDS).seconds.do(run_cache_eviction)
    while True:
        try:
            schedule.run_pending()
            time.sleep(60) # Check schedule every 60 seconds
        except Exception as e:
            logging.error(f"Error in cache eviction scheduler loop: {e}", exc_info=True)
            time.sleep(300) # Wait longer after an error

# === Flask Routes ===

@app.route('/')
def index():
    """Serves the main HTML page."""
    error_message = None; is_processing = False; is_ready_for_streaming = False
    stream_url = None; status_detail = "Initializing..."

    if os.path.exists(PROCESSING_ERROR_FILE):
        try:
            with open(PROCESSING_ERROR_FILE, 'r') as f: error_message = f.read()
            status_detail = "প্রসেসিং ব্যর্থ হয়েছে"
        except Exception as e: error_message = f"Error file read error: {e}"; status_detail = "ত্রুটি ফাইল পড়তে সমস্যা"
    elif os.path.exists(DROPBOX_UPLOAD_COMPLETE_FILE):
        is_ready_for_streaming = True; stream_url = f"/stream/{MASTER_PLAYLIST_NAME}"
        status_detail = "স্ট্রিমিংয়ের জন্য প্রস্তুত (সার্ভার ক্যাশ)"
    elif os.path.exists(PROCESSING_LOCK_FILE):
        is_processing = True; status_detail = "প্রসেসিং চলছে..."
    else: # Assume needs to start
        status_detail = "প্রসেসিং শুরু হচ্ছে..."
        if not (processing_thread and processing_thread.is_alive()): start_processing_thread()
        is_processing = True # Show processing while it starts

    logging.info(f"Render index: ready={is_ready_for_streaming}, processing={is_processing}, error={bool(error_message)}")
    return render_template('index.html', hls_ready=is_ready_for_streaming, processing=is_processing, error=error_message, master_playlist_stream_url=stream_url, status_detail=status_detail)

@app.route('/stream/<path:filename>')
def stream_hls_files(filename):
    """Serves HLS files, caching from Dropbox if necessary."""
    if '..' in filename or filename.startswith('/'): abort(403)
    cache_path = os.path.join(HLS_CACHE_DIR, filename)

    if os.path.exists(cache_path) and os.path.getsize(cache_path) > 0:
        logging.debug(f"Cache hit: {filename}")
        touch_cache_timestamp(cache_path)
        response = send_from_directory(HLS_CACHE_DIR, filename, conditional=True)
    else:
        logging.info(f"Cache miss: {filename}")
        downloaded_path = download_from_dropbox_to_cache(filename)
        if downloaded_path:
            logging.info(f"Serving newly cached: {filename}")
            response = send_from_directory(HLS_CACHE_DIR, filename, conditional=True)
        else:
            logging.error(f"Failed to cache/serve: {filename}")
            abort(404)

    # Add headers to prevent client-side caching issues if needed
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


# === Application Startup & Background Threads ===
processing_thread = None
cache_eviction_thread = None

# Define thread start functions (same as before)
def start_processing_thread():
    global processing_thread
    if processing_thread and processing_thread.is_alive(): return
    if os.path.exists(DROPBOX_UPLOAD_COMPLETE_FILE): return
    if os.path.exists(PROCESSING_ERROR_FILE): return
    if os.path.exists(PROCESSING_LOCK_FILE):
         try: lock_age = time.time() - os.path.getmtime(PROCESSING_LOCK_FILE); max_lock_age = 7200
              if lock_age <= max_lock_age: logging.warning(f"Active lock file found. Exiting."); return
              logging.warning(f"Stale lock file found. Removing."); os.remove(PROCESSING_LOCK_FILE)
         except OSError as e: logging.error(f"Lock file error: {e}."); return
    if not DROPBOX_ACCESS_TOKEN: logging.error("Cannot start: DROPBOX_ACCESS_TOKEN missing."); return
    logging.info("Starting background processing thread...")
    processing_thread = threading.Thread(target=run_processing_job, name="ProcessingThread", daemon=True); processing_thread.start()

def start_cache_eviction_thread():
    global cache_eviction_thread
    if cache_eviction_thread and cache_eviction_thread.is_alive(): return
    logging.info("Starting cache eviction scheduler thread...")
    cache_eviction_thread = threading.Thread(target=cache_eviction_scheduler, name="CacheEvictionThread", daemon=True); cache_eviction_thread.start()

# --- Initialize Directories and Start Threads on Module Load ---
# This block runs when the module is first imported by Gunicorn worker
try:
    logging.info("Initializing application...")
    ensure_dir(DOWNLOAD_DIR)
    ensure_dir(STATIC_DIR)
    ensure_dir(HLS_TRANSCODE_DIR)
    ensure_dir(HLS_CACHE_DIR)
    start_processing_thread()
    start_cache_eviction_thread()
    logging.info("Initialization routines called.")
except Exception as e:
    logging.critical(f"CRITICAL ERROR during initialization: {e}", exc_info=True)
    # Optionally write to error file if possible
    if not os.path.exists(PROCESSING_ERROR_FILE):
        try:
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(f"CRITICAL INIT ERROR: {e}")
        except IOError: pass
    # Re-raise the exception to potentially stop the worker from starting badly configured
    raise
# --- End Initialization ---


# === Main Execution Block (for direct run `python app.py` only) ===
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    logging.info(f"Starting Flask development server on http://0.0.0.0:{port}")
    # Use `debug=False` for stability, especially with threads.
    app.run(host='0.0.0.0', port=port, debug=False)

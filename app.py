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

# === Helper Functions (ensure_dir, check_ffmpeg, download_video, initialize_dropbox_client, upload_to_dropbox, get_or_create_direct_shareable_link) ===
# These functions remain largely the same as in the previous version.
# Make sure `upload_hls_to_dropbox` uploads from HLS_TRANSCODE_DIR
# and `transcode_to_hls` outputs to HLS_TRANSCODE_DIR.

# (Assuming previous helper functions are here)
# ... ensure_dir, check_ffmpeg, download_video ...
# ... initialize_dropbox_client, upload_to_dropbox, get_or_create_direct_shareable_link ...

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
    # ... (same as before) ...
    try:
        result = subprocess.run(['ffmpeg', '-version'], check=True, capture_output=True, text=True, timeout=10)
        logging.info("ffmpeg check successful.")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
        error_msg = f"ffmpeg not found or failed check: {e}"
        logging.error(error_msg)
        try:
            with open(PROCESSING_ERROR_FILE, 'w') as f:
                f.write(f"Fatal Error: ffmpeg is required but not found or not working.\nDetails: {e}")
        except IOError as io_err:
             logging.error(f"Failed to write ffmpeg error to file: {io_err}")
        return False

def download_video(url, dest_path):
    # ... (same as before, ensure error file is written on failure) ...
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

        with requests.get(url, stream=True, timeout=300) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))
            bytes_downloaded = 0
            start_time = time.time()
            if total_size > 0:
                 logging.info(f"Downloading {total_size / (1024*1024):.2f} MB...")
            else:
                 logging.info("Downloading (size unknown)...")

            with open(dest_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192*4):
                    f.write(chunk)
                    bytes_downloaded += len(chunk)

            end_time = time.time()
            if bytes_downloaded > 0 and end_time > start_time:
                 download_speed = (bytes_downloaded / (1024*1024)) / (end_time - start_time + 1e-6)
                 logging.info(f"Download complete ({bytes_downloaded / (1024*1024):.2f} MB) in {end_time - start_time:.2f}s ({download_speed:.2f} MB/s).")
            else:
                 logging.info(f"Download complete ({bytes_downloaded / (1024*1024):.2f} MB).")


        if os.path.getsize(dest_path) == 0:
             try: os.remove(dest_path)
             except OSError as e: logging.warning(f"Could not remove empty download artifact {dest_path}: {e}")
             raise ValueError("Downloaded file is empty.")
        with open(DOWNLOAD_COMPLETE_FILE, 'w') as f:
             f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
        return True

    except requests.exceptions.RequestException as e:
        error_msg = f"Download failed (Network/HTTP Error): {e}"
        logging.error(error_msg)
    except (IOError, ValueError, Exception) as e:
        error_msg = f"Download failed (File/System Error or Other): {e}"
        logging.error(error_msg)

    try:
        with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
    except IOError as io_err: logging.error(f"Failed to write download error to file: {io_err}")
    if os.path.exists(dest_path):
        try: os.remove(dest_path)
        except OSError as e: logging.warning(f"Could not remove failed download artifact {dest_path}: {e}")
    return False


def initialize_dropbox_client():
    # ... (same as before) ...
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
    # ... (same improved version as before with chunking) ...
    max_retries = 3
    retry_delay = 5 # seconds
    chunk_size = 100 * 1024 * 1024 # Upload in 100MB chunks for larger files

    # Check if local file exists before attempting upload
    if not os.path.exists(local_path):
        logging.error(f"Local file not found for upload: {local_path}")
        return False
    if os.path.getsize(local_path) == 0:
        logging.warning(f"Local file is empty, skipping upload: {local_path}")
        return True # Treat empty file upload as 'success' to not block the process

    file_size = os.path.getsize(local_path)
    logging.info(f"Attempting to upload {local_path} ({file_size / (1024*1024):.2f} MB) to Dropbox path: {dropbox_path}")

    for attempt in range(max_retries):
        try:
            with open(local_path, 'rb') as f:
                if file_size <= chunk_size:
                    # Upload small files directly
                    dbx.files_upload(f.read(), dropbox_path, mode=WriteMode('overwrite'))
                    logging.info(f"Successfully uploaded small file to {dropbox_path}")
                else:
                    # Use upload session for large files
                    upload_session_start_result = dbx.files_upload_session_start(f.read(chunk_size))
                    cursor = dropbox.files.UploadSessionCursor(session_id=upload_session_start_result.session_id,
                                                               offset=f.tell())
                    commit = dropbox.files.CommitInfo(path=dropbox_path, mode=WriteMode('overwrite'))
                    logging.info(f"Started upload session for large file: {cursor.session_id}, offset: {cursor.offset}")

                    while f.tell() < file_size:
                        # Calculate remaining bytes and chunk size to read
                        bytes_to_read = min(chunk_size, file_size - f.tell())
                        chunk_data = f.read(bytes_to_read)

                        if not chunk_data: # Should not happen if f.tell() < file_size, but safety check
                             logging.warning(f"Read empty chunk unexpectedly for session {cursor.session_id}, offset: {cursor.offset}. Breaking loop.")
                             break

                        if (file_size - f.tell()) <= 0: # Check if this is the last chunk based on f.tell() AFTER read
                            logging.info(f"Uploading final chunk ({len(chunk_data)} bytes) for session {cursor.session_id}...")
                            dbx.files_upload_session_finish(chunk_data, cursor, commit)
                            logging.info(f"Finished upload session for large file to {dropbox_path}")
                        else:
                            logging.info(f"Uploading next chunk ({len(chunk_data)} bytes) for session {cursor.session_id}, offset: {cursor.offset}...")
                            dbx.files_upload_session_append_v2(chunk_data, cursor)
                            cursor.offset = f.tell() # Update offset based on current position in file
                            logging.info(f"Chunk uploaded, new offset: {cursor.offset}")

            return True # Upload successful

        except ApiError as e:
            logging.error(f"Dropbox API error on attempt {attempt+1} uploading {local_path} to {dropbox_path}: {e}")
            # Specific check for file locking or access issues
            if 'conflict' in str(e) and 'file_lock' in str(e):
                logging.error("File lock conflict during upload. Another process might be accessing the file.")
        except IOError as e:
            logging.error(f"File I/O error reading {local_path} for Dropbox upload: {e}")
            return False # Don't retry IO errors
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
    # ... (same improved version as before) ...
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
                    if public_links:
                         link = public_links[0].url
                         logging.info(f"Found existing public shared link: {link}")
                    elif shared_links:
                         link = shared_links[0].url
                         logging.warning(f"Found existing non-public shared link: {link}. Streaming might fail if not public.")
                    else:
                         logging.error(f"Shared link reported to exist, but none found for {dropbox_path}.")
                         return None
                else:
                    logging.error(f"Could not fetch existing shared link for {dropbox_path} after 'already_exists' error.")
                    return None
            else:
                raise e

        # Convert to direct link format (prefer dl.dropboxusercontent.com)
        parsed_link = urlparse(link)
        if "dropbox.com" in parsed_link.netloc:
             # Handle /scl/ links (newer share links)
             if parsed_link.path.startswith('/scl/'):
                  # Add raw=1, remove other query params like dl=0, st=..., rlkey=...
                  direct_path = parsed_link.path
                  # Construct new URL with dl.dropboxusercontent.com and raw=1 query
                  direct_link = urlunparse(('https', 'dl.dropboxusercontent.com', direct_path, '', 'raw=1', ''))
             # Handle /s/ links (older share links)
             elif parsed_link.path.startswith('/s/'):
                  direct_path = parsed_link.path
                  direct_link = urlunparse(('https', 'dl.dropboxusercontent.com', direct_path, '', 'raw=1', ''))
             else: # Fallback for unknown dropbox.com paths
                  logging.warning(f"Unknown Dropbox path structure for direct link conversion: {link}. Trying basic replacement.")
                  direct_link = link.replace("www.dropbox.com", "dl.dropboxusercontent.com").split('?')[0] + "?raw=1"

        elif "dl.dropboxusercontent.com" in parsed_link.netloc:
             # Already a direct link domain, ensure query is clean (e.g., remove dl=0)
             # For HLS, '?raw=1' might still be needed for playlists from /scl/, but often not for segments.
             # Let's keep it simple and assume it's okay as is, or add raw=1 if missing.
             if '?raw=1' not in link:
                  direct_link = link.split('?')[0] + '?raw=1' # Ensure raw=1 for consistency
             else:
                  direct_link = link
        else:
             # Unknown domain
             logging.warning(f"Link domain not recognized for direct conversion: {link}. Using as is.")
             direct_link = link

        logging.info(f"Direct shareable link for {dropbox_path}: {direct_link}")
        return direct_link

    except ApiError as e:
        logging.error(f"Dropbox API error getting/creating share link for {dropbox_path}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error getting/creating share link for {dropbox_path}: {e}", exc_info=True)
    return None

def upload_hls_to_dropbox(dbx, local_transcode_dir, dropbox_base_path):
    """Uploads all HLS files from local_transcode_dir to Dropbox."""
    # ... (This function logic remains mostly the same, but reads from local_transcode_dir)
    master_playlist_local_path = os.path.join(local_transcode_dir, MASTER_PLAYLIST_NAME)
    master_playlist_dropbox_path = f"{dropbox_base_path}/{MASTER_PLAYLIST_NAME}"
    all_uploads_successful = True

    if not os.path.exists(master_playlist_local_path):
        logging.error(f"Master playlist not found locally for upload: {master_playlist_local_path}")
        return None # Cannot proceed without master playlist

    if not upload_to_dropbox(dbx, master_playlist_local_path, master_playlist_dropbox_path):
        logging.error("Failed to upload master playlist to Dropbox. Aborting HLS upload.")
        return None

    for item in os.listdir(local_transcode_dir):
        local_item_path = os.path.join(local_transcode_dir, item)
        if os.path.isdir(local_item_path) and item.isdigit():
            resolution = item
            dropbox_res_dir = f"{dropbox_base_path}/{resolution}"
            logging.info(f"Uploading contents of {local_item_path} to Dropbox folder {dropbox_res_dir}")
            try:
                segment_files = [f for f in os.listdir(local_item_path) if os.path.isfile(os.path.join(local_item_path, f))]
            except OSError as e:
                 logging.error(f"Could not list files in {local_item_path}: {e}")
                 all_uploads_successful = False
                 continue

            for filename in segment_files:
                local_file = os.path.join(local_item_path, filename)
                dropbox_file_path = f"{dropbox_res_dir}/{filename}"
                if not upload_to_dropbox(dbx, local_file, dropbox_file_path):
                    logging.error(f"Failed to upload segment/playlist {local_file}. Upload may be incomplete.")
                    all_uploads_successful = False
                    # Decide if you want to abort completely here or just log and continue

    if not all_uploads_successful:
        logging.error("One or more HLS files failed to upload to Dropbox.")
        # return None # Fail completely if any part fails

    logging.info("Finished uploading HLS files to Dropbox.")

    # Get the direct shareable link for the master playlist
    master_playlist_url = get_or_create_direct_shareable_link(dbx, master_playlist_dropbox_path)
    if not master_playlist_url:
         logging.error("Failed to get shareable link for the master playlist.")
         return None

    # Store the BASE URL (without master.m3u8) locally for server use
    base_url = master_playlist_url.rsplit('/', 1)[0] + '/'
    try:
        with open(DROPBOX_BASE_URL_FILE, 'w') as f:
            f.write(base_url)
        logging.info(f"Saved Dropbox base URL to {DROPBOX_BASE_URL_FILE}: {base_url}")
        # Also create the upload complete marker
        with open(DROPBOX_UPLOAD_COMPLETE_FILE, 'w') as f:
             f.write(time.strftime("%Y-%m-%d %H:%M:%S"))

    except IOError as e:
        logging.error(f"Failed to save Dropbox base URL to file: {e}")
        return None # Fail if we can't save the base URL needed for caching

    return base_url # Return base URL on success

# === Transcoding Function (Outputs to HLS_TRANSCODE_DIR) ===
def transcode_to_hls(input_path, output_base_dir, resolutions):
    """Transcodes video to HLS format locally."""
    # Note: This function now ONLY does local transcoding. Upload is separate.
    if not os.path.exists(input_path) or os.path.getsize(input_path) == 0:
        error_msg = f"Input video file not found or is empty: {input_path}"
        logging.error(error_msg)
        return False # Use boolean to indicate success/failure of transcoding

    logging.info(f"Starting HLS transcoding from {input_path} into {output_base_dir}...")
    ensure_dir(output_base_dir) # Ensure the base output dir exists
    master_playlist_content = "#EXTM3U\n#EXT-X-VERSION:3\n"
    ffmpeg_commands = []
    resolution_details_for_master = []

    # Clean the output directory before transcoding
    try:
        if os.path.exists(output_base_dir):
            shutil.rmtree(output_base_dir)
            logging.info(f"Cleaned previous transcoding output directory: {output_base_dir}")
        os.makedirs(output_base_dir) # Recreate it empty
    except Exception as e:
         logging.error(f"Failed to clean/create transcoding output directory {output_base_dir}: {e}")
         return False

    # Prepare ffmpeg commands
    for height, v_bitrate, a_bitrate in resolutions:
        res_output_dir = os.path.join(output_base_dir, str(height))
        ensure_dir(res_output_dir)
        relative_playlist_path = f"{height}/playlist.m3u8"
        segment_path_pattern = os.path.join(res_output_dir, 'segment%03d.ts')
        absolute_playlist_path = os.path.join(res_output_dir, 'playlist.m3u8')
        cmd = [
            'ffmpeg', '-y', '-i', input_path,
            '-vf', f'scale=-2:{height}', '-c:v', 'libx264', '-crf', '23', '-preset', 'fast',
            '-b:v', v_bitrate, '-maxrate', v_bitrate, '-bufsize', f'{int(v_bitrate[:-1])*2}k',
            '-c:a', 'aac', '-ar', '48000', '-b:a', a_bitrate,
            '-f', 'hls', '-hls_time', '6', '-hls_list_size', '0',
            '-hls_segment_filename', segment_path_pattern,
            '-hls_flags', 'delete_segments+append_list', '-start_number', '0',
            absolute_playlist_path
        ]
        ffmpeg_commands.append({'cmd': cmd, 'height': height})
        bandwidth = int(v_bitrate[:-1]) * 1000 + int(a_bitrate[:-1]) * 1000
        resolution_details_for_master.append({
            'bandwidth': bandwidth, 'height': height, 'playlist_path': relative_playlist_path
        })

    start_time_total = time.time()
    transcoding_successful = True
    for item in ffmpeg_commands:
        # ... (ffmpeg execution logic - same as before, sets transcoding_successful=False on failure) ...
        cmd = item['cmd']
        height = item['height']
        logging.info(f"Running ffmpeg for {height}p...")
        logging.debug(f"Command: {' '.join(cmd)}")
        start_time_res = time.time()
        try:
            result = subprocess.run(
                cmd, check=True, capture_output=True, text=True, timeout=FFMPEG_TIMEOUT
            )
            end_time_res = time.time()
            logging.info(f"ffmpeg finished successfully for {height}p in {end_time_res - start_time_res:.2f}s.")
        except subprocess.CalledProcessError as e:
            error_msg = (f"Transcoding failed for {height}p (ffmpeg exit code {e.returncode}).\n"
                         f"STDERR (last 1000 chars):\n...{e.stderr[-1000:]}")
            logging.error(error_msg)
            transcoding_successful = False
            break
        except subprocess.TimeoutExpired as e:
            error_msg = f"Transcoding timed out for {height}p after {FFMPEG_TIMEOUT} seconds."
            logging.error(error_msg)
            stderr_output = e.stderr.decode('utf-8', errors='ignore') if e.stderr else "N/A"
            logging.error(f"STDERR (last 1000): ...{stderr_output[-1000:]}")
            error_msg += f"\nSTDERR: {stderr_output[-1000:]}"
            transcoding_successful = False
            break
        except Exception as e:
            error_msg = f"Unexpected error during transcoding for {height}p: {e}"
            logging.error(error_msg, exc_info=True)
            transcoding_successful = False
            break

    if not transcoding_successful:
         logging.error("Aborting HLS processing due to ffmpeg error.")
         try:
              with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
         except IOError as io_err: logging.error(f"Failed to write ffmpeg error to file: {io_err}")
         return False

    # Create master playlist locally
    logging.info("All resolutions transcoded successfully locally.")
    for detail in resolution_details_for_master:
        master_playlist_content += f'#EXT-X-STREAM-INF:BANDWIDTH={detail["bandwidth"]},RESOLUTION=x{detail["height"]}\n'
        master_playlist_content += f'{detail["playlist_path"]}\n'

    master_playlist_path = os.path.join(output_base_dir, MASTER_PLAYLIST_NAME)
    try:
        with open(master_playlist_path, 'w') as f:
            f.write(master_playlist_content)
        logging.info(f"Local master playlist created successfully at {master_playlist_path}")
        end_time_total = time.time()
        logging.info(f"Total local transcoding time: {end_time_total - start_time_total:.2f}s")
        return True # Indicate transcoding success
    except IOError as e:
        error_msg = f"Failed to write local master playlist: {e}"
        logging.error(error_msg)
        with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
        return False


# === Main Processing Job Function (Download -> Transcode -> Upload) ===
def run_processing_job():
    """Downloads source video, transcodes to HLS, uploads HLS to Dropbox."""
    # Check lock file and completion marker
    if os.path.exists(PROCESSING_LOCK_FILE):
        logging.warning("Lock file found. Checking status...")
        if os.path.exists(DROPBOX_UPLOAD_COMPLETE_FILE):
             logging.info("Dropbox upload complete marker found. Processing already done.")
             return
        elif os.path.exists(PROCESSING_ERROR_FILE):
             logging.warning("Lock file and error file found. Processing likely failed previously. Manual cleanup needed.")
             return
        else:
             # Check for stale lock
             try:
                  lock_age = time.time() - os.path.getmtime(PROCESSING_LOCK_FILE)
                  max_lock_age = 7200 # 2 hours
                  if lock_age > max_lock_age:
                       logging.warning(f"Stale lock file found (age: {lock_age:.0f}s). Removing and retrying.")
                       os.remove(PROCESSING_LOCK_FILE)
                  else:
                       logging.warning(f"Active lock file found (age: {lock_age:.0f}s). Assuming another process is active.")
                       return
             except OSError as e:
                  logging.error(f"Error checking/removing lock file: {e}. Aborting.")
                  return

    logging.info("Starting video processing job (Download -> Transcode -> Dropbox Upload)...")
    lock_acquired = False
    try:
        ensure_dir(BASE_DIR)
        # Create lock file
        with open(PROCESSING_LOCK_FILE, 'w') as f:
            f.write(f'Processing started at: {time.strftime("%Y-%m-%d %H:%M:%S")}')
        lock_acquired = True
        logging.info(f"Created processing lock file: {PROCESSING_LOCK_FILE}")

        # --- Cleanup previous state files ---
        files_to_clean = [PROCESSING_ERROR_FILE, DROPBOX_BASE_URL_FILE, DROPBOX_UPLOAD_COMPLETE_FILE, DOWNLOAD_COMPLETE_FILE]
        for file_path in files_to_clean:
            if os.path.exists(file_path):
                logging.warning(f"Removing previous state file: {file_path}")
                try: os.remove(file_path)
                except OSError as e: logging.error(f"Could not remove {file_path}: {e}")
        # Clean temp transcode dir and cache dir
        for dir_path in [HLS_TRANSCODE_DIR, HLS_CACHE_DIR]:
             if os.path.exists(dir_path):
                  logging.warning(f"Removing previous directory: {dir_path}")
                  try: shutil.rmtree(dir_path)
                  except Exception as e: logging.error(f"Could not remove {dir_path}: {e}")
        # Ensure directories exist
        ensure_dir(STATIC_DIR); ensure_dir(HLS_TRANSCODE_DIR); ensure_dir(HLS_CACHE_DIR); ensure_dir(DOWNLOAD_DIR)
        # --- Cleanup done ---

        # 1. Check ffmpeg
        if not check_ffmpeg(): return

        # 2. Initialize Dropbox Client
        dbx = initialize_dropbox_client()
        if not dbx:
             error_msg = "Failed to initialize Dropbox client."
             with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
             return

        # 3. Download Video
        download_path = os.path.join(DOWNLOAD_DIR, DOWNLOADED_FILENAME)
        if not download_video(VIDEO_URL, download_path): return

        # 4. Transcode Video locally
        if not transcode_to_hls(download_path, HLS_TRANSCODE_DIR, RESOLUTIONS):
            logging.error("Transcoding step failed.")
            # Error file should be written by transcode_to_hls
            return

        # 5. Upload HLS files to Dropbox
        base_url = upload_hls_to_dropbox(dbx, HLS_TRANSCODE_DIR, DROPBOX_HLS_FOLDER_PATH)
        if not base_url:
            error_msg = "Dropbox upload step failed after transcoding."
            logging.error(error_msg)
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
            return

        logging.info(f"Processing job completed successfully. Dropbox Base URL: {base_url}")

        # 6. Cleanup local transcoded files (optional)
        try:
            shutil.rmtree(HLS_TRANSCODE_DIR)
            logging.info(f"Removed local transcoding directory: {HLS_TRANSCODE_DIR}")
        except Exception as e:
            logging.warning(f"Could not remove local transcoding directory {HLS_TRANSCODE_DIR}: {e}")

    except Exception as e:
        error_msg = f"Critical unexpected error in processing job: {e}"
        logging.error(error_msg, exc_info=True)
        try:
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
        except IOError as io_err: logging.error(f"Failed to write critical error to file: {io_err}")
    # finally:
        # Keep the lock file on success (indicated by DROPBOX_UPLOAD_COMPLETE_FILE)
        # Remove lock file on failure? Or require manual intervention?
        # Let's keep it for now. Manual removal of lock/error file needed to retry.


# === Cache Management ===

def touch_cache_timestamp(cache_file_path):
    """Updates the .last_accessed timestamp file in the directory of the cache file."""
    try:
        cache_dir = os.path.dirname(cache_file_path)
        if cache_dir == HLS_CACHE_DIR: # Don't put marker in root cache dir for master playlist
             return
        marker_path = os.path.join(cache_dir, CACHE_ACCESS_MARKER)
        ensure_dir(cache_dir) # Ensure directory exists
        with open(marker_path, 'a'): # Create file if not exists
             os.utime(marker_path, None) # Update access and modification time to now
        # logging.debug(f"Touched cache timestamp: {marker_path}")
    except Exception as e:
        logging.warning(f"Failed to update cache timestamp for {cache_file_path}: {e}")


def download_from_dropbox_to_cache(relative_path):
    """Downloads a specific HLS file from Dropbox to the local cache."""
    cache_path = os.path.join(HLS_CACHE_DIR, relative_path)
    cache_dir = os.path.dirname(cache_path)
    ensure_dir(cache_dir)

    # Simple lock mechanism per file download
    lock_path = cache_path + '.downloading'
    if os.path.exists(lock_path):
        logging.warning(f"Download already in progress for {relative_path}. Waiting briefly.")
        time.sleep(1) # Wait a moment
        # Recheck if file now exists after waiting
        if os.path.exists(cache_path) and os.path.getsize(cache_path) > 0:
            logging.info(f"File {relative_path} appeared in cache after waiting.")
            return cache_path # File was downloaded by another request
        else:
            logging.error(f"Download lock file still exists for {relative_path}, but file not cached. Aborting redundant download.")
            return None # Avoid multiple downloads

    try:
        # Create download lock
        with open(lock_path, 'w') as f: f.write('locked')

        # Get base URL
        if not os.path.exists(DROPBOX_BASE_URL_FILE):
            logging.error("Dropbox base URL file not found. Cannot download to cache.")
            return None
        with open(DROPBOX_BASE_URL_FILE, 'r') as f:
            base_url = f.read().strip()
        if not base_url:
            logging.error("Dropbox base URL is empty. Cannot download to cache.")
            return None

        # Construct full URL
        # Ensure no double slashes and handle potential query strings in base_url (though unlikely)
        dropbox_url = base_url.rstrip('/') + '/' + relative_path.lstrip('/')
        # Append ?raw=1 if it looks like a playlist file or needed
        if relative_path.endswith('.m3u8') and '?raw=1' not in dropbox_url:
             dropbox_url += '?raw=1'


        logging.info(f"Cache miss for {relative_path}. Downloading from Dropbox: {dropbox_url}")

        with requests.get(dropbox_url, stream=True, timeout=60) as r: # Shorter timeout for segments
            r.raise_for_status()
            with open(cache_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        if os.path.getsize(cache_path) > 0:
            logging.info(f"Successfully downloaded and cached: {cache_path}")
            touch_cache_timestamp(cache_path) # Update timestamp on successful download
            return cache_path
        else:
            logging.warning(f"Downloaded file is empty: {cache_path}. Removing.")
            try: os.remove(cache_path)
            except OSError: pass
            return None

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download {relative_path} from Dropbox ({dropbox_url}): {e}")
        # Clean up empty cache file if download failed mid-way
        if os.path.exists(cache_path) and os.path.getsize(cache_path) == 0:
             try: os.remove(cache_path)
             except OSError: pass
        return None
    except Exception as e:
        logging.error(f"Unexpected error downloading {relative_path} to cache: {e}")
        return None
    finally:
         # Remove download lock
         if os.path.exists(lock_path):
              try: os.remove(lock_path)
              except OSError: pass


def run_cache_eviction():
    """Scans cache directory and removes expired items."""
    logging.info("Running cache eviction check...")
    now = time.time()
    try:
        if not os.path.exists(HLS_CACHE_DIR):
            logging.info("Cache directory does not exist. Skipping eviction.")
            return

        # Iterate through items directly in HLS_CACHE_DIR (these would be resolution folders like '360', '720')
        for item_name in os.listdir(HLS_CACHE_DIR):
            item_path = os.path.join(HLS_CACHE_DIR, item_name)
            # Only consider directories (resolution folders)
            if os.path.isdir(item_path):
                marker_path = os.path.join(item_path, CACHE_ACCESS_MARKER)
                if os.path.exists(marker_path):
                    try:
                        last_access_time = os.path.getmtime(marker_path)
                        age = now - last_access_time
                        if age > CACHE_MAX_AGE_SECONDS:
                            logging.info(f"Cache directory {item_path} expired (age: {age:.0f}s > {CACHE_MAX_AGE_SECONDS}s). Removing.")
                            shutil.rmtree(item_path)
                        # else:
                        #     logging.debug(f"Cache directory {item_path} is fresh (age: {age:.0f}s). Keeping.")
                    except OSError as e:
                        logging.error(f"Error accessing cache marker or directory {item_path}: {e}")
                    except Exception as e:
                        logging.error(f"Unexpected error processing cache item {item_path}: {e}", exc_info=True)
                else:
                    # Directory exists but no marker? Maybe created but never accessed?
                    # Or marker deleted erroneously? Check directory mtime as fallback? Risky.
                    # Let's assume if no marker, it's either very new or stale. Check dir mtime.
                    try:
                         dir_mtime = os.path.getmtime(item_path)
                         dir_age = now - dir_mtime
                         # If dir is old and has no marker, likely safe to remove
                         if dir_age > CACHE_MAX_AGE_SECONDS * 1.1 : # Add a buffer
                              logging.warning(f"Cache directory {item_path} has no access marker and is old (age: {dir_age:.0f}s). Removing.")
                              shutil.rmtree(item_path)
                    except OSError as e:
                         logging.error(f"Error checking directory mtime {item_path}: {e}")

    except Exception as e:
        logging.error(f"Error during cache eviction scan: {e}", exc_info=True)
    logging.info("Cache eviction check finished.")


def cache_eviction_scheduler():
    """Runs the cache eviction function periodically."""
    logging.info(f"Starting cache eviction scheduler. Interval: {CACHE_CHECK_INTERVAL_SECONDS}s")
    # Run once immediately (optional)
    # run_cache_eviction()

    # Schedule the job
    schedule.every(CACHE_CHECK_INTERVAL_SECONDS).seconds.do(run_cache_eviction)

    while True:
        schedule.run_pending()
        time.sleep(60) # Check every minute if jobs are due


# === Flask Routes ===

@app.route('/')
def index():
    """Serves the main HTML page."""
    error_message = None
    is_processing = False
    is_ready_for_streaming = False
    stream_url = None
    status_detail = "Initializing..."

    if os.path.exists(PROCESSING_ERROR_FILE):
        try:
            with open(PROCESSING_ERROR_FILE, 'r') as f: error_message = f.read()
            status_detail = "প্রসেসিং ব্যর্থ হয়েছে"
        except Exception as e: error_message = f"Could not read error file: {e}"; status_detail = "ত্রুটি ফাইল পড়তে সমস্যা"

    elif os.path.exists(DROPBOX_UPLOAD_COMPLETE_FILE): # Check if upload to Dropbox is done
        is_ready_for_streaming = True
        stream_url = f"/stream/{MASTER_PLAYLIST_NAME}" # Point to the server's caching stream route
        status_detail = "স্ট্রিমিংয়ের জন্য প্রস্তুত (সার্ভার ক্যাশ)"
        logging.info(f"Dropbox upload complete. Serving stream URL: {stream_url}")

    elif os.path.exists(PROCESSING_LOCK_FILE):
        is_processing = True
        status_detail = "প্রসেসিং চলছে (ট্রান্সকোড/আপলোড)..."

    else: # No lock, no completion, no error - Try starting
         status_detail = "প্রসেসিং শুরু হচ্ছে..."
         if not (processing_thread and processing_thread.is_alive()):
              logging.warning("No processing state files found. Attempting to start processing thread.")
              start_processing_thread()
              is_processing = True
         else:
              is_processing = True
              status_detail = "প্রসেসিং চলছে (স্টেট অস্পষ্ট)..."


    logging.info(f"Rendering index: ready={is_ready_for_streaming}, processing={is_processing}, error={bool(error_message)}, status='{status_detail}'")
    return render_template('index.html',
                           hls_ready=is_ready_for_streaming,
                           processing=is_processing,
                           error=error_message,
                           # Pass the SERVER's stream URL, not Dropbox URL
                           master_playlist_stream_url=stream_url,
                           status_detail=status_detail)


@app.route('/stream/<path:filename>')
def stream_hls_files(filename):
    """Serves HLS files, caching them from Dropbox if necessary."""
    # Basic security check
    if '..' in filename or filename.startswith('/'):
        logging.warning(f"Directory traversal attempt blocked for: {filename}")
        abort(403)

    cache_path = os.path.join(HLS_CACHE_DIR, filename)

    # 1. Check cache
    if os.path.exists(cache_path) and os.path.getsize(cache_path) > 0:
        logging.debug(f"Cache hit for: {filename}")
        touch_cache_timestamp(cache_path) # Update access time
        return send_from_directory(HLS_CACHE_DIR, filename, conditional=True)
    else:
        logging.info(f"Cache miss for: {filename}. Attempting download from Dropbox.")
        # 2. Cache miss - Download from Dropbox
        downloaded_path = download_from_dropbox_to_cache(filename)
        if downloaded_path:
            # 3. Serve the newly downloaded file
            return send_from_directory(HLS_CACHE_DIR, filename, conditional=True)
        else:
            # 4. Download failed
            logging.error(f"Failed to download {filename} from Dropbox to cache.")
            abort(404) # Or 500 Internal Server Error

# === Application Startup & Background Threads ===
processing_thread = None
cache_eviction_thread = None

def start_processing_thread():
    # ... (same logic as before, checks lock, error, completion marker) ...
    global processing_thread
    if processing_thread and processing_thread.is_alive(): return
    if os.path.exists(DROPBOX_UPLOAD_COMPLETE_FILE): return
    if os.path.exists(PROCESSING_ERROR_FILE): return

    if os.path.exists(PROCESSING_LOCK_FILE):
         try: # Stale lock check
              lock_age = time.time() - os.path.getmtime(PROCESSING_LOCK_FILE)
              max_lock_age = 7200 # 2 hours
              if lock_age > max_lock_age:
                   logging.warning(f"Stale lock file found. Removing and starting thread.")
                   os.remove(PROCESSING_LOCK_FILE)
              else:
                   logging.warning(f"Active lock file found. Not starting new thread.")
                   return
         except OSError as e:
              logging.error(f"Error checking/removing lock file: {e}.")
              return

    if not DROPBOX_ACCESS_TOKEN:
        logging.error("Cannot start processing thread: DROPBOX_ACCESS_TOKEN not set.")
        # Optionally write to error file
        return

    logging.info("Starting background processing thread...")
    processing_thread = threading.Thread(target=run_processing_job, name="ProcessingThread", daemon=True)
    processing_thread.start()

def start_cache_eviction_thread():
    """Starts the cache eviction scheduler thread."""
    global cache_eviction_thread
    if cache_eviction_thread and cache_eviction_thread.is_alive():
        logging.info("Cache eviction thread already running.")
        return

    logging.info("Starting cache eviction scheduler thread...")
    cache_eviction_thread = threading.Thread(target=cache_eviction_scheduler, name="CacheEvictionThread", daemon=True)
    cache_eviction_thread.start()


@app.before_first_request
def initialize_app():
    """Initialize directories and start background threads."""
    logging.info("Initializing application...")
    ensure_dir(DOWNLOAD_DIR)
    ensure_dir(STATIC_DIR)
    ensure_dir(HLS_TRANSCODE_DIR)
    ensure_dir(HLS_CACHE_DIR)
    start_processing_thread()
    start_cache_eviction_thread()
    logging.info("Application initialization complete.")


# === Main Execution Block ===
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    # Important: Set debug=False when using background threads with Flask's default server
    # or use a production server like Gunicorn/Waitress. The reloader can cause issues.
    # For Gunicorn, threading works okay.
    app.run(host='0.0.0.0', port=port, debug=False)

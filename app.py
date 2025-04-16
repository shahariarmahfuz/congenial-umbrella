import os
import subprocess
import requests
import threading
import logging
import time
import dropbox # ড্রপবক্স ইম্পোর্ট
from dropbox.exceptions import ApiError, AuthError
from dropbox.files import WriteMode
from flask import Flask, render_template, send_from_directory, abort, Response

# === Logging Configuration ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# === Flask App Initialization ===
app = Flask(__name__)

# === Configuration Constants ===
VIDEO_URL = "[https://www.dropbox.com/scl/fi/qrzcox70ca91sb0kvs3e6/AQMjFm0PTBsLYCQ5zjKeCNSDa5bcmSWIGn_NYwUdErAVoCos5otAlo6NY8ZPSzF3Tq0epd8y_GX1mBMllyHtrCTY.mp4?rlkey=ftkmjlu69k1f32r2hw0x2jvk2&st=timxwsta&raw=1](https://www.dropbox.com/scl/fi/kw2rpr2vsl7hf9gaddtsg/VID_20250330_041149_786.mp4?rlkey=rb347g41y8r0ekqvu3vea2znp&st=c15wzwjp&raw=1)"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, 'downloads')
STATIC_DIR = os.path.join(BASE_DIR, 'static')
HLS_DIR = os.path.join(STATIC_DIR, 'hls') # Local HLS path before upload
DOWNLOADED_FILENAME = "source_video.mp4"
MASTER_PLAYLIST_NAME = "master.m3u8"

# --- Dropbox Configuration ---
# --- Dropbox Configuration ---
DROPBOX_ACCESS_TOKEN = "sl.u.AFoXkairOjvTRvgcYqJr5ZU4nt6lI9SqUO5exYo45vNQPSVERcpAAMXe6jMWKqmrSER1DX2XvXofYZGmD1TfZ9M9CR3MrMjkoFPXJf9TJk1GZCkvVZrM-lzCFs9p_U1_yTKIroo0zFII5tq2B2jyhekGJfkC8LGDM_SX-i_SmHm5qbGVN-vDxtcNk1iWJtUU0wi2yoPNmSHMTKMNFztttNSzleMYEi4lIdiwXzRPnDXRcbfyqviA2dbKriSjfBTROFPMf52Uodzyt8aWWM4CS9GEXhz2QsffIjewsVlZh1Mv3xt94DhgJNgI6gUKhhmkNFk5TxoLFS6qqaYa72mpzhxd3p1ZnzJXcw1sWyuflW5JseTBTp8rLsTh8raB_d9gRxM3gbpcD5jIHopJf2YwBHpnEVVog6WPJiwJCIJWtIFdTIBJ9uBwoLODbm41X2nMAQQLkXmRZtHJNrneS7eTUiNY3MYU2aXojMTWQ3_PS28bW645KB3zqHNAeqd8ZLf3OGylkkazcfK--7fWsbE5yKVeqTbpHVIyBl44KWWlY4OeKUVCpKpbjZD26M3ec_PfFeSb6CEkcIscx65VRTWDcsO8KonmJF89L1Xayo7hjzsKKL-n41tQSswt5hCkbckfZTD1jAwsDGdSSbiUzFlyQbNVY-6k9Jk2x2uqRUzOXMclfNAIaLCgtoIk9BW_cKfsl6kgqRTAPg6aDWNkqOmOe68SN28zwBGteXUFnqUnZTYWZMdU9Cgs8-KUABhqnLyUb0Xd-cVCcrapbjoJFxs_aJ4lJSm8sutBtFFzcHwbjnJi_05esuOFvGn4UQVF9xN1o32KHaeQRytuGqqup6jGcrhZoQFkZ2Fih6Ai4daCrojtjn_lNUnzBMFwlk8JJnasWONX_cvM1gjcI0qcY_lQRM0pZ58XNcgBBk5PiCCPfk5fOVi-oqODaZ_BKVBIqyZ-qn_ltd0SCRiw-yDUJ4Rg99A4shKXgOMiK4rL7UAi-RXpOmWNgAMMwOzRcTaINLKT5jfiGUAYt1b3Cued6JZhWMImq0Fbxf9AwAjgmtZpNK7CgWA46j1XJeSumM18o_MvbM0kf5W1HJTr43-NlE0nBbHVC0mZiWh7SLWM7S4qNj8WRnlEAYlJM7OaDp589jXe3GTxJhSvbYpr0PdKXRODZ9mU-ZYv7-U0i94YpEkenGJxIOOhPlRB25MdNaqLut6W_-_h6N5cpHkzzOtlT_BK0WHYDUngAtYAmHHkxz2BRhurtRxTEkZE3xwi0X5X5b5X_a0vR-1Quwfj5fsaNmy35U6oxJwB6ZsOfKIaeBlxsCiIi0NsaPCWA5_mZEH-6eoZR_wDCBKIzWuajkwwX5WrkTjirNfnGlLiduNh0ZGoiptCJ-3D1m56cXYGhmVrnBjjJPjD0S0F9AohIH9lmzq9WHui"
DROPBOX_HLS_FOLDER_PATH = "/HLS_Streams"

if not DROPBOX_ACCESS_TOKEN:
    logging.warning("DROPBOX_ACCESS_TOKEN environment variable not set. Dropbox upload will fail.")
if not DROPBOX_HLS_FOLDER_PATH.startswith('/'):
     logging.warning("DROPBOX_HLS_FOLDER_PATH should start with a '/'. Using provided path anyway.")
     DROPBOX_HLS_FOLDER_PATH = '/' + DROPBOX_HLS_FOLDER_PATH.lstrip('/')


# Define desired output resolutions and bitrates (height, video_bitrate, audio_bitrate)
RESOLUTIONS = [
    (360, '800k', '96k'),
    (480, '1400k', '128k'),
    (720, '2800k', '128k')
]
FFMPEG_TIMEOUT = 1800 # 30 minutes

# === State Management Files ===
PROCESSING_LOCK_FILE = os.path.join(BASE_DIR, '.processing.lock')
HLS_READY_FILE = os.path.join(HLS_DIR, '.hls_ready') # Indicates local HLS generation is complete
DOWNLOAD_COMPLETE_FILE = os.path.join(DOWNLOAD_DIR, '.download_complete')
PROCESSING_ERROR_FILE = os.path.join(BASE_DIR, '.processing.error')
DROPBOX_LINK_FILE = os.path.join(BASE_DIR, '.dropbox_link') # Stores the final Dropbox master playlist URL

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
        try:
            with open(PROCESSING_ERROR_FILE, 'w') as f:
                f.write(f"Fatal Error: ffmpeg is required but not found or not working.\nDetails: {e}")
        except IOError as io_err:
             logging.error(f"Failed to write ffmpeg error to file: {io_err}")
        return False

def download_video(url, dest_path):
    """Downloads the video file if marker doesn't exist."""
    # ... (download_video function remains unchanged) ...
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
                for chunk in r.iter_content(chunk_size=8192*4): # Slightly larger chunk size
                    f.write(chunk)
                    bytes_downloaded += len(chunk)
                    # Optional: Add progress logging here if needed

            end_time = time.time()
            if bytes_downloaded > 0 and end_time > start_time:
                 download_speed = (bytes_downloaded / (1024*1024)) / (end_time - start_time)
                 logging.info(f"Download complete ({bytes_downloaded / (1024*1024):.2f} MB) in {end_time - start_time:.2f}s ({download_speed:.2f} MB/s).")
            else:
                 logging.info(f"Download complete ({bytes_downloaded / (1024*1024):.2f} MB).")


        if os.path.getsize(dest_path) == 0:
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


# === Dropbox Helper Functions ===

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
    """Uploads a single file to Dropbox."""
    max_retries = 3
    retry_delay = 5 # seconds
    for attempt in range(max_retries):
        try:
            with open(local_path, 'rb') as f:
                logging.info(f"Uploading {local_path} to Dropbox path: {dropbox_path}")
                dbx.files_upload(f.read(), dropbox_path, mode=WriteMode('overwrite'))
                logging.info(f"Successfully uploaded to {dropbox_path}")
                return True
        except ApiError as e:
            logging.error(f"Dropbox API error uploading {local_path} to {dropbox_path}: {e}")
        except IOError as e:
            logging.error(f"File I/O error reading {local_path} for Dropbox upload: {e}")
            return False # Don't retry IO errors
        except Exception as e:
             logging.error(f"Unexpected error uploading {local_path} to {dropbox_path}: {e}")

        if attempt < max_retries - 1:
            logging.warning(f"Retrying Dropbox upload for {local_path} in {retry_delay}s...")
            time.sleep(retry_delay)
        else:
            logging.error(f"Dropbox upload failed for {local_path} after {max_retries} attempts.")
            return False
    return False


def get_or_create_direct_shareable_link(dbx, dropbox_path):
    """Gets or creates a direct shareable link ('raw=1') for a Dropbox file."""
    try:
        # Check for existing shared links
        shared_links = dbx.sharing_list_shared_links(path=dropbox_path, direct_only=True).links
        if shared_links:
            link = shared_links[0].url
            logging.info(f"Found existing direct shared link for {dropbox_path}")
        else:
            # Create a new shared link if none exists
            logging.info(f"Creating new shared link for {dropbox_path}")
            settings = dropbox.sharing.SharedLinkSettings(requested_visibility=dropbox.sharing.RequestedVisibility.public)
            link_metadata = dbx.sharing_create_shared_link_with_settings(dropbox_path, settings=settings)
            link = link_metadata.url
            logging.info(f"Created new shared link: {link}")

        # Ensure the link is a direct download link
        if "?dl=0" in link:
            direct_link = link.replace("?dl=0", "?raw=1")
        elif "?dl=1" in link:
             direct_link = link.replace("?dl=1", "?raw=1")
        elif "?raw=1" not in link:
            direct_link = link + "?raw=1"
        else:
             direct_link = link # Already a raw link

        logging.info(f"Direct shareable link for {dropbox_path}: {direct_link}")
        return direct_link

    except ApiError as e:
        logging.error(f"Dropbox API error getting/creating share link for {dropbox_path}: {e}")
        # Handle specific errors like 'shared_link_already_exists' if necessary, though list_shared_links should handle it.
    except Exception as e:
        logging.error(f"Unexpected error getting/creating share link for {dropbox_path}: {e}")

    return None

def upload_hls_to_dropbox(dbx, local_hls_dir, dropbox_base_path):
    """Uploads all HLS files (master playlist and segments) to Dropbox."""
    master_playlist_local_path = os.path.join(local_hls_dir, MASTER_PLAYLIST_NAME)
    master_playlist_dropbox_path = f"{dropbox_base_path}/{MASTER_PLAYLIST_NAME}"

    # Upload master playlist first
    if not upload_to_dropbox(dbx, master_playlist_local_path, master_playlist_dropbox_path):
        logging.error("Failed to upload master playlist to Dropbox. Aborting HLS upload.")
        return None # Indicates failure

    # Upload segment directories and files
    for item in os.listdir(local_hls_dir):
        local_item_path = os.path.join(local_hls_dir, item)
        # Upload only resolution directories (e.g., '360', '480', '720')
        if os.path.isdir(local_item_path) and item.isdigit():
            resolution = item
            dropbox_res_dir = f"{dropbox_base_path}/{resolution}"
            logging.info(f"Uploading contents of {local_item_path} to Dropbox folder {dropbox_res_dir}")

            # List files within the resolution directory
            try:
                segment_files = [f for f in os.listdir(local_item_path) if os.path.isfile(os.path.join(local_item_path, f))]
            except OSError as e:
                 logging.error(f"Could not list files in {local_item_path}: {e}")
                 continue # Skip this directory

            for filename in segment_files:
                local_file = os.path.join(local_item_path, filename)
                dropbox_file_path = f"{dropbox_res_dir}/{filename}"
                if not upload_to_dropbox(dbx, local_file, dropbox_file_path):
                    logging.error(f"Failed to upload segment/playlist {local_file}. Upload may be incomplete.")
                    # Decide if you want to abort completely here or just log and continue
                    # return None # Abort if any segment fails

    logging.info("Finished uploading HLS files to Dropbox.")

    # Get the direct shareable link for the master playlist
    master_playlist_url = get_or_create_direct_shareable_link(dbx, master_playlist_dropbox_path)
    if not master_playlist_url:
         logging.error("Failed to get shareable link for the master playlist.")
         return None

    # Store the link locally
    try:
        with open(DROPBOX_LINK_FILE, 'w') as f:
            f.write(master_playlist_url)
        logging.info(f"Saved Dropbox master playlist link to {DROPBOX_LINK_FILE}")
    except IOError as e:
        logging.error(f"Failed to save Dropbox link to file: {e}")
        # Even if saving fails, we got the link, so return it but log the issue
        return master_playlist_url # Return URL but acknowledge file write error

    return master_playlist_url


# === Transcoding Function (Modified for Dropbox Upload) ===

def transcode_to_hls(input_path, output_base_dir, resolutions, dbx):
    """Transcodes video to HLS and uploads to Dropbox."""
    # Check if already processed and uploaded
    if os.path.exists(DROPBOX_LINK_FILE) and os.path.exists(HLS_READY_FILE):
        logging.info("Dropbox link file and HLS ready marker found. Assuming already processed and uploaded.")
        try:
            with open(DROPBOX_LINK_FILE, 'r') as f:
                return f.read().strip() # Return existing link
        except IOError as e:
            logging.error(f"Could not read existing Dropbox link file: {e}. Proceeding with transcode/upload.")

    if os.path.exists(HLS_READY_FILE):
         logging.info("Local HLS ready marker found, but Dropbox link missing. Proceeding to upload.")
         # No need to transcode again if local files exist and seem valid
         if os.path.exists(os.path.join(output_base_dir, MASTER_PLAYLIST_NAME)):
             master_playlist_url = upload_hls_to_dropbox(dbx, output_base_dir, DROPBOX_HLS_FOLDER_PATH)
             if master_playlist_url:
                 # Create HLS ready marker again just to be sure state is consistent
                 with open(HLS_READY_FILE, 'w') as f: f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
                 logging.info("Dropbox upload complete after finding local HLS files.")
                 return master_playlist_url
             else:
                 error_msg = "Found local HLS files, but Dropbox upload failed."
                 logging.error(error_msg)
                 with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
                 return None # Indicate failure
         else:
             logging.warning("HLS ready marker found, but master playlist missing locally. Re-transcoding.")
             try: os.remove(HLS_READY_FILE)
             except OSError as e: logging.error(f"Could not remove stale HLS ready marker: {e}")


    if not os.path.exists(input_path) or os.path.getsize(input_path) == 0:
        error_msg = f"Input video file not found or is empty: {input_path}"
        logging.error(error_msg)
        with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
        return None # Use None to indicate failure

    logging.info(f"Starting HLS transcoding from {input_path} into {output_base_dir}...")
    ensure_dir(output_base_dir)
    master_playlist_content = "#EXTM3U\n#EXT-X-VERSION:3\n"
    ffmpeg_commands = []
    resolution_details_for_master = []

    # Prepare ffmpeg commands
    for height, v_bitrate, a_bitrate in resolutions:
        res_output_dir = os.path.join(output_base_dir, str(height))
        ensure_dir(res_output_dir)
        # IMPORTANT: HLS playlist expects relative paths for segments from its location
        relative_playlist_path = f"{height}/playlist.m3u8" # Path relative to master for master playlist
        segment_path_pattern = os.path.join(res_output_dir, 'segment%03d.ts') # Local path for ffmpeg
        absolute_playlist_path = os.path.join(res_output_dir, 'playlist.m3u8') # Local path for ffmpeg

        cmd = [
            'ffmpeg', '-i', input_path,
            '-vf', f'scale=-2:{height}',
            '-c:v', 'libx264', '-crf', '23', '-preset', 'veryfast', # 'fast' or 'medium' might be better for quality vs speed
            '-b:v', v_bitrate, '-maxrate', v_bitrate, '-bufsize', f'{int(v_bitrate[:-1])*2}k',
            '-c:a', 'aac', '-ar', '48000', '-b:a', a_bitrate,
            '-f', 'hls',
            '-hls_time', '6',      # Segment duration in seconds
            '-hls_list_size', '0', # Keep all segments in the playlist
            '-hls_segment_filename', segment_path_pattern,
            '-hls_flags', 'delete_segments+append_list', # Overwrite segments but append to list if restarting
             # '-start_number', '0', # Optional: ensure segment numbering starts from 0
            absolute_playlist_path
        ]
        ffmpeg_commands.append({'cmd': cmd, 'height': height})
        bandwidth = int(v_bitrate[:-1]) * 1000 + int(a_bitrate[:-1]) * 1000
        resolution_details_for_master.append({
            'bandwidth': bandwidth,
            'height': height,
            'playlist_path': relative_playlist_path # Use relative path for the master playlist
        })

    # Execute ffmpeg commands
    start_time_total = time.time()
    for item in ffmpeg_commands:
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
            logging.debug(f"FFmpeg ({height}p) STDOUT: {result.stdout[-500:]}") # Log last bit of stdout
            logging.debug(f"FFmpeg ({height}p) STDERR: {result.stderr[-500:]}") # Log last bit of stderr

        except subprocess.CalledProcessError as e:
            error_msg = (f"Transcoding failed for {height}p (ffmpeg exit code {e.returncode}).\n"
                         f"STDERR (last 1000 chars):\n...{e.stderr[-1000:]}")
            logging.error(error_msg)
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
            return None # Use None to indicate failure
        except subprocess.TimeoutExpired as e:
            error_msg = f"Transcoding timed out for {height}p after {FFMPEG_TIMEOUT} seconds."
            logging.error(error_msg)
            if e.stderr: logging.error(f"STDERR (last 1000): ...{e.stderr[-1000:]}")
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
            return None # Use None to indicate failure
        except Exception as e:
            error_msg = f"Unexpected error during transcoding for {height}p: {e}"
            logging.error(error_msg, exc_info=True)
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
            return None # Use None to indicate failure

    # Create master playlist locally
    logging.info("All resolutions transcoded successfully locally.")
    for detail in resolution_details_for_master:
        master_playlist_content += f'#EXT-X-STREAM-INF:BANDWIDTH={detail["bandwidth"]},RESOLUTION=x{detail["height"]}\n'
        # Use the RELATIVE path here
        master_playlist_content += f'{detail["playlist_path"]}\n'

    master_playlist_path = os.path.join(output_base_dir, MASTER_PLAYLIST_NAME)
    try:
        with open(master_playlist_path, 'w') as f:
            f.write(master_playlist_content)
        logging.info(f"Local master playlist created successfully at {master_playlist_path}")
        # Create local HLS ready marker now
        with open(HLS_READY_FILE, 'w') as f:
             f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
        logging.info("Local HLS processing complete. Ready marker created.")
        end_time_total = time.time()
        logging.info(f"Total local transcoding time: {end_time_total - start_time_total:.2f}s")

        # --- Now Upload to Dropbox ---
        logging.info("Starting Dropbox upload process...")
        master_playlist_url = upload_hls_to_dropbox(dbx, output_base_dir, DROPBOX_HLS_FOLDER_PATH)
        if master_playlist_url:
            logging.info("Successfully uploaded HLS files to Dropbox and obtained link.")
            # Optionally: Clean up local HLS files after successful upload
            # try:
            #     import shutil
            #     shutil.rmtree(output_base_dir)
            #     logging.info(f"Removed local HLS directory: {output_base_dir}")
            # except Exception as e:
            #     logging.warning(f"Could not remove local HLS directory {output_base_dir}: {e}")
            return master_playlist_url # Success! Return the Dropbox URL
        else:
            error_msg = "HLS transcoding successful locally, but Dropbox upload failed."
            logging.error(error_msg)
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
            return None # Indicate failure

    except IOError as e:
        error_msg = f"Failed to write local master playlist: {e}"
        logging.error(error_msg)
        with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
        return None # Indicate failure


# === Main Processing Job Function ===

def run_processing_job():
    """Downloads, transcodes, and uploads video to Dropbox."""
    if os.path.exists(PROCESSING_LOCK_FILE):
        logging.warning("Lock file found. Checking status...")
        if os.path.exists(DROPBOX_LINK_FILE):
             logging.info("Dropbox link file found. Processing already completed successfully.")
             return
        elif os.path.exists(PROCESSING_ERROR_FILE):
             logging.warning("Lock file and error file found. Processing likely failed previously.")
             # Consider logic here: Maybe allow retry after some time? For now, just exit.
             return
        else:
             logging.warning("Lock file exists, but no Dropbox link or error file. Assuming another process is active or stalled.")
             return # Avoid running concurrently

    logging.info("Starting video processing job (Download -> Transcode -> Dropbox Upload)...")
    master_playlist_url = None
    try:
        ensure_dir(BASE_DIR)
        # Create lock file
        with open(PROCESSING_LOCK_FILE, 'w') as f:
            f.write(f'Processing started at: {time.strftime("%Y-%m-%d %H:%M:%S")}')
        logging.info(f"Created processing lock file: {PROCESSING_LOCK_FILE}")

        # Clean up previous error file if it exists
        if os.path.exists(PROCESSING_ERROR_FILE):
            logging.warning("Removing previous error file.")
            try: os.remove(PROCESSING_ERROR_FILE)
            except OSError as e: logging.error(f"Could not remove previous error file: {e}")
        # Clean up previous Dropbox link file if re-processing
        if os.path.exists(DROPBOX_LINK_FILE):
            logging.warning("Removing previous Dropbox link file before starting.")
            try: os.remove(DROPBOX_LINK_FILE)
            except OSError as e: logging.error(f"Could not remove previous Dropbox link file: {e}")
        # Clean up previous HLS ready file if re-processing
        if os.path.exists(HLS_READY_FILE):
             logging.warning("Removing previous HLS ready marker before starting.")
             try: os.remove(HLS_READY_FILE)
             except OSError as e: logging.error(f"Could not remove previous HLS ready marker: {e}")

        ensure_dir(STATIC_DIR)
        ensure_dir(HLS_DIR) # Local temporary HLS storage

        # 1. Check ffmpeg
        if not check_ffmpeg():
            logging.error("ffmpeg check failed. Aborting processing.")
            # Error file already created by check_ffmpeg
            return

        # 2. Initialize Dropbox Client
        dbx = initialize_dropbox_client()
        if not dbx:
            error_msg = "Failed to initialize Dropbox client. Check token and connection."
            logging.error(error_msg)
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
            return

        # 3. Download Video
        download_path = os.path.join(DOWNLOAD_DIR, DOWNLOADED_FILENAME)
        if not download_video(VIDEO_URL, download_path):
            logging.error("Download step failed. Aborting processing.")
            # Error file already created by download_video
            return

        # 4. Transcode Video and Upload to Dropbox
        master_playlist_url = transcode_to_hls(download_path, HLS_DIR, RESOLUTIONS, dbx)
        if not master_playlist_url:
            logging.error("Transcoding and/or Dropbox upload step failed.")
            # Error file should have been created by transcode_to_hls
            return

        logging.info(f"Processing job completed successfully. Master Playlist URL: {master_playlist_url}")

    except Exception as e:
        error_msg = f"Critical unexpected error in processing job: {e}"
        logging.error(error_msg, exc_info=True)
        try:
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
        except IOError as io_err: logging.error(f"Failed to write critical error to file: {io_err}")
    # finally:
        # Keep the lock file unless you specifically want to allow re-runs easily.
        # If successful, the DROPBOX_LINK_FILE acts as the primary success indicator.
        # If failed, the PROCESSING_ERROR_FILE indicates failure.
        # Removing the lock file here might cause race conditions if the app restarts quickly.
        # if master_playlist_url: # Only remove lock on success?
        #     if os.path.exists(PROCESSING_LOCK_FILE):
        #         try: os.remove(PROCESSING_LOCK_FILE)
        #         except OSError as e: logging.warning(f"Could not remove lock file on success: {e}")


# === Flask Routes ===

@app.route('/')
def index():
    """Serves the main HTML page displaying status and video player."""
    error_message = None
    is_processing = False
    is_ready_for_streaming = False
    dropbox_url = None

    if os.path.exists(PROCESSING_ERROR_FILE):
        try:
            with open(PROCESSING_ERROR_FILE, 'r') as f:
                error_message = f.read()
            logging.warning(f"Found error file: {PROCESSING_ERROR_FILE}")
        except Exception as e:
            error_message = f"Could not read error file: {e}"
            logging.error(error_message)

    elif os.path.exists(DROPBOX_LINK_FILE):
        try:
            with open(DROPBOX_LINK_FILE, 'r') as f:
                dropbox_url = f.read().strip()
            if dropbox_url:
                is_ready_for_streaming = True
                logging.info(f"Found Dropbox link file. Ready for streaming from: {dropbox_url}")
            else:
                 error_message = "Dropbox link file is empty. Processing may have failed silently."
                 logging.error(error_message)
        except Exception as e:
            error_message = f"Could not read Dropbox link file: {e}"
            logging.error(error_message)

    elif os.path.exists(PROCESSING_LOCK_FILE):
        is_processing = True
        logging.info("Processing lock file exists and Dropbox link is not ready -> Status: Processing")

    else: # No lock, no link, no error
         # This state means processing hasn't started or finished cleanly without leaving a trace
         if not (processing_thread and processing_thread.is_alive()):
              logging.warning("No processing state files found. Attempting to start processing thread.")
              start_processing_thread() # Try to start it
              is_processing = True # Assume it will start processing now
         else:
              # This is an odd state - thread alive but no lock file?
              logging.warning("Processing thread is alive but no lock file found. State unclear.")
              error_message = "Server state is unclear. Processing might be initializing."
              is_processing = True # Treat as processing for the user


    logging.info(f"Rendering index: ready_for_streaming={is_ready_for_streaming}, processing={is_processing}, error exists={bool(error_message)}")
    return render_template('index.html',
                           hls_ready=is_ready_for_streaming, # Use this flag for template logic
                           processing=is_processing,
                           error=error_message,
                           dropbox_master_playlist_url=dropbox_url) # Pass the URL


# --- Route for serving local HLS files (kept for potential debugging, but not primary use) ---
@app.route('/hls/<path:filename>')
def serve_hls_files(filename):
    """Serves HLS playlist and segment files locally (mainly for debug)."""
    hls_directory = HLS_DIR
    logging.debug(f"Request for LOCAL HLS file (DEBUG): {filename} from directory {hls_directory}")
    if '..' in filename or filename.startswith('/'):
        logging.warning(f"Directory traversal attempt blocked for: {filename}")
        abort(403)
    # Check if the requested file actually exists locally before trying to send
    requested_path = os.path.join(hls_directory, filename)
    if not os.path.isfile(requested_path):
         logging.warning(f"Local HLS file not found: {requested_path}")
         abort(404)

    try:
        return send_from_directory(hls_directory, filename, conditional=True)
    except FileNotFoundError:
        # This shouldn't happen due to the check above, but handle defensively
        logging.warning(f"Local HLS file not found (send_from_directory): {requested_path}")
        abort(404)
    except Exception as e:
        logging.error(f"Error serving local HLS file {filename}: {e}", exc_info=True)
        abort(500)


# === Application Startup & Background Thread ===
processing_thread = None

def start_processing_thread():
    """Starts the video processing in a background thread if not already running/finished."""
    global processing_thread
    if processing_thread and processing_thread.is_alive():
        logging.info("Processing thread is already running.")
        return
    # Check the definitive success marker first
    if os.path.exists(DROPBOX_LINK_FILE):
        logging.info("Dropbox link file exists. Assuming processing completed successfully. No need to start thread.")
        return
    # Check if it failed previously
    if os.path.exists(PROCESSING_ERROR_FILE):
        logging.warning("Error file exists. Not starting processing thread automatically. Please check the error.")
        return
    # Check if locked by another potential process
    if os.path.exists(PROCESSING_LOCK_FILE):
         logging.warning("Processing lock file exists, but Dropbox link/error file missing. Not starting new thread.")
         return

    logging.info("Starting background processing thread (Download -> Transcode -> Dropbox Upload)...")
    processing_thread = threading.Thread(target=run_processing_job, name="ProcessingThread", daemon=True)
    processing_thread.start()

# --- Ensure processing starts when the application boots ---
# Check if Dropbox token is available before starting
if DROPBOX_ACCESS_TOKEN:
    start_processing_thread()
else:
    logging.error("Cannot start processing thread: DROPBOX_ACCESS_TOKEN is not set.")
    # Create an error file to indicate this configuration issue
    try:
         with open(PROCESSING_ERROR_FILE, 'w') as f:
              f.write("Configuration Error: DROPBOX_ACCESS_TOKEN environment variable is not set.")
    except IOError as e:
         logging.error(f"Failed to write configuration error to file: {e}")

# === Main Execution Block ===
if __name__ == '__main__':
    # Use port 8000 for Gunicorn compatibility as defined in Dockerfile CMD
    # Or use 8080 if running directly with `python app.py` locally (e.g., on Replit)
    port = int(os.environ.get('PORT', 8000))
    # Set debug=False for production/Gunicorn
    app.run(host='0.0.0.0', port=port, debug=False)

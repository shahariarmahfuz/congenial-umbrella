import os
import subprocess
import requests
import threading
import logging
import time
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
# <<<--- শুধুমাত্র এই লাইনটি পরিবর্তন করা হয়েছে --->>>
VIDEO_URL = "https://video-mxp1-1.xx.fbcdn.net/o1/v/t2/f2/m69/AQM8S3pFxa70tno6zop7jYr1U16B60EHmFPInE6TGBwoOaJnQOXYtCml3Qkpv-p01h-Mq8WY8cuwf4HDp-EFVkCJ.mp4?strext=1&_nc_cat=103&_nc_sid=5e9851&_nc_ht=video-mxp1-1.xx.fbcdn.net&_nc_ohc=PsDTzb3w2UsQ7kNvwEdobVT&efg=eyJ2ZW5jb2RlX3RhZyI6Inhwdl9wcm9ncmVzc2l2ZS5GQUNFQk9PSy4uQzMuNzIwLmRhc2hfaDI2NC1iYXNpYy1nZW4yXzcyMHAiLCJ4cHZfYXNzZXRfaWQiOjE0MDc5NDYzMjM1NTI4NTksInZpX3VzZWNhc2VfaWQiOjEwMTIyLCJkdXJhdGlvbl9zIjoyMDAsInVybGdlbl9zb3VyY2UiOiJ3d3cifQ%3D%3D&ccb=17-1&vs=f2a9875f9f41aa3d&_nc_vs=HBksFQIYOnBhc3N0aHJvdWdoX2V2ZXJzdG9yZS9HTmhqVUIzRVlmM3FMbU1DQVBzVVd5WFBEaXNHYm1kakFBQUYVAALIAQAVAhg6cGFzc3Rocm91Z2hfZXZlcnN0b3JlL0dJTkhVaDB6TFExRGFSOEZBR2tockdVaUNUdEdickZxQUFBRhUCAsgBACgAGAAbAogHdXNlX29pbAExEnByb2dyZXNzaXZlX3JlY2lwZQExFQAAJrap08ehoYAFFQIoAkMzLBdAaRiLQ5WBBhgZZGFzaF9oMjY0LWJhc2ljLWdlbjJfNzIwcBEAdQIA&_nc_zt=28&oh=00_AfEkU99vSfDRfVji51klRkyvAj5hml5FUlj3hFYozoLfGg&oe=68056F07&dl=1"
# <<<------------------------------------------>>>

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, 'downloads')
# HLS files will be under 'static/hls' directory
STATIC_DIR = os.path.join(BASE_DIR, 'static')
HLS_DIR = os.path.join(STATIC_DIR, 'hls')
DOWNLOADED_FILENAME = "source_video.mp4" # ফাইলের নাম একই রাখা হলো
MASTER_PLAYLIST_NAME = "master.m3u8"

# Define desired output resolutions and bitrates (height, video_bitrate, audio_bitrate)
RESOLUTIONS = [
    (360, '800k', '96k'),
    (480, '1400k', '128k'),
    (720, '2800k', '128k') # যদি মূল ভিডিও 720p বা তার বেশি হয়
]
FFMPEG_TIMEOUT = 1800 # Timeout for each ffmpeg command in seconds (30 minutes)

# === State Management Files ===
PROCESSING_LOCK_FILE = os.path.join(BASE_DIR, '.processing.lock')
HLS_READY_FILE = os.path.join(HLS_DIR, '.hls_ready')
DOWNLOAD_COMPLETE_FILE = os.path.join(DOWNLOAD_DIR, '.download_complete')
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
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    bytes_downloaded += len(chunk)

            end_time = time.time()
            download_speed = (bytes_downloaded / (1024*1024)) / (end_time - start_time + 1e-6)
            logging.info(f"Download complete ({bytes_downloaded / (1024*1024):.2f} MB) in {end_time - start_time:.2f}s ({download_speed:.2f} MB/s).")

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


def transcode_to_hls(input_path, output_base_dir, resolutions):
    """Transcodes video to HLS format for multiple resolutions."""
    if os.path.exists(HLS_READY_FILE):
        logging.info("HLS ready marker file found. Skipping transcoding.")
        return True

    if not os.path.exists(input_path) or os.path.getsize(input_path) == 0:
        error_msg = f"Input video file not found or is empty: {input_path}"
        logging.error(error_msg)
        with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
        return False

    logging.info(f"Starting HLS transcoding from {input_path} into {output_base_dir}...")
    ensure_dir(output_base_dir)
    master_playlist_content = "#EXTM3U\n#EXT-X-VERSION:3\n"
    ffmpeg_commands = []
    resolution_details_for_master = []

    # Prepare ffmpeg commands
    for height, v_bitrate, a_bitrate in resolutions:
        res_output_dir = os.path.join(output_base_dir, str(height))
        ensure_dir(res_output_dir)
        relative_playlist_path = os.path.join(str(height), 'playlist.m3u8')
        segment_path_pattern = os.path.join(res_output_dir, 'segment%03d.ts')
        absolute_playlist_path = os.path.join(res_output_dir, 'playlist.m3u8')

        cmd = [
            'ffmpeg', '-i', input_path,
            '-vf', f'scale=-2:{height}',
            '-c:v', 'libx264', '-crf', '23', '-preset', 'veryfast',
            '-b:v', v_bitrate, '-maxrate', v_bitrate, '-bufsize', f'{int(v_bitrate[:-1])*2}k',
            '-c:a', 'aac', '-ar', '48000', '-b:a', a_bitrate,
            '-f', 'hls',
            '-hls_time', '6',
            '-hls_list_size', '0',
            '-hls_segment_filename', segment_path_pattern,
            '-hls_flags', 'delete_segments+append_list',
            absolute_playlist_path
        ]
        ffmpeg_commands.append({'cmd': cmd, 'height': height})
        bandwidth = int(v_bitrate[:-1]) * 1000 + int(a_bitrate[:-1]) * 1000
        resolution_details_for_master.append({
            'bandwidth': bandwidth,
            'height': height,
            'playlist_path': relative_playlist_path
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
        except subprocess.CalledProcessError as e:
            error_msg = (f"Transcoding failed for {height}p (ffmpeg exit code {e.returncode}).\n"
                         f"STDERR (last 500 chars):\n...{e.stderr[-500:]}")
            logging.error(error_msg)
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
            return False
        except subprocess.TimeoutExpired as e:
            error_msg = f"Transcoding timed out for {height}p after {FFMPEG_TIMEOUT} seconds."
            logging.error(error_msg)
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
            return False
        except Exception as e:
            error_msg = f"Unexpected error during transcoding for {height}p: {e}"
            logging.error(error_msg, exc_info=True)
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
            return False

    # Create master playlist
    logging.info("All resolutions transcoded successfully.")
    for detail in resolution_details_for_master:
        master_playlist_content += f'#EXT-X-STREAM-INF:BANDWIDTH={detail["bandwidth"]},RESOLUTION=x{detail["height"]}\n'
        master_playlist_content += f'{detail["playlist_path"]}\n'

    master_playlist_path = os.path.join(output_base_dir, MASTER_PLAYLIST_NAME)
    try:
        with open(master_playlist_path, 'w') as f:
            f.write(master_playlist_content)
        logging.info(f"Master playlist created successfully at {master_playlist_path}")
        with open(HLS_READY_FILE, 'w') as f:
             f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
        logging.info("HLS processing complete. Ready marker created.")
        end_time_total = time.time()
        logging.info(f"Total transcoding time: {end_time_total - start_time_total:.2f}s")
        return True
    except IOError as e:
        error_msg = f"Failed to write master playlist: {e}"
        logging.error(error_msg)
        with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
        return False


def run_processing_job():
    """The main job function to download and transcode, run in a thread."""
    if os.path.exists(PROCESSING_LOCK_FILE):
        logging.warning("Lock file found. Processing might be running or finished/failed previously.")
        if os.path.exists(HLS_READY_FILE):
             logging.info("HLS already ready, exiting processing thread.")
             return
        logging.warning("HLS not ready, but lock file exists. Assuming another process is active or failed.")
        return

    logging.info("Starting video processing job...")
    try:
        ensure_dir(BASE_DIR)
        with open(PROCESSING_LOCK_FILE, 'w') as f:
            f.write(f'Processing started at: {time.strftime("%Y-%m-%d %H:%M:%S")}')
        logging.info(f"Created processing lock file: {PROCESSING_LOCK_FILE}")

        if os.path.exists(PROCESSING_ERROR_FILE):
            logging.warning("Removing previous error file.")
            try: os.remove(PROCESSING_ERROR_FILE)
            except OSError as e: logging.error(f"Could not remove previous error file: {e}")

        ensure_dir(STATIC_DIR) # Ensure static dir exists before HLS check/creation
        ensure_dir(HLS_DIR)    # Ensure HLS dir exists

        if not check_ffmpeg():
            logging.error("ffmpeg check failed. Aborting processing.")
            return

        download_path = os.path.join(DOWNLOAD_DIR, DOWNLOADED_FILENAME)
        if not download_video(VIDEO_URL, download_path):
            logging.error("Download step failed. Aborting processing.")
            return

        if not transcode_to_hls(download_path, HLS_DIR, RESOLUTIONS):
            logging.error("Transcoding step failed.")
            return

        logging.info("Processing job completed successfully.")

    except Exception as e:
        error_msg = f"Critical unexpected error in processing job: {e}"
        logging.error(error_msg, exc_info=True)
        try:
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
        except IOError as io_err: logging.error(f"Failed to write critical error to file: {io_err}")
    # finally:
        # Consider whether to remove the lock file on success/failure
        # Current logic keeps it.


# === Flask Routes ===

@app.route('/')
def index():
    """Serves the main HTML page displaying status and video player."""
    error_message = None
    is_processing = False
    is_hls_ready = os.path.exists(HLS_READY_FILE)

    if os.path.exists(PROCESSING_ERROR_FILE):
        try:
            with open(PROCESSING_ERROR_FILE, 'r') as f:
                error_message = f.read()
            logging.warning(f"Found error file: {PROCESSING_ERROR_FILE}")
        except Exception as e:
            error_message = f"Could not read error file: {e}"
            logging.error(error_message)
    elif not is_hls_ready and os.path.exists(PROCESSING_LOCK_FILE):
        is_processing = True
        logging.info("Processing lock file exists and HLS is not ready -> Status: Processing")
    elif not is_hls_ready and not os.path.exists(PROCESSING_LOCK_FILE):
         if not (processing_thread and processing_thread.is_alive()):
              logging.warning("HLS not ready and no lock file. Attempting to start processing thread.")
              start_processing_thread()
              is_processing = True # Assume it will start processing
         else:
              logging.warning("HLS not ready, no lock file, but thread is alive? State unclear.")
              error_message = "Server state is unclear. Processing thread active but no lock file."

    logging.info(f"Rendering index: hls_ready={is_hls_ready}, processing={is_processing}, error exists={bool(error_message)}")
    return render_template('index.html',
                           hls_ready=is_hls_ready,
                           processing=is_processing,
                           error=error_message)

@app.route('/hls/<path:filename>')
def serve_hls_files(filename):
    """Serves HLS playlist and segment files (.m3u8, .ts)."""
    hls_directory = HLS_DIR
    logging.debug(f"Request for HLS file: {filename} from directory {hls_directory}")
    if '..' in filename or filename.startswith('/'):
        logging.warning(f"Directory traversal attempt blocked for: {filename}")
        abort(403)
    try:
        return send_from_directory(hls_directory, filename, conditional=True)
    except FileNotFoundError:
        logging.warning(f"HLS file not found: {os.path.join(hls_directory, filename)}")
        abort(404)
    except Exception as e:
        logging.error(f"Error serving HLS file {filename}: {e}", exc_info=True)
        abort(500)


# === Application Startup & Background Thread ===
processing_thread = None

def start_processing_thread():
    """Starts the video processing in a background thread if not already running/finished."""
    global processing_thread
    if processing_thread and processing_thread.is_alive():
        logging.info("Processing thread is already running.")
        return
    if os.path.exists(HLS_READY_FILE):
        logging.info("HLS already ready. No need to start processing thread.")
        return
    if os.path.exists(PROCESSING_LOCK_FILE):
         logging.warning("HLS not ready, but lock file exists. Not starting new thread.")
         return

    logging.info("Starting background processing thread...")
    processing_thread = threading.Thread(target=run_processing_job, name="ProcessingThread", daemon=True)
    processing_thread.start()

# --- Ensure processing starts when the application boots ---
start_processing_thread()


# === Main Execution Block ===
if __name__ == '__main__':
    # Use port 8080 for Replit webview proxy
    # Set debug=False for stability on Replit
    app.run(host='0.0.0.0', port=8000, debug=False)


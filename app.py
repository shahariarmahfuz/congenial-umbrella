import os
import subprocess
import requests
import threading
import logging
import time
import shutil # ডিরেক্টরি পরিষ্কার করার জন্য
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
# <<<--- ফেসবুক URL - এটি অস্থায়ী হতে পারে --->>>
VIDEO_URL = "https://video-mxp1-1.xx.fbcdn.net/o1/v/t2/f2/m69/AQM8S3pFxa70tno6zop7jYr1U16B60EHmFPInE6TGBwoOaJnQOXYtCml3Qkpv-p01h-Mq8WY8cuwf4HDp-EFVkCJ.mp4?strext=1&_nc_cat=103&_nc_sid=5e9851&_nc_ht=video-mxp1-1.xx.fbcdn.net&_nc_ohc=PsDTzb3w2UsQ7kNvwEdobVT&efg=eyJ2ZW5jb2RlX3RhZyI6Inhwdl9wcm9ncmVzc2l2ZS5GQUNFQk9PSy4uQzMuNzIwLmRhc2hfaDI2NC1iYXNpYy1nZW4yXzcyMHAiLCJ4cHZfYXNzZXRfaWQiOjE0MDc5NDYzMjM1NTI4NTksInZpX3VzZWNhc2VfaWQiOjEwMTIyLCJkdXJhdGlvbl9zIjoyMDAsInVybGdlbl9zb3VyY2UiOiJ3d3cifQ%3D%3D&ccb=17-1&vs=f2a9875f9f41aa3d&_nc_vs=HBksFQIYOnBhc3N0aHJvdWdoX2V2ZXJzdG9yZS9HTmhqVUIzRVlmM3FMbU1DQVBzVVd5WFBEaXNHYm1kakFBQUYVAALIAQAVAhg6cGFzc3Rocm91Z2hfZXZlcnN0b3JlL0dJTkhVaDB6TFExRGFSOEZBR2tockdVaUNUdEdickZxQUFBRhUCAsgBACgAGAAbAogHdXNlX29pbAExEnByb2dyZXNzaXZlX3JlY2lwZQExFQAAJrap08ehoYAFFQIoAkMzLBdAaRiLQ5WBBhgZZGFzaF9oMjY0LWJhc2ljLWdlbjJfNzIwcBEAdQIA&_nc_zt=28&oh=00_AfEkU99vSfDRfVji51klRkyvAj5hml5FUlj3hFYozoLfGg&oe=68056F07&dl=1"
# <<<------------------------------------------>>>

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, 'downloads')
STATIC_DIR = os.path.join(BASE_DIR, 'static')
HLS_DIR = os.path.join(STATIC_DIR, 'hls')
DOWNLOADED_FILENAME = "source_video.mp4"
MASTER_PLAYLIST_NAME = "master.m3u8"

# Standard resolutions (height, width - assuming 16:9, video_bitrate, audio_bitrate)
# স্ট্যান্ডার্ড রেজোলিউশন ফরম্যাট ব্যবহার করা হচ্ছে
RESOLUTIONS = [
    (360, 640, '800k', '96k'),
    (480, 854, '1400k', '128k'),
    (720, 1280, '2800k', '128k') # Assuming input is at least 720p
]
FFMPEG_TIMEOUT = 1800 # 30 minutes per resolution

# === State Management Files ===
PROCESSING_LOCK_FILE = os.path.join(BASE_DIR, '.processing.lock')
HLS_READY_FILE = os.path.join(HLS_DIR, '.hls_ready')
DOWNLOAD_COMPLETE_FILE = os.path.join(DOWNLOAD_DIR, '.download_complete')
PROCESSING_ERROR_FILE = os.path.join(BASE_DIR, '.processing.error')

# === Helper Functions ===

def ensure_dir(directory):
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
            logging.info(f"Created directory: {directory}")
        except OSError as e:
            logging.error(f"Failed to create directory {directory}: {e}")
            raise

def cleanup_previous_run():
    """Removes old state files and directories before starting."""
    files_to_remove = [PROCESSING_LOCK_FILE, HLS_READY_FILE, DOWNLOAD_COMPLETE_FILE, PROCESSING_ERROR_FILE]
    dirs_to_remove = [HLS_DIR, DOWNLOAD_DIR] # Remove HLS and downloads dir for a clean run

    for file_path in files_to_remove:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logging.info(f"Removed previous state file: {file_path}")
            except OSError as e:
                logging.warning(f"Could not remove previous state file {file_path}: {e}")

    for dir_path in dirs_to_remove:
        if os.path.exists(dir_path):
            try:
                shutil.rmtree(dir_path) # Use shutil.rmtree for directories
                logging.info(f"Removed previous directory: {dir_path}")
            except OSError as e:
                logging.warning(f"Could not remove previous directory {dir_path}: {e}")

    # Recreate necessary directories after cleanup
    ensure_dir(STATIC_DIR)
    ensure_dir(HLS_DIR)
    ensure_dir(DOWNLOAD_DIR)


def check_ffmpeg():
    try:
        # Use -loglevel error to suppress verbose output unless there's an error
        result = subprocess.run(['ffmpeg', '-version', '-loglevel', 'error'], check=True, capture_output=True, text=True, timeout=10)
        logging.info("ffmpeg check successful.")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
        error_msg = f"ffmpeg not found or failed check: {e}"
        if isinstance(e, subprocess.CalledProcessError):
            error_msg += f"\nSTDERR: {e.stderr}"
        logging.error(error_msg)
        try:
            with open(PROCESSING_ERROR_FILE, 'w') as f:
                f.write(f"Fatal Error: ffmpeg is required but not found or not working.\nDetails: {e}")
        except IOError as io_err:
             logging.error(f"Failed to write ffmpeg error to file: {io_err}")
        return False

def download_video(url, dest_path):
    dest_dir = os.path.dirname(dest_path)
    # ensure_dir(dest_dir) # Called in cleanup

    if os.path.exists(DOWNLOAD_COMPLETE_FILE):
         logging.info("Download marker file found. Skipping download as cleanup wasn't intended.")
         if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
             return True
         else:
             logging.warning(f"Marker file exists but video file '{dest_path}' is missing. Re-running.")
             # Proceed to download if marker exists but file doesn't (marker should be removed by cleanup)

    logging.info(f"Starting download from URL to {dest_path}...")
    logging.warning("Facebook URLs might be temporary and could expire.")
    try:
        # if os.path.exists(dest_path): os.remove(dest_path) # Cleanup should handle this

        with requests.get(url, stream=True, timeout=60) as r: # Increased timeout slightly
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))
            bytes_downloaded = 0
            start_time = time.time()
            logging.info(f"Downloading {'{:.2f}'.format(total_size / (1024*1024)) if total_size > 0 else 'unknown size'} MB...")

            with open(dest_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192*4): # Slightly larger chunk size
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)
                        bytes_downloaded += len(chunk)

            end_time = time.time()
            if bytes_downloaded == 0 and total_size != 0: # Check if nothing was downloaded
                 raise ValueError(f"Downloaded 0 bytes but content-length was {total_size}.")
            if os.path.exists(dest_path) and os.path.getsize(dest_path) == 0 and bytes_downloaded > 0:
                raise ValueError("Downloaded file is empty despite receiving bytes.")

            download_duration = end_time - start_time
            download_speed = (bytes_downloaded / (1024*1024)) / (download_duration + 1e-6) # Avoid division by zero
            logging.info(f"Download complete ({bytes_downloaded / (1024*1024):.2f} MB) in {download_duration:.2f}s ({download_speed:.2f} MB/s).")

        with open(DOWNLOAD_COMPLETE_FILE, 'w') as f: f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
        return True

    except requests.exceptions.RequestException as e:
        error_msg = f"Download failed (Network/HTTP Error): {e}"
    except (IOError, ValueError, Exception) as e:
        error_msg = f"Download failed (File/System/Other Error): {e}"

    logging.error(error_msg)
    try:
        with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
    except IOError as io_err: logging.error(f"Failed to write download error to file: {io_err}")
    if os.path.exists(dest_path):
        try: os.remove(dest_path)
        except OSError as e: logging.warning(f"Could not remove failed download artifact {dest_path}: {e}")
    return False

def transcode_to_hls(input_path, output_base_dir, resolutions):
    if os.path.exists(HLS_READY_FILE):
        logging.info("HLS ready marker file found. Skipping transcoding.")
        return True

    if not os.path.exists(input_path) or os.path.getsize(input_path) == 0:
        error_msg = f"Input video file not found or is empty: {input_path}"
        logging.error(error_msg)
        with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
        return False

    logging.info(f"Starting HLS transcoding from {input_path} into {output_base_dir}...")
    # ensure_dir(output_base_dir) # Called in cleanup

    master_playlist_content = "#EXTM3U\n#EXT-X-VERSION:3\n"
    ffmpeg_commands = []
    resolution_details_for_master = []

    # Prepare ffmpeg commands for each resolution
    for height, width, v_bitrate, a_bitrate in resolutions:
        res_output_dir = os.path.join(output_base_dir, str(height))
        ensure_dir(res_output_dir) # Ensure sub-directory exists
        # Use relative path for playlist reference in master
        relative_playlist_path = f'{height}/playlist.m3u8'
        segment_filename_pattern = os.path.join(res_output_dir, 'segment%05d.ts') # Use more digits for longer videos
        absolute_playlist_path = os.path.join(res_output_dir, 'playlist.m3u8')

        cmd = [
            'ffmpeg', '-hide_banner', '-y', # Hide banner, overwrite output without asking
            '-i', input_path,
            '-vf', f'scale={width}:{height}', # Use fixed width and height
            '-c:v', 'libx264', '-preset', 'veryfast', # Faster encoding preset
            '-profile:v', 'baseline', # Baseline profile for broader compatibility
            '-level', '3.1',          # Level compatible with many devices
            '-crf', '23',             # Constant Rate Factor (quality vs size)
            '-b:v', v_bitrate, '-maxrate', v_bitrate, '-bufsize', f'{int(v_bitrate[:-1])*2}k', # Bitrate control
            '-c:a', 'aac', '-ac', '2', '-ar', '44100', '-b:a', a_bitrate, # Audio settings (use 44.1kHz)
            '-f', 'hls',
            '-hls_time', '4',           # Shorter segment duration (e.g., 4 seconds)
            '-hls_playlist_type', 'vod', # Indicate it's Video on Demand
            '-hls_list_size', '0',        # Keep all segments in the playlist (for VOD)
            '-hls_segment_filename', segment_filename_pattern,
            # '-hls_flags', 'delete_segments', # Don't delete segments for VOD replay
            absolute_playlist_path
        ]
        ffmpeg_commands.append({'cmd': cmd, 'height': height})
        bandwidth = int(v_bitrate[:-1]) * 1000 + int(a_bitrate[:-1]) * 1000
        resolution_details_for_master.append({
            'bandwidth': bandwidth,
            'width': width,
            'height': height,
            'playlist_path': relative_playlist_path
        })

    # Execute ffmpeg commands sequentially
    start_time_total = time.time()
    for item in ffmpeg_commands:
        cmd = item['cmd']
        height = item['height']
        logging.info(f"Running ffmpeg for {height}p...")
        logging.debug(f"Command: {' '.join(cmd)}")
        start_time_res = time.time()
        try:
            result = subprocess.run(
                cmd, check=True, capture_output=True, text=True, timeout=FFMPEG_TIMEOUT, encoding='utf-8'
            )
            end_time_res = time.time()
            logging.info(f"ffmpeg finished successfully for {height}p in {end_time_res - start_time_res:.2f}s.")
            logging.debug(f"ffmpeg output for {height}p:\n{result.stderr}") # Log stderr for info
        except subprocess.CalledProcessError as e:
            error_msg = (f"Transcoding failed for {height}p (ffmpeg exit code {e.returncode}).\n"
                         f"Command: {' '.join(e.cmd)}\n"
                         f"STDERR:\n{e.stderr}")
            logging.error(error_msg)
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
            return False
        except subprocess.TimeoutExpired as e:
            error_msg = f"Transcoding timed out for {height}p after {FFMPEG_TIMEOUT} seconds.\nCommand: {' '.join(e.cmd)}"
            logging.error(error_msg)
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
            return False
        except Exception as e:
            error_msg = f"Unexpected error during transcoding for {height}p: {e}"
            logging.error(error_msg, exc_info=True)
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
            return False

    # Create master playlist with corrected RESOLUTION format
    logging.info("All resolutions transcoded.")
    for detail in resolution_details_for_master:
        # Using standard WIDTHxHEIGHT format
        master_playlist_content += f'#EXT-X-STREAM-INF:BANDWIDTH={detail["bandwidth"]},RESOLUTION={detail["width"]}x{detail["height"]}\n'
        master_playlist_content += f'{detail["playlist_path"]}\n'

    master_playlist_path = os.path.join(output_base_dir, MASTER_PLAYLIST_NAME)
    try:
        with open(master_playlist_path, 'w') as f:
            f.write(master_playlist_content)
        logging.info(f"Master playlist created: {master_playlist_path}")
        # Create ready marker only after master playlist is written
        with open(HLS_READY_FILE, 'w') as f:
             f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
        logging.info("HLS processing complete. Ready marker created.")
        end_time_total = time.time()
        logging.info(f"Total transcoding time: {end_time_total - start_time_total:.2f}s")
        return True
    except IOError as e:
        error_msg = f"Failed to write master playlist or ready file: {e}"
        logging.error(error_msg)
        with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
        return False

def run_processing_job():
    """Main job: clean, check ffmpeg, download, transcode."""
    # Check lock file robustly
    if os.path.exists(PROCESSING_LOCK_FILE):
        try:
            with open(PROCESSING_LOCK_FILE, 'r') as f:
                start_time_str = f.read().split(': ')[-1]
                # Optional: Check if lock file is too old, indicating a stale process
                logging.warning(f"Lock file found (Processing started around {start_time_str}). Assuming another process is active or failed.")
        except Exception as e:
             logging.warning(f"Lock file found but couldn't read start time: {e}. Assuming process is active/failed.")

        if os.path.exists(HLS_READY_FILE):
             logging.info("HLS already ready, ignoring lock file and exiting processing thread.")
             if os.path.exists(PROCESSING_LOCK_FILE): # Clean up lock if HLS is ready
                 try: os.remove(PROCESSING_LOCK_FILE)
                 except OSError as e: logging.warning(f"Could not remove lock file even though HLS is ready: {e}")
             return
        else:
            # HLS not ready, lock exists - let the assumed process finish or timeout
            logging.warning("HLS not ready and lock file exists. Will not start new process.")
            return

    logging.info("Starting new video processing job...")
    lock_acquired = False
    try:
        # --- Perform Cleanup ---
        cleanup_previous_run()
        # --- Cleanup Done ---

        # --- Acquire Lock ---
        ensure_dir(BASE_DIR) # Ensure base dir exists for lock file
        with open(PROCESSING_LOCK_FILE, 'w') as f:
            f.write(f'Processing started at: {time.strftime("%Y-%m-%d %H:%M:%S")}')
        lock_acquired = True
        logging.info(f"Created processing lock file: {PROCESSING_LOCK_FILE}")
        # --- Lock Acquired ---

        # --- Start Processing Steps ---
        if not check_ffmpeg():
            logging.error("ffmpeg check failed. Aborting.")
            # Error file written by check_ffmpeg()
            return # Exit thread

        download_path = os.path.join(DOWNLOAD_DIR, DOWNLOADED_FILENAME)
        if not download_video(VIDEO_URL, download_path):
            logging.error("Download step failed. Aborting.")
            # Error file written by download_video()
            return # Exit thread

        if not transcode_to_hls(input_path=download_path,
                                output_base_dir=HLS_DIR,
                                resolutions=RESOLUTIONS):
            logging.error("Transcoding step failed. Aborting.")
            # Error file written by transcode_to_hls()
            return # Exit thread
        # --- Processing Steps Done ---

        logging.info("Processing job completed successfully.")
        # Remove lock file on successful completion
        if lock_acquired:
            try:
                os.remove(PROCESSING_LOCK_FILE)
                logging.info(f"Removed processing lock file on success: {PROCESSING_LOCK_FILE}")
                lock_acquired = False
            except OSError as e:
                logging.error(f"Failed to remove lock file on success: {e}")


    except Exception as e:
        error_msg = f"Critical unexpected error in processing job: {e}"
        logging.error(error_msg, exc_info=True)
        try:
            # Ensure error file reflects the critical failure
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
        except IOError as io_err: logging.error(f"Failed to write critical error to file: {io_err}")
    finally:
        # Ensure lock file is removed if an *uncaught* exception occurred *after* acquiring it
        # Note: Specific failures in download/transcode steps handle their errors and return early.
        # This finally block might be redundant if errors are handled well, but acts as a safeguard.
        # Consider carefully if you want the lock removed on *any* error.
        # If the process might be recoverable or needs inspection, leaving the lock might be desired.
        # Current logic removes lock only on success. If an error occurred, the lock remains.
         pass


# === Flask Routes ===

@app.route('/')
def index():
    """Serves the main HTML page displaying status and video player."""
    error_message = None
    processing_status = "idle" # States: idle, processing, ready, error

    if os.path.exists(PROCESSING_ERROR_FILE):
        try:
            with open(PROCESSING_ERROR_FILE, 'r') as f:
                error_message = f.read()
            processing_status = "error"
            logging.warning(f"Status: Error file found at {PROCESSING_ERROR_FILE}")
        except Exception as e:
            error_message = f"Could not read error file: {e}"
            processing_status = "error" # Treat as error if file exists but unreadable
            logging.error(error_message)

    elif os.path.exists(HLS_READY_FILE):
        processing_status = "ready"
        logging.info("Status: HLS ready marker found.")
        # Clean up lock file if HLS is ready but lock somehow persists
        if os.path.exists(PROCESSING_LOCK_FILE):
            try:
                 os.remove(PROCESSING_LOCK_FILE)
                 logging.warning("Removed orphan lock file as HLS is ready.")
            except OSError as e:
                 logging.warning(f"Could not remove orphan lock file: {e}")

    elif os.path.exists(PROCESSING_LOCK_FILE):
        processing_status = "processing"
        logging.info("Status: Processing lock file found, HLS not ready.")

    else: # No error, not ready, no lock -> Should be idle or starting
        # Check if thread needs starting (it might have died unexpectedly without cleanup)
        if not (processing_thread and processing_thread.is_alive()):
             logging.warning("Status: Idle/Unknown. No state files found and thread not alive. Attempting to restart processing.")
             start_processing_thread() # Attempt to restart
             processing_status = "processing" # Assume it will start
        else:
             # This state should ideally not be reached if locking is correct
             logging.warning("Status: Ambiguous - No state files, but processing thread is alive.")
             processing_status = "processing" # Report as processing
             error_message = "Server state is ambiguous (thread alive, no lock file)."


    logging.info(f"Rendering index with status: {processing_status}")
    return render_template('index.html',
                           status=processing_status, # Pass single status string
                           error=error_message)

@app.route('/hls/<path:filename>')
def serve_hls_files(filename):
    """Serves HLS playlist and segment files safely."""
    hls_directory = os.path.abspath(HLS_DIR) # Use absolute path for security check
    safe_filename = filename.replace('../', '').lstrip('/') # Basic sanitization
    requested_path = os.path.abspath(os.path.join(hls_directory, safe_filename))

    # Security Check: Ensure the requested path is within the HLS directory
    if not requested_path.startswith(hls_directory):
        logging.warning(f"Directory traversal attempt blocked for: {filename} -> {requested_path}")
        abort(403) # Forbidden

    logging.debug(f"Serving HLS file: {safe_filename} from {hls_directory}")

    # Add CORS headers for HLS files
    response = None
    try:
        response = send_from_directory(hls_directory, safe_filename, conditional=True)
        # Add CORS headers - crucial for HLS playback in browsers
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Origin, Content-Type, Accept'
        return response
    except FileNotFoundError:
        logging.warning(f"HLS file not found: {requested_path}")
        abort(404)
    except Exception as e:
        logging.error(f"Error serving HLS file {safe_filename}: {e}", exc_info=True)
        abort(500)

# === Application Startup & Background Thread ===
processing_thread = None

def start_processing_thread():
    """Starts the video processing in a background thread if conditions are met."""
    global processing_thread
    if processing_thread and processing_thread.is_alive():
        logging.info("Processing thread already running.")
        return
    if os.path.exists(HLS_READY_FILE):
        logging.info("HLS already marked as ready. No processing needed.")
        # Ensure lock is removed if it exists
        if os.path.exists(PROCESSING_LOCK_FILE):
            try: os.remove(PROCESSING_LOCK_FILE)
            except OSError as e: logging.warning(f"Could not remove stale lock file: {e}")
        return
    # Check lock file again *just before* starting thread
    if os.path.exists(PROCESSING_LOCK_FILE):
         logging.warning("Lock file exists. Preventing start of new processing thread.")
         return

    logging.info("Starting background processing thread...")
    processing_thread = threading.Thread(target=run_processing_job, name="ProcessingThread", daemon=True)
    processing_thread.start()

# --- Start processing immediately when the Flask app starts ---
# This might run *before* the server is ready for requests in some deployment scenarios,
# but is generally okay for development/simple deployments.
start_processing_thread()

# === Main Execution Block ===
if __name__ == '__main__':
    # Note: Setting debug=True with threads can sometimes cause issues or duplicate thread starts.
    # Keep debug=False for stability, especially when dealing with background tasks.
    # Use port 8000 or another suitable port.
    app.run(host='0.0.0.0', port=8000, debug=False)

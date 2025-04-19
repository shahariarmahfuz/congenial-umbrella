import os
import subprocess
import requests
import threading
import logging
import time
import shutil
from flask import Flask, render_template, send_from_directory, abort, Response, request, redirect, url_for, flash

# === Logging Configuration ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# === Flask App Initialization ===
app = Flask(__name__)
# Flash messages need a secret key
app.secret_key = os.urandom(24) # বা একটি নির্দিষ্ট স্ট্রিং ব্যবহার করুন

# === Configuration Constants ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, 'uploads') # Changed from DOWNLOAD_DIR
STATIC_DIR = os.path.join(BASE_DIR, 'static')
HLS_DIR = os.path.join(STATIC_DIR, 'hls')
ALLOWED_EXTENSIONS = {'mp4', 'mov', 'avi', 'mkv', 'webm'} # অনুমোদিত ভিডিও এক্সটেনশন
UPLOADED_FILENAME = "source_video" # মূল ফাইলের নামের বেস (এক্সটেনশন যোগ হবে)
MASTER_PLAYLIST_NAME = "master.m3u8"

# Define desired output resolutions and bitrates (height, video_bitrate, audio_bitrate)
RESOLUTIONS = [
    (360, '800k', '96k'),
    (480, '1400k', '128k'),
    (720, '2800k', '128k') # যদি মূল ভিডিও 720p বা তার বেশি হয়
]
FFMPEG_TIMEOUT = 1800 # Timeout for each ffmpeg command in seconds (30 minutes)

# === State Management Files ===
# These now represent the state of the *last* processed video
PROCESSING_LOCK_FILE = os.path.join(BASE_DIR, '.processing.lock')
HLS_READY_FILE = os.path.join(HLS_DIR, '.hls_ready')
PROCESSING_ERROR_FILE = os.path.join(BASE_DIR, '.processing.error')
# .download_complete is removed

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

def clear_previous_state():
    """Removes old state files and HLS data before processing a new video."""
    logging.info("Clearing previous state files and HLS directory...")
    files_to_remove = [PROCESSING_LOCK_FILE, HLS_READY_FILE, PROCESSING_ERROR_FILE]
    for f_path in files_to_remove:
        if os.path.exists(f_path):
            try:
                os.remove(f_path)
                logging.info(f"Removed state file: {f_path}")
            except OSError as e:
                logging.warning(f"Could not remove state file {f_path}: {e}")

    if os.path.exists(HLS_DIR):
        try:
            # Remove everything inside HLS_DIR but not the directory itself
            for item in os.listdir(HLS_DIR):
                item_path = os.path.join(HLS_DIR, item)
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                else:
                    os.remove(item_path)
            logging.info(f"Cleared contents of HLS directory: {HLS_DIR}")
        except Exception as e:
            logging.error(f"Could not clear HLS directory {HLS_DIR}: {e}")
    else:
        ensure_dir(HLS_DIR) # Ensure it exists if it was somehow deleted

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
            # Write error to file so the main page can display it
            with open(PROCESSING_ERROR_FILE, 'w') as f:
                f.write(f"Fatal Error: ffmpeg is required but not found or not working.\nDetails: {e}")
        except IOError as io_err:
             logging.error(f"Failed to write ffmpeg error to file: {io_err}")
        return False

def allowed_file(filename):
    """Checks if the uploaded file extension is allowed."""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def transcode_to_hls(input_path, output_base_dir, resolutions):
    """Transcodes video to HLS format for multiple resolutions."""
    # HLS_READY_FILE check is removed here, assumes this is called only when needed

    if not os.path.exists(input_path) or os.path.getsize(input_path) == 0:
        error_msg = f"Input video file not found or is empty: {input_path}"
        logging.error(error_msg)
        with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
        return False

    logging.info(f"Starting HLS transcoding from {input_path} into {output_base_dir}...")
    ensure_dir(output_base_dir) # Ensure HLS dir exists
    master_playlist_content = "#EXTM3U\n#EXT-X-VERSION:3\n"
    ffmpeg_commands = []
    resolution_details_for_master = []

    # Prepare ffmpeg commands
    for height, v_bitrate, a_bitrate in resolutions:
        res_output_dir = os.path.join(output_base_dir, str(height))
        ensure_dir(res_output_dir)
        relative_playlist_path = os.path.join(str(height), 'playlist.m3u8') # Relative path for master playlist
        segment_path_pattern = os.path.join(res_output_dir, 'segment%03d.ts')
        absolute_playlist_path = os.path.join(res_output_dir, 'playlist.m3u8')

        cmd = [
            'ffmpeg', '-i', input_path,
            '-vf', f'scale=-2:{height}',
            '-c:v', 'libx264', '-crf', '23', '-preset', 'veryfast', # Consider 'fast' or 'medium' for better quality/size ratio if speed isn't critical
            '-b:v', v_bitrate, '-maxrate', v_bitrate, '-bufsize', f'{int(v_bitrate[:-1])*2}k',
            '-c:a', 'aac', '-ar', '48000', '-b:a', a_bitrate,
            '-f', 'hls',
            '-hls_time', '6',        # Segment duration in seconds
            '-hls_list_size', '0',   # Keep all segments in the playlist
            '-hls_segment_filename', segment_path_pattern,
            '-hls_flags', 'delete_segments', # Don't append, start fresh
            absolute_playlist_path
        ]
        ffmpeg_commands.append({'cmd': cmd, 'height': height})
        bandwidth = int(v_bitrate[:-1]) * 1000 + int(a_bitrate[:-1]) * 1000 # Approximate bandwidth
        resolution_details_for_master.append({
            'bandwidth': bandwidth,
            'height': height,
            'playlist_path': relative_playlist_path
        })

    # Execute ffmpeg commands
    start_time_total = time.time()
    success = True
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
            # Log stderr for potential warnings even on success
            if result.stderr:
                 logging.debug(f"ffmpeg stderr for {height}p:\n{result.stderr[-1000:]}") # Log last part of stderr
        except subprocess.CalledProcessError as e:
            error_msg = (f"Transcoding failed for {height}p (ffmpeg exit code {e.returncode}).\n"
                         f"Input: {input_path}\n"
                         f"Command: {' '.join(e.cmd)}\n"
                         f"STDERR (last 1000 chars):\n...{e.stderr[-1000:]}")
            logging.error(error_msg)
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
            success = False
            break # Stop processing other resolutions if one fails
        except subprocess.TimeoutExpired as e:
            error_msg = (f"Transcoding timed out for {height}p after {FFMPEG_TIMEOUT} seconds.\n"
                         f"Input: {input_path}\n"
                         f"Command: {' '.join(e.cmd)}")
            logging.error(error_msg)
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
            success = False
            break
        except Exception as e:
            error_msg = (f"Unexpected error during transcoding for {height}p: {e}\n"
                         f"Input: {input_path}")
            logging.error(error_msg, exc_info=True)
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
            success = False
            break

    if not success:
        logging.error("Aborting HLS generation due to ffmpeg error.")
        return False

    # Create master playlist if all transcodes succeeded
    logging.info("All resolutions transcoded successfully.")
    for detail in resolution_details_for_master:
        master_playlist_content += f'#EXT-X-STREAM-INF:BANDWIDTH={detail["bandwidth"]},RESOLUTION=x{detail["height"]}\n' # Assuming width is variable
        master_playlist_content += f'{detail["playlist_path"]}\n'

    master_playlist_path = os.path.join(output_base_dir, MASTER_PLAYLIST_NAME)
    try:
        with open(master_playlist_path, 'w') as f:
            f.write(master_playlist_content)
        logging.info(f"Master playlist created successfully at {master_playlist_path}")
        # Create the ready marker only after the master playlist is written
        with open(HLS_READY_FILE, 'w') as f:
             f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
        logging.info("HLS processing complete. Ready marker created.")
        end_time_total = time.time()
        logging.info(f"Total transcoding time: {end_time_total - start_time_total:.2f}s")
        return True
    except IOError as e:
        error_msg = f"Failed to write master playlist or ready marker: {e}"
        logging.error(error_msg)
        with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
        return False

def run_processing_job(uploaded_video_path):
    """The main job function to transcode the uploaded video, run in a thread."""
    # Lock file is created *before* calling this thread now
    logging.info(f"Processing job started for: {uploaded_video_path}")

    try:
        if not check_ffmpeg():
            logging.error("ffmpeg check failed inside processing job. Aborting.")
            # Error file should have been created by check_ffmpeg
            return # Exit the thread

        if not transcode_to_hls(uploaded_video_path, HLS_DIR, RESOLUTIONS):
            logging.error("Transcoding step failed.")
            # transcode_to_hls should have written to PROCESSING_ERROR_FILE
            return # Exit the thread

        logging.info("Processing job completed successfully.")

    except Exception as e:
        error_msg = f"Critical unexpected error in processing job for {uploaded_video_path}: {e}"
        logging.error(error_msg, exc_info=True)
        try:
            with open(PROCESSING_ERROR_FILE, 'w') as f: f.write(error_msg)
        except IOError as io_err: logging.error(f"Failed to write critical error to file: {io_err}")

    finally:
        # Remove the processing lock file regardless of success or failure
        if os.path.exists(PROCESSING_LOCK_FILE):
            try:
                os.remove(PROCESSING_LOCK_FILE)
                logging.info(f"Removed processing lock file: {PROCESSING_LOCK_FILE}")
            except OSError as e:
                logging.error(f"Failed to remove processing lock file: {e}")
        # Clean up the uploaded source file after processing (optional)
        # if os.path.exists(uploaded_video_path):
        #     try:
        #         os.remove(uploaded_video_path)
        #         logging.info(f"Removed uploaded source file: {uploaded_video_path}")
        #     except OSError as e:
        #         logging.warning(f"Could not remove uploaded source file {uploaded_video_path}: {e}")


# === Flask Routes ===

@app.route('/', methods=['GET'])
def index():
    """Serves the main page: upload form or video player based on state."""
    error_message = None
    is_processing = False
    is_hls_ready = os.path.exists(HLS_READY_FILE)

    if os.path.exists(PROCESSING_ERROR_FILE):
        try:
            with open(PROCESSING_ERROR_FILE, 'r') as f:
                error_message = f.read()
            logging.warning(f"Found error file: {PROCESSING_ERROR_FILE}")
            # Keep the error file until the next successful processing
        except Exception as e:
            error_message = f"Could not read error file: {e}"
            logging.error(error_message)

    # Check for processing lock *after* checking for errors
    if not error_message and os.path.exists(PROCESSING_LOCK_FILE):
        is_processing = True
        logging.info("Processing lock file exists -> Status: Processing")

    # Determine which template to render
    if is_hls_ready and not is_processing and not error_message:
        # Video is ready to be played
        logging.info("Rendering index.html (Player)")
        return render_template('index.html', hls_ready=True, processing=False, error=None)
    elif is_processing:
        # Video is currently being processed
        logging.info("Rendering index.html (Processing)")
        # Pass processing=True to index.html which can show a message
        return render_template('index.html', hls_ready=False, processing=True, error=None)
    elif error_message:
        # An error occurred during the last processing attempt
        logging.info("Rendering upload.html (Error occurred)")
        # Show the upload form again, along with the error
        return render_template('upload.html', error=error_message)
    else:
        # No video processed yet, or previous one finished/failed without leaving HLS ready
        logging.info("Rendering upload.html (Ready for upload)")
        return render_template('upload.html', error=None)


@app.route('/upload', methods=['POST'])
def upload_file():
    """Handles the video file upload."""
    if 'video' not in request.files:
        flash('No file part')
        return redirect(request.url) # Redirect back to the upload page implicitly via '/'

    file = request.files['video']
    if file.filename == '':
        flash('No selected file')
        return redirect(url_for('index'))

    if file and allowed_file(file.filename):
        # Clear previous state before starting new upload processing
        clear_previous_state()

        # Create necessary directories
        ensure_dir(UPLOAD_DIR)
        ensure_dir(STATIC_DIR)
        ensure_dir(HLS_DIR)

        # Save the uploaded file
        original_filename = file.filename
        file_ext = original_filename.rsplit('.', 1)[1].lower()
        # Use a fixed name for simplicity, prevents needing dynamic paths in transcode
        # If handling multiple users/videos simultaneously, use unique names (e.g., uuid)
        save_path = os.path.join(UPLOAD_DIR, f"{UPLOADED_FILENAME}.{file_ext}")

        try:
            if os.path.exists(save_path):
                 logging.warning(f"Removing existing upload file: {save_path}")
                 os.remove(save_path)
            file.save(save_path)
            logging.info(f"File uploaded successfully and saved to {save_path}")

            # Create the lock file *before* starting the thread
            try:
                 with open(PROCESSING_LOCK_FILE, 'w') as f:
                     f.write(f'Processing started at: {time.strftime("%Y-%m-%d %H:%M:%S")} for {original_filename}')
                 logging.info(f"Created processing lock file: {PROCESSING_LOCK_FILE}")
            except IOError as e:
                 flash(f"Error creating lock file: {e}")
                 logging.error(f"Error creating lock file: {e}")
                 # Consider cleaning up the uploaded file here
                 return redirect(url_for('index'))


            # Start the transcoding process in a background thread
            logging.info("Starting background processing thread...")
            processing_thread = threading.Thread(
                target=run_processing_job,
                args=(save_path,), # Pass the saved file path
                name="ProcessingThread",
                daemon=True # Allows app to exit even if thread is running (use False if job must finish)
            )
            processing_thread.start()

            flash(f'Upload successful! "{original_filename}" is now processing.')
            # Redirect to index, which will show the "Processing" status
            return redirect(url_for('index'))

        except Exception as e:
            flash(f'Error saving or processing file: {e}')
            logging.error(f"Error during file save or thread start: {e}", exc_info=True)
            # Clean up lock file if it was created
            if os.path.exists(PROCESSING_LOCK_FILE): os.remove(PROCESSING_LOCK_FILE)
            return redirect(url_for('index'))

    else:
        flash('Invalid file type. Allowed types are: ' + ', '.join(ALLOWED_EXTENSIONS))
        return redirect(url_for('index'))


@app.route('/hls/<path:filename>')
def serve_hls_files(filename):
    """Serves HLS playlist and segment files (.m3u8, .ts)."""
    hls_directory = HLS_DIR
    # Security check: Prevent accessing files outside HLS_DIR
    if '..' in filename or filename.startswith('/'):
        logging.warning(f"Directory traversal attempt blocked for: {filename}")
        abort(403) # Forbidden

    logging.debug(f"Request for HLS file: {filename} from directory {hls_directory}")
    file_path = os.path.join(hls_directory, filename)

    # Ensure the requested file is *within* the intended HLS structure
    # Check if the file exists relative to the HLS_DIR
    if not os.path.exists(file_path):
         logging.warning(f"HLS file not found: {file_path}")
         abort(404)

    # Additional check: Ensure the resolved path is truly under HLS_DIR
    # This helps prevent issues with symlinks etc. pointing outside
    abs_hls_dir = os.path.abspath(hls_directory)
    abs_file_path = os.path.abspath(file_path)
    if not abs_file_path.startswith(abs_hls_dir):
         logging.error(f"Security risk: Attempt to access file outside HLS directory resolved path: {abs_file_path}")
         abort(403) # Forbidden

    try:
        # Use conditional=True for better caching (sends 304 Not Modified if browser cache is valid)
        return send_from_directory(hls_directory, filename, conditional=True)
    except FileNotFoundError:
        # This check is somewhat redundant due to the os.path.exists above, but good practice
        logging.warning(f"HLS file not found by send_from_directory: {file_path}")
        abort(404)
    except Exception as e:
        logging.error(f"Error serving HLS file {filename}: {e}", exc_info=True)
        abort(500) # Internal Server Error


# === Application Startup ===
# No background thread started automatically on boot anymore

# === Main Execution Block ===
if __name__ == '__main__':
    # Check ffmpeg availability on startup
    if not check_ffmpeg():
        logging.critical("ffmpeg is required but not available. The application might not function correctly.")
        # The app will still run, but processing will fail and show an error on the page.

    # Ensure base directories exist on startup
    ensure_dir(BASE_DIR)
    ensure_dir(UPLOAD_DIR)
    ensure_dir(STATIC_DIR)
    ensure_dir(HLS_DIR)

    # Use port 8000 or another preferred port
    # Set debug=False for production/stability
    app.run(host='0.0.0.0', port=8000, debug=False)
        

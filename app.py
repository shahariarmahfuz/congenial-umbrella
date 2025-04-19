import os
import subprocess
import requests # Note: requests is no longer used but kept for potential future use
import threading
import logging
import time
import shutil
import uuid # <<<--- Unique ID জেনারেট করার জন্য
from flask import Flask, render_template, send_from_directory, abort, Response, request, redirect, url_for, flash

# === Logging Configuration ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# === Flask App Initialization ===
app = Flask(__name__)
app.secret_key = os.urandom(24) # Flash messages need a secret key

# === Configuration Constants ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, 'uploads')
STATIC_DIR = os.path.join(BASE_DIR, 'static')
HLS_DIR = os.path.join(STATIC_DIR, 'hls')
ALLOWED_EXTENSIONS = {'mp4', 'mov', 'avi', 'mkv', 'webm'}
SOURCE_VIDEO_BASENAME = "source" # আপলোড করা ফাইলের বেস নাম (এক্সটেনশন সহ সেভ হবে)
MASTER_PLAYLIST_NAME = "master.m3u8"

# State filenames (relative to the video's HLS directory)
PROCESSING_LOCK_FILENAME = ".processing.lock"
HLS_READY_FILENAME = ".hls_ready"
PROCESSING_ERROR_FILENAME = ".processing.error"


# Define desired output resolutions and bitrates
RESOLUTIONS = [
    (360, '800k', '96k'),
    (480, '1400k', '128k'),
    (720, '2800k', '128k')
]
FFMPEG_TIMEOUT = 1800 # 30 minutes

# === Helper Functions ===

def ensure_dir(directory):
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
            logging.info(f"Created directory: {directory}")
        except OSError as e:
            logging.error(f"Failed to create directory {directory}: {e}")
            raise

def check_ffmpeg():
    try:
        # Check if ffmpeg is accessible
        result = subprocess.run(['ffmpeg', '-version'], check=True, capture_output=True, text=True, timeout=10)
        logging.info("ffmpeg check successful.")
        return True
    except Exception as e:
        logging.error(f"ffmpeg check failed: {e}")
        # This is a global check, hard to associate with a specific video ID here
        # Consider how to handle this if ffmpeg disappears mid-operation
        return False

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def clear_hls_directory_contents(directory_path):
    """Removes contents of a specific HLS directory before transcoding."""
    if not os.path.isdir(directory_path):
        logging.warning(f"HLS directory not found for clearing: {directory_path}")
        return
    logging.info(f"Clearing contents of HLS directory: {directory_path}")
    try:
        for item in os.listdir(directory_path):
            item_path = os.path.join(directory_path, item)
            try:
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                else:
                    os.remove(item_path)
            except Exception as e:
                logging.error(f"Could not remove item {item_path}: {e}")
    except Exception as e:
        logging.error(f"Could not list or clear directory {directory_path}: {e}")


def transcode_to_hls(video_id, input_path, output_base_dir, resolutions):
    """Transcodes video to HLS, specific to a video_id."""
    # output_base_dir is now specific, e.g., static/hls/video_id

    if not os.path.exists(input_path) or os.path.getsize(input_path) == 0:
        error_msg = f"[{video_id}] Input video file not found or is empty: {input_path}"
        logging.error(error_msg)
        # Write error state file within the video's HLS directory
        error_file_path = os.path.join(output_base_dir, PROCESSING_ERROR_FILENAME)
        try:
            with open(error_file_path, 'w') as f: f.write(error_msg)
        except IOError as e:
             logging.error(f"[{video_id}] Failed to write error state file {error_file_path}: {e}")
        return False

    logging.info(f"[{video_id}] Starting HLS transcoding from {input_path} into {output_base_dir}...")
    ensure_dir(output_base_dir) # Ensure video specific HLS dir exists

    master_playlist_content = "#EXTM3U\n#EXT-X-VERSION:3\n"
    ffmpeg_commands = []
    resolution_details_for_master = []

    # Prepare ffmpeg commands relative to the video's HLS directory
    for height, v_bitrate, a_bitrate in resolutions:
        # Resolution specific dir inside the video's HLS dir
        res_output_dir = os.path.join(output_base_dir, str(height))
        ensure_dir(res_output_dir)

        # Paths relative to the video's HLS directory for the master playlist
        relative_playlist_path = os.path.join(str(height), 'playlist.m3u8')
        # Absolute path for ffmpeg segment output
        segment_path_pattern = os.path.join(res_output_dir, 'segment%03d.ts')
        # Absolute path for ffmpeg playlist output
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
            '-hls_flags', 'delete_segments', # Start fresh for this video ID
            absolute_playlist_path
        ]
        ffmpeg_commands.append({'cmd': cmd, 'height': height})
        bandwidth = int(v_bitrate[:-1]) * 1000 + int(a_bitrate[:-1]) * 1000
        resolution_details_for_master.append({
            'bandwidth': bandwidth,
            'height': height,
            'playlist_path': relative_playlist_path # Path relative to master playlist
        })

    # Execute ffmpeg commands
    start_time_total = time.time()
    success = True
    error_file_path = os.path.join(output_base_dir, PROCESSING_ERROR_FILENAME)

    for item in ffmpeg_commands:
        cmd = item['cmd']
        height = item['height']
        logging.info(f"[{video_id}] Running ffmpeg for {height}p...")
        logging.debug(f"[{video_id}] Command: {' '.join(cmd)}")
        start_time_res = time.time()
        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=FFMPEG_TIMEOUT)
            end_time_res = time.time()
            logging.info(f"[{video_id}] ffmpeg finished successfully for {height}p in {end_time_res - start_time_res:.2f}s.")
            if result.stderr:
                 logging.debug(f"[{video_id}] ffmpeg stderr for {height}p:\n{result.stderr[-1000:]}")
        except subprocess.CalledProcessError as e:
            error_msg = (f"[{video_id}] Transcoding failed for {height}p (ffmpeg exit code {e.returncode}).\n"
                         f"Input: {input_path}\n"
                         f"Command: {' '.join(e.cmd)}\n"
                         f"STDERR (last 1000 chars):\n...{e.stderr[-1000:]}")
            logging.error(error_msg)
            try:
                with open(error_file_path, 'w') as f: f.write(error_msg)
            except IOError as io_err:
                logging.error(f"[{video_id}] Failed to write error state file {error_file_path}: {io_err}")
            success = False
            break
        except subprocess.TimeoutExpired as e:
            error_msg = (f"[{video_id}] Transcoding timed out for {height}p after {FFMPEG_TIMEOUT} seconds.\n"
                         f"Input: {input_path}\n"
                         f"Command: {' '.join(e.cmd)}")
            logging.error(error_msg)
            try:
                with open(error_file_path, 'w') as f: f.write(error_msg)
            except IOError as io_err:
                 logging.error(f"[{video_id}] Failed to write error state file {error_file_path}: {io_err}")
            success = False
            break
        except Exception as e:
            error_msg = f"[{video_id}] Unexpected error during transcoding for {height}p: {e}\nInput: {input_path}"
            logging.error(error_msg, exc_info=True)
            try:
                 with open(error_file_path, 'w') as f: f.write(error_msg)
            except IOError as io_err:
                logging.error(f"[{video_id}] Failed to write error state file {error_file_path}: {io_err}")
            success = False
            break

    if not success:
        logging.error(f"[{video_id}] Aborting HLS generation due to ffmpeg error.")
        return False

    # Create master playlist if all transcodes succeeded
    logging.info(f"[{video_id}] All resolutions transcoded successfully.")
    for detail in resolution_details_for_master:
        master_playlist_content += f'#EXT-X-STREAM-INF:BANDWIDTH={detail["bandwidth"]},RESOLUTION=x{detail["height"]}\n'
        master_playlist_content += f'{detail["playlist_path"]}\n' # These paths are relative to the master playlist

    master_playlist_path = os.path.join(output_base_dir, MASTER_PLAYLIST_NAME)
    ready_file_path = os.path.join(output_base_dir, HLS_READY_FILENAME)
    try:
        with open(master_playlist_path, 'w') as f:
            f.write(master_playlist_content)
        logging.info(f"[{video_id}] Master playlist created successfully at {master_playlist_path}")

        # Create the ready marker file
        with open(ready_file_path, 'w') as f:
             f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
        logging.info(f"[{video_id}] HLS processing complete. Ready marker created: {ready_file_path}")
        end_time_total = time.time()
        logging.info(f"[{video_id}] Total transcoding time: {end_time_total - start_time_total:.2f}s")
        return True
    except IOError as e:
        error_msg = f"[{video_id}] Failed to write master playlist or ready marker: {e}"
        logging.error(error_msg)
        # Write error state file
        try:
            with open(error_file_path, 'w') as f: f.write(error_msg)
        except IOError as io_err:
             logging.error(f"[{video_id}] Failed to write error state file {error_file_path}: {io_err}")
        return False


def run_processing_job(video_id, uploaded_video_path, hls_output_dir):
    """The main job function to transcode a specific uploaded video."""
    lock_file_path = os.path.join(hls_output_dir, PROCESSING_LOCK_FILENAME)
    error_file_path = os.path.join(hls_output_dir, PROCESSING_ERROR_FILENAME)
    ready_file_path = os.path.join(hls_output_dir, HLS_READY_FILENAME)

    logging.info(f"[{video_id}] Processing job started in thread.")
    # Lock file should already exist, created by the /upload route

    try:
        # Clear any previous HLS content for this ID before starting
        clear_hls_directory_contents(hls_output_dir)
        # Also remove potential stale error/ready files from previous runs for this ID
        if os.path.exists(error_file_path): os.remove(error_file_path)
        if os.path.exists(ready_file_path): os.remove(ready_file_path)

        if not check_ffmpeg():
             # This is a global issue, hard to report per-video reliably here
             error_msg = f"[{video_id}] ffmpeg check failed. Aborting processing."
             logging.critical(error_msg)
             with open(error_file_path, 'w') as f: f.write(error_msg)
             return # Exit the thread

        if not transcode_to_hls(video_id, uploaded_video_path, hls_output_dir, RESOLUTIONS):
            logging.error(f"[{video_id}] Transcoding step failed.")
            # transcode_to_hls should have written the error file
            return # Exit the thread

        logging.info(f"[{video_id}] Processing job completed successfully.")

    except Exception as e:
        error_msg = f"[{video_id}] Critical unexpected error in processing job: {e}"
        logging.error(error_msg, exc_info=True)
        try:
            with open(error_file_path, 'w') as f: f.write(error_msg)
        except IOError as io_err: logging.error(f"[{video_id}] Failed to write critical error to file: {io_err}")

    finally:
        # Remove the processing lock file for this specific video ID
        if os.path.exists(lock_file_path):
            try:
                os.remove(lock_file_path)
                logging.info(f"[{video_id}] Removed processing lock file: {lock_file_path}")
            except OSError as e:
                logging.error(f"[{video_id}] Failed to remove processing lock file: {e}")
        # Optional: Clean up the source uploaded file after processing
        # Be careful with this if you might need to reprocess later
        # if os.path.exists(uploaded_video_path):
        #     try:
        #         os.remove(uploaded_video_path)
        #         logging.info(f"[{video_id}] Removed uploaded source file: {uploaded_video_path}")
        #     except OSError as e:
        #         logging.warning(f"[{video_id}] Could not remove source file {uploaded_video_path}: {e}")


# === Flask Routes ===

@app.route('/', methods=['GET'])
def index():
    """Serves the main upload page."""
    logging.info("Rendering index.html (upload form)")
    # This page now only shows the upload form and potentially global messages
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handles the video file upload, assigns ID, starts processing."""
    if 'video' not in request.files:
        flash('কোন ফাইল অংশ নেই।')
        return redirect(url_for('index'))

    file = request.files['video']
    if file.filename == '':
        flash('কোন ফাইল নির্বাচন করা হয়নি।')
        return redirect(url_for('index'))

    if file and allowed_file(file.filename):
        original_filename = file.filename
        file_ext = original_filename.rsplit('.', 1)[1].lower()
        video_id = str(uuid.uuid4()) # Generate unique ID
        logging.info(f"Upload received for '{original_filename}', assigned ID: {video_id}")

        # Define paths specific to this video ID
        video_upload_dir = os.path.join(UPLOAD_DIR, video_id)
        video_hls_dir = os.path.join(HLS_DIR, video_id)
        save_path = os.path.join(video_upload_dir, f"{SOURCE_VIDEO_BASENAME}.{file_ext}")
        lock_file_path = os.path.join(video_hls_dir, PROCESSING_LOCK_FILENAME)

        try:
            # Create necessary directories for this video
            ensure_dir(video_upload_dir)
            ensure_dir(video_hls_dir) # HLS dir needed for lock file

            # Check if already processing this (unlikely with UUIDs, but good practice)
            if os.path.exists(lock_file_path):
                flash(f"ভিডিও আইডি {video_id} ইতিমধ্যে প্রসেস করা হচ্ছে।")
                logging.warning(f"[{video_id}] Upload attempt while lock file exists.")
                return redirect(url_for('video_status', video_id=video_id)) # Redirect to status

            # Save the uploaded file
            file.save(save_path)
            logging.info(f"[{video_id}] File saved to {save_path}")

            # Create the lock file *before* starting the thread
            with open(lock_file_path, 'w') as f:
                f.write(f'Processing started at: {time.strftime("%Y-%m-%d %H:%M:%S")} for {original_filename}')
            logging.info(f"[{video_id}] Created processing lock file: {lock_file_path}")

            # Start the transcoding process in a background thread
            logging.info(f"[{video_id}] Starting background processing thread...")
            processing_thread = threading.Thread(
                target=run_processing_job,
                args=(video_id, save_path, video_hls_dir), # Pass ID and paths
                name=f"ProcessingThread-{video_id}",
                daemon=True
            )
            processing_thread.start()

            flash(f'"{original_filename}" আপলোড সফল! আইডি: {video_id}. ভিডিওটি এখন প্রসেস হচ্ছে।')
            # Redirect to the specific video's status page
            return redirect(url_for('video_status', video_id=video_id))

        except Exception as e:
            flash(f'ফাইল সেভ বা প্রসেসিং শুরু করতে ত্রুটি: {e}')
            logging.error(f"[{video_id or 'UNKNOWN'}] Error during upload handling: {e}", exc_info=True)
            # Clean up lock file if it was created
            if 'lock_file_path' in locals() and os.path.exists(lock_file_path):
                try: os.remove(lock_file_path)
                except OSError: pass
            # Clean up HLS dir if created
            if 'video_hls_dir' in locals() and os.path.exists(video_hls_dir):
                 try: shutil.rmtree(video_hls_dir)
                 except OSError: pass
            # Clean up upload dir if created
            if 'video_upload_dir' in locals() and os.path.exists(video_upload_dir):
                 try: shutil.rmtree(video_upload_dir)
                 except OSError: pass
            return redirect(url_for('index'))

    else:
        flash('অবৈধ ফাইলের প্রকার। অনুমোদিত প্রকারগুলি: ' + ', '.join(ALLOWED_EXTENSIONS))
        return redirect(url_for('index'))


@app.route('/video/<video_id>')
def video_status(video_id):
    """Displays status (processing, ready, error) or player for a specific video ID."""
    logging.info(f"[{video_id}] Status check request received.")
    video_hls_dir = os.path.join(HLS_DIR, video_id)

    # Define paths for state files within the video's HLS directory
    lock_file_path = os.path.join(video_hls_dir, PROCESSING_LOCK_FILENAME)
    ready_file_path = os.path.join(video_hls_dir, HLS_READY_FILENAME)
    error_file_path = os.path.join(video_hls_dir, PROCESSING_ERROR_FILENAME)

    status = 'not_found' # Default status
    error_message = None
    hls_ready = False
    processing = False

    if not os.path.isdir(video_hls_dir) and not os.path.isdir(os.path.join(UPLOAD_DIR, video_id)):
         logging.warning(f"[{video_id}] HLS and Upload directory not found.")
         # Consider flashing a message here? Or just render 'not_found' status.
         # abort(404) might be too harsh if it was just deleted.
         pass # Will render with status 'not_found'
    elif os.path.exists(error_file_path):
        status = 'error'
        try:
            with open(error_file_path, 'r') as f:
                error_message = f.read()
            logging.warning(f"[{video_id}] Found error file: {error_file_path}")
        except Exception as e:
            error_message = f"Could not read error file: {e}"
            logging.error(f"[{video_id}] {error_message}")
    elif os.path.exists(ready_file_path):
        status = 'ready'
        hls_ready = True
        logging.info(f"[{video_id}] Found ready file: {ready_file_path}")
    elif os.path.exists(lock_file_path):
        status = 'processing'
        processing = True
        logging.info(f"[{video_id}] Found lock file: {lock_file_path}")
    else:
         # Directories exist, but no state files found. Maybe deleted or never processed?
         logging.warning(f"[{video_id}] No state files found in {video_hls_dir}. Assuming 'not_found' or incomplete.")
         status = 'not_found' # Or perhaps 'unknown'?


    logging.info(f"[{video_id}] Rendering video_status.html with status: {status}")
    return render_template('video_status.html',
                           video_id=video_id,
                           status=status,
                           hls_ready=hls_ready, # Explicitly pass for template logic
                           processing=processing, # Explicitly pass for template logic
                           error=error_message)


@app.route('/hls/<video_id>/<path:filename>')
def serve_hls_files(video_id, filename):
    """Serves HLS files for a specific video ID."""
    video_hls_dir = os.path.join(HLS_DIR, video_id)
    logging.debug(f"[{video_id}] Request for HLS file: {filename} from directory {video_hls_dir}")

    # Basic security checks
    if '..' in filename or filename.startswith('/') or '..' in video_id or video_id.startswith('/'):
        logging.warning(f"[{video_id}] Directory traversal attempt blocked for: {filename}")
        abort(403) # Forbidden

    # Construct the full path to the requested file
    file_path = os.path.join(video_hls_dir, filename)

    # Check if the directory for the video ID exists
    if not os.path.isdir(video_hls_dir):
        logging.warning(f"[{video_id}] HLS directory not found: {video_hls_dir}")
        abort(404)

    # Use safe_join (or similar) in production Flask for better security, but basic check here:
    abs_hls_dir = os.path.abspath(video_hls_dir)
    abs_file_path = os.path.abspath(file_path)
    if not abs_file_path.startswith(abs_hls_dir):
         logging.error(f"[{video_id}] Security risk: Attempt to access file outside HLS directory: {abs_file_path}")
         abort(403) # Forbidden

    # Check if the specific file exists within that directory
    if not os.path.isfile(file_path):
        logging.warning(f"[{video_id}] HLS file not found: {file_path}")
        abort(404)

    try:
        # Serve the file from the specific video's HLS directory
        return send_from_directory(video_hls_dir, filename, conditional=True)
    except FileNotFoundError:
        # Should be caught by isfile check above, but belt-and-suspenders
        logging.warning(f"[{video_id}] HLS file not found by send_from_directory: {file_path}")
        abort(404)
    except Exception as e:
        logging.error(f"[{video_id}] Error serving HLS file {filename}: {e}", exc_info=True)
        abort(500) # Internal Server Error


# === Application Startup ===
# Check ffmpeg on startup
if not check_ffmpeg():
    logging.critical("ffmpeg is required but not available. Processing will fail.")

# Ensure base directories exist on startup
ensure_dir(BASE_DIR)
ensure_dir(UPLOAD_DIR)
ensure_dir(STATIC_DIR)
ensure_dir(HLS_DIR)


# === Main Execution Block ===
if __name__ == '__main__':
    # Set debug=False for stability
    app.run(host='0.0.0.0', port=8000, debug=False)

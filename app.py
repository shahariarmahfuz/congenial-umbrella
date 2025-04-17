# app.py (Main Server)
import os
import subprocess
import requests
import threading
import logging
import time
import json
import uuid # For unique video IDs
import shutil # For file operations like moving
from flask import Flask, request, render_template, send_from_directory, abort, jsonify, url_for, redirect

# === Logging Configuration ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# === Flask App Initialization ===
app = Flask(__name__)

# === Configuration Constants ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, 'uploads')
STATIC_DIR = os.path.join(BASE_DIR, 'static')
HLS_DIR = os.path.join(STATIC_DIR, 'hls') # Main dir for all HLS videos
VIDEO_STATUS_FILE = os.path.join(BASE_DIR, 'video_status.json') # Simple JSON DB for status
MAX_CONTENT_LENGTH = 1024 * 1024 * 1024 # 1 GB upload limit (adjust as needed)
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH

# URLs for the converter servers (Replace with actual IPs/hostnames and ports)
CONVERTER_SERVERS = {
    "360p": "https://three60p-q9ho.onrender.com", # Example URL for 360p converter
    "480p": "https://four80p-dqur.onrender.com", # Example URL for 480p converter
    "720p": "https://seven20p-tq7s.onrender.com"  # Example URL for 720p converter
}

# Define desired output resolutions and bitrates (height, video_bitrate, audio_bitrate)
# Passed to converters
RESOLUTIONS = {
    "360p": (360, '800k', '96k'),
    "480p": (480, '1400k', '128k'),
    "720p": (720, '2800k', '128k')
}

FFMPEG_TIMEOUT = 3600 # Timeout for each ffmpeg command (on converters) in seconds (60 minutes)

# === State Management ===
# In-memory dictionary to hold video processing status.
# For persistence, load/save from VIDEO_STATUS_FILE.
# Structure: { "video_id": {"status": "...", "error": "...", "qualities_done": [], "manifest_path": "..."} }
video_processing_status = {}
status_lock = threading.Lock() # To safely access/modify the status dict

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

def load_status():
    """Loads video status from the JSON file."""
    global video_processing_status
    with status_lock:
        if os.path.exists(VIDEO_STATUS_FILE):
            try:
                with open(VIDEO_STATUS_FILE, 'r') as f:
                    video_processing_status = json.load(f)
                    logging.info(f"Loaded video status for {len(video_processing_status)} videos.")
            except (json.JSONDecodeError, IOError) as e:
                logging.error(f"Failed to load status file {VIDEO_STATUS_FILE}: {e}. Starting fresh.")
                video_processing_status = {}
        else:
             logging.info("Status file not found. Starting fresh.")
             video_processing_status = {}

def save_status():
    """Saves the current video status to the JSON file."""
    with status_lock:
        try:
            # Create a copy to avoid issues if dict changes during write
            status_copy = video_processing_status.copy()
            with open(VIDEO_STATUS_FILE, 'w') as f:
                json.dump(status_copy, f, indent=4)
                logging.debug(f"Saved video status for {len(status_copy)} videos.")
        except IOError as e:
            logging.error(f"Failed to save status file {VIDEO_STATUS_FILE}: {e}")
        except Exception as e:
             logging.error(f"Unexpected error saving status: {e}", exc_info=True)


def update_video_status(video_id, status=None, error=None, quality_done=None, manifest_path=None):
    """Updates the status of a video."""
    with status_lock:
        if video_id not in video_processing_status:
            video_processing_status[video_id] = {"status": "unknown", "error": None, "qualities_done": [], "manifest_path": None}

        if status is not None:
            video_processing_status[video_id]["status"] = status
        if error is not None:
             # Append errors if multiple occur
             current_error = video_processing_status[video_id].get("error")
             if current_error:
                 video_processing_status[video_id]["error"] = f"{current_error}\n{error}"
             else:
                video_processing_status[video_id]["error"] = error
        if quality_done is not None:
            if quality_done not in video_processing_status[video_id]["qualities_done"]:
                 video_processing_status[video_id]["qualities_done"].append(quality_done)
        if manifest_path is not None:
            video_processing_status[video_id]["manifest_path"] = manifest_path

    # Save status immediately after update
    save_status()
    logging.info(f"Updated status for {video_id}: {video_processing_status.get(video_id, {})}")


def get_video_status(video_id):
    """Gets the status of a video."""
    with status_lock:
        return video_processing_status.get(video_id, {"status": "not_found", "error": None, "qualities_done": []})

# === Background Processing Task ===

def process_video_distributed(video_id, source_filepath):
    """
    Manages the distributed video processing workflow in a background thread.
    1. Calls converters.
    2. Polls for completion.
    3. Collects HLS files.
    4. Creates master manifest.
    """
    thread_name = threading.current_thread().name
    logging.info(f"[{thread_name}] Starting distributed processing for video_id: {video_id}")
    update_video_status(video_id, status="processing_distribution")

    # Ensure target HLS directory exists for this video
    video_hls_dir = os.path.join(HLS_DIR, video_id)
    ensure_dir(video_hls_dir)

    # 1. Trigger conversion on each server
    # Use the main server's external URL for converters to download the source
    # Note: This assumes the main server is reachable from converters.
    # If running locally, 'localhost' might work, but in production, use the actual IP/domain.
    try:
        # Get the hostname/IP Flask is running on (might need configuration in production)
        server_name = request.host.split(':')[0] # Gets 'localhost' or IP/domain from request
        server_port = request.host.split(':')[1] if ':' in request.host else 5000 # Default Flask port if not specified
        # Ensure scheme (http/https)
        scheme = request.scheme
        source_download_url = f"{scheme}://{server_name}:{server_port}{url_for('download_source_video', video_id=video_id)}"
        logging.info(f"[{thread_name}] Source download URL for converters: {source_download_url}")
    except Exception as e:
        logging.error(f"[{thread_name}] Failed to generate source download URL: {e}. Trying fallback.")
        # Fallback (adjust if needed, e.g., read from config)
        source_download_url = f"http://127.0.0.1:5000/download_source/{video_id}"
        logging.warning(f"[{thread_name}] Using fallback source URL: {source_download_url}")


    conversion_jobs = {}
    active_converters = {}

    for quality, base_url in CONVERTER_SERVERS.items():
        converter_url = f"{base_url}/convert"
        payload = {
            "video_id": video_id,
            "source_url": source_download_url,
            "target_height": RESOLUTIONS[quality][0],
            "video_bitrate": RESOLUTIONS[quality][1],
            "audio_bitrate": RESOLUTIONS[quality][2],
            "timeout": FFMPEG_TIMEOUT
        }
        try:
            response = requests.post(converter_url, json=payload, timeout=15) # Short timeout for triggering
            response.raise_for_status()
            result = response.json()
            if result.get("status") == "processing_started":
                logging.info(f"[{thread_name}] Successfully triggered conversion for {quality} on {base_url}")
                conversion_jobs[quality] = "pending"
                active_converters[quality] = base_url
            else:
                logging.error(f"[{thread_name}] Failed to start conversion for {quality} on {base_url}: {result.get('error', 'Unknown error')}")
                update_video_status(video_id, status="error", error=f"Failed to start {quality}: {result.get('error', 'Unknown')}")
                conversion_jobs[quality] = "failed_to_start"

        except requests.exceptions.RequestException as e:
            logging.error(f"[{thread_name}] Error contacting converter {base_url} for {quality}: {e}")
            update_video_status(video_id, status="error", error=f"Error contacting {quality} converter: {e}")
            conversion_jobs[quality] = "failed_to_start"

    if not active_converters:
         logging.error(f"[{thread_name}] No converters started successfully for {video_id}. Aborting.")
         update_video_status(video_id, status="error", error="No conversion jobs could be started.")
         # Clean up original upload file? Optional.
         # try: os.remove(source_filepath) except OSError: pass
         return


    # 2. Poll for completion
    update_video_status(video_id, status="processing_polling")
    polling_interval = 15 # seconds
    max_polling_time = FFMPEG_TIMEOUT + 600 # Allow extra time beyond ffmpeg timeout
    start_polling_time = time.time()

    qualities_to_monitor = list(active_converters.keys())
    completed_qualities = []
    failed_qualities = []

    while qualities_to_monitor:
        if time.time() - start_polling_time > max_polling_time:
            timeout_error = f"Polling timed out after {max_polling_time}s for qualities: {qualities_to_monitor}"
            logging.error(f"[{thread_name}] {timeout_error}")
            for q in qualities_to_monitor: failed_qualities.append(q)
            update_video_status(video_id, status="error", error=timeout_error)
            break # Exit polling loop

        quality_to_check = qualities_to_monitor.pop(0) # Process one by one
        base_url = active_converters[quality_to_check]
        status_url = f"{base_url}/status/{video_id}"

        try:
            response = requests.get(status_url, timeout=10)
            response.raise_for_status()
            result = response.json()
            current_status = result.get("status")
            logging.debug(f"[{thread_name}] Status check for {quality_to_check} ({video_id}): {current_status}")

            if current_status == "completed":
                logging.info(f"[{thread_name}] Conversion completed for {quality_to_check} ({video_id})")
                completed_qualities.append(quality_to_check)
                update_video_status(video_id, quality_done=quality_to_check) # Mark this quality as done
            elif current_status == "error":
                error_msg = result.get("error", "Unknown error from converter")
                logging.error(f"[{thread_name}] Conversion failed for {quality_to_check} ({video_id}): {error_msg}")
                failed_qualities.append(quality_to_check)
                update_video_status(video_id, status="error", error=f"{quality_to_check} failed: {error_msg}")
            else: # Still processing or pending
                qualities_to_monitor.append(quality_to_check) # Add back to the end of the queue

        except requests.exceptions.RequestException as e:
            logging.error(f"[{thread_name}] Error polling status for {quality_to_check} ({video_id}) from {base_url}: {e}")
            # Decide if this is a fatal error for this quality or retry
            # For simplicity, let's count it as a failure after one error
            failed_qualities.append(quality_to_check)
            update_video_status(video_id, status="error", error=f"Error polling {quality_to_check}: {e}")


        # Wait before next check, only if there are still items to monitor
        if qualities_to_monitor:
            time.sleep(polling_interval)

    # Check results
    if not completed_qualities:
         logging.error(f"[{thread_name}] No qualities completed successfully for {video_id}.")
         # Status already set to error if polling timed out or individual qualities failed
         if get_video_status(video_id)['status'] != "error":
             update_video_status(video_id, status="error", error="No qualities finished successfully.")
         # Clean up original upload file? Optional.
         # try: os.remove(source_filepath) except OSError: pass
         return

    logging.info(f"[{thread_name}] Completed qualities for {video_id}: {completed_qualities}")
    logging.warning(f"[{thread_name}] Failed qualities for {video_id}: {failed_qualities}")

    # 3. Collect HLS files for completed qualities
    update_video_status(video_id, status="processing_collecting")
    collected_manifests = {} # quality -> relative_path

    for quality in completed_qualities:
        base_url = active_converters[quality]
        files_list_url = f"{base_url}/files/{video_id}" # Endpoint to list files
        quality_hls_dir = os.path.join(video_hls_dir, quality)
        ensure_dir(quality_hls_dir)

        try:
            # Get the list of files from the converter
            response = requests.get(files_list_url, timeout=15)
            response.raise_for_status()
            files_to_download = response.json().get("files", [])
            logging.info(f"[{thread_name}] Files to download for {quality}: {files_to_download}")

            if not files_to_download:
                 logging.warning(f"[{thread_name}] Converter for {quality} reported completion, but returned no files.")
                 update_video_status(video_id, status="error", error=f"No files returned by {quality} converter after completion.")
                 continue # Skip this quality


            playlist_filename = None
            for filename in files_to_download:
                if filename.endswith(".m3u8"):
                    playlist_filename = filename

                download_url = f"{base_url}/files/{video_id}/{filename}"
                local_filepath = os.path.join(quality_hls_dir, filename)

                try:
                    with requests.get(download_url, stream=True, timeout=60) as r:
                        r.raise_for_status()
                        with open(local_filepath, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                f.write(chunk)
                    logging.debug(f"[{thread_name}] Downloaded {filename} for {quality} to {local_filepath}")
                except requests.exceptions.RequestException as e_dl:
                    logging.error(f"[{thread_name}] Failed to download {filename} for {quality} from {base_url}: {e_dl}")
                    update_video_status(video_id, status="error", error=f"Failed to download {filename} for {quality}: {e_dl}")
                    # Should we stop collecting for this quality? Yes.
                    collected_manifests.pop(quality, None) # Remove if partially added
                    # Clean up partially downloaded files for this quality
                    try:
                        shutil.rmtree(quality_hls_dir)
                        logging.info(f"Removed partially collected directory: {quality_hls_dir}")
                    except OSError as e_rm:
                         logging.warning(f"Could not remove partial dir {quality_hls_dir}: {e_rm}")
                    break # Stop collecting for this quality


            # If download succeeded for all files in the list and we found a playlist
            if playlist_filename and quality not in get_video_status(video_id).get('error', ''): # Check if an error occurred during download loop
                 relative_playlist_path = f"{quality}/{playlist_filename}" # Path relative to video_id dir
                 collected_manifests[quality] = relative_playlist_path
                 logging.info(f"[{thread_name}] Successfully collected files for {quality}. Manifest: {relative_playlist_path}")


        except requests.exceptions.RequestException as e_list:
            logging.error(f"[{thread_name}] Failed to list files for {quality} from {base_url}: {e_list}")
            update_video_status(video_id, status="error", error=f"Failed to list files from {quality} converter: {e_list}")


    # Check if we collected anything useful
    if not collected_manifests:
         logging.error(f"[{thread_name}] Failed to collect any valid HLS playlists for {video_id}.")
         if get_video_status(video_id)['status'] != "error":
            update_video_status(video_id, status="error", error="Failed to collect any HLS playlists.")
         # Clean up original upload file? Optional.
         # try: os.remove(source_filepath) except OSError: pass
         return

    # 4. Create Master Manifest
    update_video_status(video_id, status="processing_manifest")
    master_playlist_content = ["#EXTM3U", "#EXT-X-VERSION:3"]
    # Sort qualities for consistent master playlist order (e.g., by height)
    sorted_qualities = sorted(collected_manifests.keys(), key=lambda q: RESOLUTIONS[q][0])

    for quality in sorted_qualities:
        resolution_info = RESOLUTIONS[quality]
        height = resolution_info[0]
        bandwidth = int(resolution_info[1].replace('k','000')) # Estimate bandwidth from video bitrate
        playlist_path = collected_manifests[quality]

        master_playlist_content.append(f'#EXT-X-STREAM-INF:BANDWIDTH={bandwidth},RESOLUTION={height}x{height},NAME="{quality}"') # Assuming square pixels for simplicity
        master_playlist_content.append(playlist_path)

    master_playlist_path = os.path.join(video_hls_dir, "master.m3u8")
    try:
        with open(master_playlist_path, "w") as f:
            f.write("\n".join(master_playlist_content) + "\n")
        logging.info(f"[{thread_name}] Created master playlist for {video_id} at {master_playlist_path}")

        # 5. Final Status Update & Cleanup
        relative_master_path = f"{video_id}/master.m3u8"
        update_video_status(video_id, status="ready", manifest_path=relative_master_path)
        logging.info(f"[{thread_name}] Video processing completed successfully for {video_id}!")

        # Clean up the original uploaded file
        try:
            os.remove(source_filepath)
            logging.info(f"[{thread_name}] Removed original source file: {source_filepath}")
        except OSError as e:
            logging.warning(f"[{thread_name}] Could not remove source file {source_filepath}: {e}")

    except IOError as e:
        logging.error(f"[{thread_name}] Failed to write master playlist {master_playlist_path}: {e}")
        update_video_status(video_id, status="error", error=f"Failed to write master playlist: {e}")


# === Flask Routes ===

@app.route('/upload', methods=['GET', 'POST'])
def upload_video():
    if request.method == 'POST':
        if 'video' not in request.files:
            logging.warning("Upload attempt with no 'video' file part.")
            return jsonify({"success": False, "error": "No video file part in the request."}), 400

        file = request.files['video']
        if file.filename == '':
            logging.warning("Upload attempt with no selected file.")
            return jsonify({"success": False, "error": "No selected file."}), 400

        if file: # Check if file exists and has a name
            try:
                video_id = str(uuid.uuid4())
                ensure_dir(UPLOAD_DIR)
                # Sanitize filename basic - consider more robust sanitization
                # filename = secure_filename(file.filename) # Need Werkzeug for this
                # For simplicity, just use video_id
                _, ext = os.path.splitext(file.filename)
                save_filename = f"{video_id}{ext}"
                save_path = os.path.join(UPLOAD_DIR, save_filename)

                file.save(save_path)
                logging.info(f"Video uploaded successfully: {save_path} (ID: {video_id})")

                # Initialize status
                update_video_status(video_id, status="uploaded")

                # Start background processing
                processing_thread = threading.Thread(
                    target=process_video_distributed,
                    args=(video_id, save_path),
                    name=f"Processor-{video_id[:8]}" # Short thread name
                )
                processing_thread.start()

                return jsonify({"success": True, "video_id": video_id})

            except Exception as e:
                 # Catch potential errors during save or thread start
                 logging.error(f"Error during file upload or processing start: {e}", exc_info=True)
                 # Clean up uploaded file if it exists and something went wrong
                 if 'save_path' in locals() and os.path.exists(save_path):
                     try: os.remove(save_path)
                     except OSError: pass
                 # Clean up status entry if created
                 if 'video_id' in locals():
                     with status_lock: video_processing_status.pop(video_id, None)
                     save_status()
                 return jsonify({"success": False, "error": f"An internal error occurred: {e}"}), 500
        else:
             return jsonify({"success": False, "error": "Invalid file."}), 400

    # GET request: Show the upload form
    return render_template('upload.html')


@app.route('/download_source/<video_id>')
def download_source_video(video_id):
    """Allows converter servers to download the original uploaded video."""
    status_info = get_video_status(video_id)
    if status_info['status'] == 'not_found':
        abort(404, description="Video ID not found.")

    # Find the source file (assuming it has the video_id as basename)
    source_file = None
    try:
        for filename in os.listdir(UPLOAD_DIR):
            if filename.startswith(video_id):
                source_file = os.path.join(UPLOAD_DIR, filename)
                break
    except FileNotFoundError:
         logging.error(f"Upload directory {UPLOAD_DIR} not found when trying to serve source for {video_id}")
         abort(500, description="Server configuration error (upload dir missing).")

    if source_file and os.path.exists(source_file):
        logging.info(f"Serving source video {source_file} for download request (video_id: {video_id})")
        return send_from_directory(UPLOAD_DIR, os.path.basename(source_file), as_attachment=False) # Serve inline
    else:
        logging.warning(f"Source file for video_id {video_id} not found in {UPLOAD_DIR}")
        abort(404, description="Source video file not found or already processed.")

@app.route('/status/<video_id>')
def check_video_status(video_id):
    """API endpoint for the frontend to check video status."""
    status_info = get_video_status(video_id)
    return jsonify(status_info)

@app.route('/watch/<video_id>')
def watch_video(video_id):
    """Displays the video player page."""
    status_info = get_video_status(video_id)
    status = status_info.get("status", "not_found")
    error = status_info.get("error")
    hls_ready = (status == "ready")
    processing = status not in ["ready", "error", "not_found"]
    master_playlist_url = None
    if hls_ready and status_info.get("manifest_path"):
         master_playlist_url = url_for('serve_hls', video_id=video_id, filename='master.m3u8')
         # Note: The template needs modification to use master_playlist_url

    # Reuse index.html structure but pass necessary variables
    # You might want to create a dedicated watch.html template
    return render_template('index.html', # Or 'watch.html' if you create it
                           hls_ready=hls_ready,
                           processing=processing,
                           error=error,
                           video_id=video_id,
                           master_playlist_url=master_playlist_url # Pass this to template
                           )


@app.route('/hls/<video_id>/<path:filename>')
def serve_hls(video_id, filename):
    """Serves the master playlist and video segments."""
    video_hls_path = os.path.join(HLS_DIR, video_id)
    if not os.path.exists(video_hls_path):
         logging.warning(f"Attempt to access non-existent HLS path: {video_hls_path}")
         abort(404)

    logging.debug(f"Serving HLS file: {filename} from {video_hls_path}")
    try:
        # Important: Set correct MIME types
        if filename.endswith('.m3u8'):
            mime_type = 'application/vnd.apple.mpegurl'
        elif filename.endswith('.ts'):
            mime_type = 'video/mp2t'
        else:
            mime_type = None # Let Flask guess or default

        response = send_from_directory(video_hls_path, filename, mimetype=mime_type)
        # Add CORS headers if needed, especially if player is on a different domain
        # response.headers.add('Access-Control-Allow-Origin', '*')
        return response

    except FileNotFoundError:
        logging.warning(f"HLS file not found: {filename} in {video_hls_path}")
        abort(404)
    except Exception as e:
         logging.error(f"Error serving HLS file {filename} for {video_id}: {e}", exc_info=True)
         abort(500)


@app.route('/')
def index():
    # Redirect to upload page or show a dashboard
    return redirect(url_for('upload_video'))


# === Initialization ===
if __name__ == '__main__':
    ensure_dir(UPLOAD_DIR)
    ensure_dir(HLS_DIR)
    load_status() # Load existing status on startup
    # Use host='0.0.0.0' to make it accessible on the network
    app.run(debug=True, host='0.0.0.0', port=5000, threaded=True) # Threaded required for background tasks

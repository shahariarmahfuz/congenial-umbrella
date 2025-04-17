
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
# !!! গুরুত্বপূর্ণ: আপনার সার্ভার হোস্টনেম এখানে সেট করুন যদি url_for স্বয়ংক্রিয়ভাবে এটি সনাক্ত করতে না পারে !!!
# উদাহরণ: app.config['SERVER_NAME'] = 'your-domain.com:5000' # порт সহ
# Railway বা Render এর মতো প্ল্যাটফর্মে এটি সাধারণত প্রয়োজন হয় না যদি তারা সঠিক হেডার সেট করে।

# URLs for the converter servers (Replace with actual IPs/hostnames and ports)
# !!! আপনার কনভার্টার সার্ভারের আসল URL দিয়ে এগুলো পরিবর্তন করুন !!!
CONVERTER_SERVERS = {
    "360p": "https://three60p-g9ho.onrender.com", # আপনার 360p কনভার্টারের URL
    "480p": "https://four80p-dgur.onrender.com", # আপনার 480p কনভার্টারের URL
    "720p": "https://seven20p-tq7s.onrender.com"  # আপনার 720p কনভার্টারের URL
}

# Define desired output resolutions and bitrates (height, video_bitrate, audio_bitrate)
RESOLUTIONS = {
    "360p": (360, '800k', '96k'),
    "480p": (480, '1400k', '128k'),
    "720p": (720, '2800k', '128k')
}

FFMPEG_TIMEOUT = 3600 # Timeout for each ffmpeg command (on converters) in seconds (60 minutes)

# === State Management ===
# In-memory dictionary to hold video processing status.
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

    save_status() # Save status immediately after update
    logging.info(f"Updated status for {video_id}: {video_processing_status.get(video_id, {})}")


def get_video_status(video_id):
    """Gets the status of a video."""
    with status_lock:
        # Return a copy to prevent modification outside the lock
        return video_processing_status.get(video_id, {"status": "not_found", "error": None, "qualities_done": []}).copy()

# === Background Processing Task ===

def process_video_distributed(video_id, source_filepath, source_download_url_from_upload):
    """
    Manages the distributed video processing workflow in a background thread.
    Uses the source URL generated *during* the upload request.
    """
    thread_name = threading.current_thread().name
    logging.info(f"[{thread_name}] Starting distributed processing for video_id: {video_id}")
    # Status is already 'uploaded', change to 'processing_distribution'
    update_video_status(video_id, status="processing_distribution")

    video_hls_dir = os.path.join(HLS_DIR, video_id)
    try:
        ensure_dir(video_hls_dir)
    except Exception as e:
         logging.error(f"[{thread_name}] Failed to create HLS directory {video_hls_dir}: {e}")
         update_video_status(video_id, status="error", error=f"Failed to create HLS directory: {e}")
         # Maybe cleanup source file?
         # try: os.remove(source_filepath) except OSError: pass
         return

    # 1. Trigger conversion on each server
    source_download_url = source_download_url_from_upload
    logging.info(f"[{thread_name}] Using pre-generated source download URL for converters: {source_download_url}")

    conversion_jobs = {}
    active_converters = {}
    latest_errors = [] # Collect errors during trigger phase

    for quality, base_url in CONVERTER_SERVERS.items():
        converter_url = f"{base_url}/convert"
        payload = {
            "video_id": video_id,
            "source_url": source_download_url, # Use the passed URL
            "target_height": RESOLUTIONS[quality][0],
            "video_bitrate": RESOLUTIONS[quality][1],
            "audio_bitrate": RESOLUTIONS[quality][2],
            "timeout": FFMPEG_TIMEOUT
        }
        try:
            response = requests.post(converter_url, json=payload, timeout=20) # Increased timeout
            response.raise_for_status() # Check for HTTP errors (4xx, 5xx)
            result = response.json()
            if result.get("status") == "processing_started":
                logging.info(f"[{thread_name}] Successfully triggered conversion for {quality} on {base_url}")
                conversion_jobs[quality] = "pending"
                active_converters[quality] = base_url
            else:
                err_msg = f"Failed to start {quality} on {base_url}: {result.get('error', 'Unknown error from converter')}"
                logging.error(f"[{thread_name}] {err_msg}")
                latest_errors.append(err_msg)
                conversion_jobs[quality] = "failed_to_start"

        except requests.exceptions.Timeout:
             err_msg = f"Timeout contacting converter {base_url} for {quality}"
             logging.error(f"[{thread_name}] {err_msg}")
             latest_errors.append(err_msg)
             conversion_jobs[quality] = "failed_to_start"
        except requests.exceptions.HTTPError as e:
             err_msg = f"HTTP Error contacting converter {base_url} for {quality}: {e.response.status_code} {e.response.reason}"
             logging.error(f"[{thread_name}] {err_msg}")
             latest_errors.append(err_msg)
             conversion_jobs[quality] = "failed_to_start"
        except requests.exceptions.RequestException as e:
            err_msg = f"Error contacting converter {base_url} for {quality}: {e}"
            logging.error(f"[{thread_name}] {err_msg}")
            latest_errors.append(err_msg)
            conversion_jobs[quality] = "failed_to_start"
        except Exception as e: # Catch unexpected errors during request/response handling
             err_msg = f"Unexpected error triggering {quality} on {base_url}: {e}"
             logging.error(f"[{thread_name}] {err_msg}", exc_info=True)
             latest_errors.append(err_msg)
             conversion_jobs[quality] = "failed_to_start"

    # Update status with any errors encountered during triggering
    if latest_errors:
        update_video_status(video_id, error="\n".join(latest_errors))

    # Check if any converters were successfully triggered
    if not active_converters:
         logging.error(f"[{thread_name}] No converters started successfully for {video_id}. Aborting.")
         # Ensure the status reflects the failure
         if get_video_status(video_id).get('status') != 'error':
             update_video_status(video_id, status="error", error="No conversion jobs could be started.")
         # Clean up original upload file? Optional.
         # try: os.remove(source_filepath) except OSError: pass
         return

    # 2. Poll for completion
    update_video_status(video_id, status="processing_polling")
    polling_interval = 20 # Increase interval slightly? seconds
    max_polling_time = FFMPEG_TIMEOUT + 600 # Allow extra time beyond ffmpeg timeout
    start_polling_time = time.time()

    qualities_to_monitor = list(active_converters.keys())
    completed_qualities = []
    failed_qualities = []
    polling_errors = [] # Collect errors during polling phase

    while qualities_to_monitor:
        current_time = time.time()
        if current_time - start_polling_time > max_polling_time:
            timeout_error = f"Polling timed out after {max_polling_time}s for qualities: {qualities_to_monitor}"
            logging.error(f"[{thread_name}] {timeout_error}")
            polling_errors.append(timeout_error)
            for q in qualities_to_monitor: failed_qualities.append(q)
            update_video_status(video_id, error=timeout_error) # Add timeout error
            break # Exit polling loop

        # Wait *before* checking in the loop (except first iteration)
        if current_time != start_polling_time: # Avoid sleep on first check
            time.sleep(polling_interval)

        quality_to_check = qualities_to_monitor.pop(0) # Get next quality from the front
        base_url = active_converters[quality_to_check]
        status_url = f"{base_url}/status/{video_id}"

        try:
            logging.debug(f"[{thread_name}] Polling status for {quality_to_check} at {status_url}")
            response = requests.get(status_url, timeout=15) # Timeout for status check
            response.raise_for_status() # Check for HTTP errors
            result = response.json()
            current_status = result.get("status")
            logging.debug(f"[{thread_name}] Status check response for {quality_to_check} ({video_id}): {current_status}")

            if current_status == "completed":
                logging.info(f"[{thread_name}] Conversion completed for {quality_to_check} ({video_id})")
                completed_qualities.append(quality_to_check)
                update_video_status(video_id, quality_done=quality_to_check) # Mark this quality as done in the main status dict
            elif current_status == "error":
                error_msg = result.get("error", f"Unknown error reported by {quality_to_check} converter")
                logging.error(f"[{thread_name}] Conversion failed for {quality_to_check} ({video_id}): {error_msg}")
                failed_qualities.append(quality_to_check)
                polling_errors.append(f"{quality_to_check} failed: {error_msg}")
                update_video_status(video_id, error=f"{quality_to_check} conversion failed: {error_msg}") # Add specific error
            elif current_status == "pending" or current_status == "processing" or current_status == "downloading":
                 qualities_to_monitor.append(quality_to_check) # Add back to the end of the queue to check later
            else: # Unknown status from converter
                 logging.warning(f"[{thread_name}] Received unknown status '{current_status}' for {quality_to_check} ({video_id}) from {base_url}")
                 # Treat as still processing for now, add back to queue
                 qualities_to_monitor.append(quality_to_check)

        except requests.exceptions.Timeout:
             logging.error(f"[{thread_name}] Timeout polling status for {quality_to_check} ({video_id}) from {base_url}")
             # Keep polling this quality for a while? Or count as failure?
             # Let's add back to queue for one retry, then fail it.
             # For simplicity now, let's treat timeout as needing retry.
             qualities_to_monitor.append(quality_to_check) # Retry later
        except requests.exceptions.HTTPError as e:
             # Handle specific HTTP errors, e.g., 404 might mean converter lost state
             logging.error(f"[{thread_name}] HTTP Error {e.response.status_code} polling status for {quality_to_check} ({video_id}) from {base_url}: {e.response.reason}")
             # If it's 404 maybe it failed permanently?
             if e.response.status_code == 404:
                  fail_msg = f"Polling {quality_to_check} failed with 404 (converter lost state or invalid ID?)"
                  logging.error(f"[{thread_name}] {fail_msg}")
                  failed_qualities.append(quality_to_check)
                  polling_errors.append(fail_msg)
                  update_video_status(video_id, error=fail_msg)
             else: # Other HTTP errors, maybe retry
                  qualities_to_monitor.append(quality_to_check)
        except requests.exceptions.RequestException as e:
            logging.error(f"[{thread_name}] Network error polling status for {quality_to_check} ({video_id}) from {base_url}: {e}")
            # Retry maybe?
            qualities_to_monitor.append(quality_to_check)
        except json.JSONDecodeError:
             logging.error(f"[{thread_name}] Invalid JSON response polling status for {quality_to_check} ({video_id}) from {base_url}")
             # Treat as failure? Or retry? Let's count as failure.
             fail_msg = f"Invalid JSON response from {quality_to_check} status endpoint."
             failed_qualities.append(quality_to_check)
             polling_errors.append(fail_msg)
             update_video_status(video_id, error=fail_msg)
        except Exception as e: # Catch unexpected errors
            logging.error(f"[{thread_name}] Unexpected error polling status for {quality_to_check} ({video_id}): {e}", exc_info=True)
            fail_msg = f"Unexpected error polling {quality_to_check}: {e}"
            failed_qualities.append(quality_to_check)
            polling_errors.append(fail_msg)
            update_video_status(video_id, error=fail_msg)


    # Update status with any errors encountered during polling
    if polling_errors:
         # Errors already added individually, maybe add a summary note
         update_video_status(video_id, error="Errors occurred during processing status check (see details above).")


    # Check results after polling finishes or times out
    if not completed_qualities:
         logging.error(f"[{thread_name}] No qualities completed successfully for {video_id}.")
         # Ensure status is error if not already set
         if get_video_status(video_id).get('status') != 'error':
             update_video_status(video_id, status="error", error="Processing failed: No qualities finished successfully.")
         # Clean up original upload file?
         # try: os.remove(source_filepath) except OSError: pass
         return

    logging.info(f"[{thread_name}] Completed qualities for {video_id}: {completed_qualities}")
    if failed_qualities: # Log only if there were failures
        logging.warning(f"[{thread_name}] Failed or timed out qualities for {video_id}: {failed_qualities}")

    # 3. Collect HLS files for completed qualities
    update_video_status(video_id, status="processing_collecting")
    collected_manifests = {} # quality -> relative_path
    collection_errors = [] # Collect errors during collection

    for quality in completed_qualities:
        base_url = active_converters[quality]
        files_list_url = f"{base_url}/files/{video_id}" # Endpoint to list files
        quality_hls_dir = os.path.join(video_hls_dir, quality)
        ensure_dir(quality_hls_dir) # Ensure target dir for this quality exists

        try:
            logging.info(f"[{thread_name}] Attempting to list files for {quality} from {files_list_url}")
            response = requests.get(files_list_url, timeout=20) # Increased timeout
            response.raise_for_status()
            response_data = response.json()
            files_to_download = response_data.get("files", [])
            logging.info(f"[{thread_name}] Files reported by converter for {quality}: {files_to_download}")

            if not files_to_download:
                 err_msg = f"Converter for {quality} reported completion, but returned no files."
                 logging.warning(f"[{thread_name}] {err_msg}")
                 collection_errors.append(err_msg)
                 # update_video_status(video_id, error=err_msg) # Maybe too noisy? Add later if needed.
                 continue # Skip collecting this quality


            playlist_filename = None
            download_error_for_quality = False # Flag per quality
            for filename in files_to_download:
                # Basic check for potentially malicious filenames (e.g., path traversal)
                if ".." in filename or filename.startswith("/"):
                     logging.error(f"[{thread_name}] Invalid filename '{filename}' received from {quality} converter. Skipping.")
                     download_error_for_quality = True
                     collection_errors.append(f"Invalid filename '{filename}' from {quality}.")
                     break # Stop collecting for this quality

                if filename.endswith(".m3u8"):
                    playlist_filename = filename

                download_url = f"{base_url}/files/{video_id}/{filename}"
                local_filepath = os.path.join(quality_hls_dir, filename)

                try:
                    logging.debug(f"[{thread_name}] Downloading {filename} for {quality} from {download_url}...")
                    with requests.get(download_url, stream=True, timeout=120) as r: # Long timeout for file download
                        r.raise_for_status()
                        with open(local_filepath, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=8192*4): # Use large chunk
                                f.write(chunk)
                    logging.debug(f"[{thread_name}] Downloaded {filename} for {quality} to {local_filepath}")

                except requests.exceptions.RequestException as e_dl:
                    err_msg = f"Failed to download {filename} for {quality} from {base_url}: {e_dl}"
                    logging.error(f"[{thread_name}] {err_msg}")
                    collection_errors.append(err_msg)
                    download_error_for_quality = True
                    break # Stop collecting files for this quality if one fails
                except IOError as e_io:
                    err_msg = f"Failed to write downloaded file {local_filepath} for {quality}: {e_io}"
                    logging.error(f"[{thread_name}] {err_msg}")
                    collection_errors.append(err_msg)
                    download_error_for_quality = True
                    break # Stop collecting

            # After attempting downloads for a quality
            if download_error_for_quality:
                 logging.error(f"[{thread_name}] Cleaning up partially collected directory due to download error: {quality_hls_dir}")
                 # Clean up partially downloaded files for this quality
                 try:
                     shutil.rmtree(quality_hls_dir)
                     logging.info(f"Removed partially collected directory: {quality_hls_dir}")
                 except OSError as e_rm:
                      logging.warning(f"Could not remove partial dir {quality_hls_dir}: {e_rm}")
            elif playlist_filename:
                 relative_playlist_path = f"{quality}/{playlist_filename}" # Path relative to video_id dir
                 collected_manifests[quality] = relative_playlist_path
                 logging.info(f"[{thread_name}] Successfully collected files for {quality}. Manifest: {relative_playlist_path}")
            else:
                 # Completed download but no m3u8 file found?
                 err_msg = f"Files collected for {quality}, but no .m3u8 playlist file was found in the list: {files_to_download}"
                 logging.warning(f"[{thread_name}] {err_msg}")
                 collection_errors.append(err_msg)
                 # Clean up the collected files as they are incomplete
                 try:
                      shutil.rmtree(quality_hls_dir)
                      logging.warning(f"Removed incomplete collected directory (no m3u8): {quality_hls_dir}")
                 except OSError as e_rm:
                      logging.warning(f"Could not remove incomplete dir {quality_hls_dir}: {e_rm}")


        except requests.exceptions.RequestException as e_list:
            err_msg = f"Failed to list files for {quality} from {base_url}: {e_list}"
            logging.error(f"[{thread_name}] {err_msg}")
            collection_errors.append(err_msg)
        except json.JSONDecodeError:
            err_msg = f"Invalid JSON response when listing files for {quality} from {base_url}"
            logging.error(f"[{thread_name}] {err_msg}")
            collection_errors.append(err_msg)
        except Exception as e: # Catch unexpected errors during listing/looping
             err_msg = f"Unexpected error collecting files for {quality}: {e}"
             logging.error(f"[{thread_name}] {err_msg}", exc_info=True)
             collection_errors.append(err_msg)


    # Update status with any collection errors
    if collection_errors:
        update_video_status(video_id, error="Errors occurred during HLS file collection:\n" + "\n".join(collection_errors))


    # Check if we collected anything useful after trying all completed qualities
    if not collected_manifests:
         logging.error(f"[{thread_name}] Failed to collect any valid HLS playlists for {video_id}.")
         # Ensure status is error if not already set
         if get_video_status(video_id).get('status') != 'error':
            update_video_status(video_id, status="error", error="Failed to collect any usable HLS playlists.")
         # Clean up original upload file? Optional.
         # try: os.remove(source_filepath) except OSError: pass
         return

    # 4. Create Master Manifest
    logging.info(f"[{thread_name}] Creating master playlist for collected qualities: {list(collected_manifests.keys())}")
    update_video_status(video_id, status="processing_manifest")
    master_playlist_content = ["#EXTM3U", "#EXT-X-VERSION:3"]
    # Sort qualities for consistent master playlist order (e.g., by height descending)
    sorted_qualities = sorted(collected_manifests.keys(), key=lambda q: RESOLUTIONS[q][0], reverse=True)

    for quality in sorted_qualities:
        resolution_info = RESOLUTIONS[quality]
        height = resolution_info[0]
        bandwidth = int(resolution_info[1].replace('k','000')) # Estimate bandwidth from video bitrate
        playlist_path = collected_manifests[quality] # Already relative path

        # Add resolution info for clarity
        master_playlist_content.append(f'#EXT-X-STREAM-INF:BANDWIDTH={bandwidth},RESOLUTION={height}x{height},NAME="{quality}"')
        master_playlist_content.append(playlist_path)

    master_playlist_filename = "master.m3u8"
    master_playlist_path = os.path.join(video_hls_dir, master_playlist_filename)
    try:
        with open(master_playlist_path, "w") as f:
            f.write("\n".join(master_playlist_content) + "\n")
        logging.info(f"[{thread_name}] Created master playlist for {video_id} at {master_playlist_path}")

        # 5. Final Status Update & Cleanup
        relative_master_path = f"{video_id}/{master_playlist_filename}"
        update_video_status(video_id, status="ready", manifest_path=relative_master_path)
        logging.info(f"[{thread_name}] Video processing completed successfully for {video_id}!")

        # Clean up the original uploaded file
        try:
            logging.info(f"[{thread_name}] Attempting to remove original source file: {source_filepath}")
            os.remove(source_filepath)
            logging.info(f"[{thread_name}] Removed original source file: {source_filepath}")
        except OSError as e:
            # This might happen if the download endpoint still has the file open, though unlikely
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

        if file:
            video_id = str(uuid.uuid4())
            save_path = None # Initialize save_path
            try:
                ensure_dir(UPLOAD_DIR)
                _, ext = os.path.splitext(file.filename)
                # Basic sanitization for extension
                safe_ext = "".join(c for c in ext if c.isalnum())
                if not safe_ext: safe_ext = '.mp4' # Default extension if needed
                else: safe_ext = '.' + safe_ext

                save_filename = f"{video_id}{safe_ext}"
                save_path = os.path.join(UPLOAD_DIR, save_filename)

                logging.info(f"Starting file save for {video_id} to {save_path}")
                start_save_time = time.time()
                file.save(save_path)
                # Ensure file write finished and is accessible
                if not os.path.exists(save_path) or os.path.getsize(save_path) == 0:
                     # Give a tiny moment for filesystem sync, though save() should block
                     time.sleep(0.1)
                     if not os.path.exists(save_path) or os.path.getsize(save_path) == 0:
                          raise IOError(f"File save failed or resulted in empty file for {video_id}")

                end_save_time = time.time()
                logging.info(f"File save completed successfully for {video_id} at {save_path} in {end_save_time - start_save_time:.2f}s")

                # --- Actions after successful save ---
                update_video_status(video_id, status="uploaded")
                logging.info(f"Status set to 'uploaded' for {video_id}")

                # Generate the download URL for converters
                source_download_url = None
                try:
                    # Try generating external URL using Flask's mechanisms
                    # Requires proper setup (SERVER_NAME or proxy headers like X-Forwarded-Host/Proto)
                    source_download_url = url_for('download_source_video', video_id=video_id, _external=True)
                    logging.info(f"Generated source URL via url_for(_external=True): {source_download_url}")
                    # Validate if it looks like a real URL
                    if not source_download_url or not source_download_url.startswith(('http://', 'https://')):
                        logging.warning(f"Generated URL '{source_download_url}' doesn't look absolute. Will try fallback.")
                        source_download_url = None # Reset to trigger fallback
                except Exception as url_error:
                     logging.warning(f"url_for(_external=True) failed: {url_error}. Will try fallback.")
                     source_download_url = None # Ensure it's None for fallback

                # Fallback: Construct URL manually from request headers (less reliable but often works)
                if not source_download_url:
                    try:
                        scheme = request.headers.get('X-Forwarded-Proto', request.scheme)
                        host = request.headers.get('X-Forwarded-Host', request.host)
                        # Ensure host doesn't include unexpected characters (basic check)
                        if not all(c.isalnum() or c in '.:-' for c in host):
                             raise ValueError(f"Invalid characters in host header: {host}")

                        path = url_for('download_source_video', video_id=video_id)
                        source_download_url = f"{scheme}://{host}{path}"
                        logging.warning(f"Using fallback constructed source URL: {source_download_url}")
                    except Exception as fallback_url_error:
                        logging.error(f"CRITICAL: Failed to generate source URL using both methods: {fallback_url_error}", exc_info=True)
                        update_video_status(video_id, status="error", error="Failed to determine server's external URL for converters.")
                        # Clean up saved file as processing cannot proceed
                        if save_path and os.path.exists(save_path): os.remove(save_path)
                        return jsonify({"success": False, "error": "Server configuration error: Cannot determine external URL."}), 500


                # Start background processing thread *after* save and URL generation
                logging.info(f"Starting background processing thread for {video_id}")
                processing_thread = threading.Thread(
                    target=process_video_distributed,
                    args=(video_id, save_path, source_download_url), # Pass the generated URL
                    name=f"Processor-{video_id[:8]}"
                )
                processing_thread.daemon = True # Allows main process to exit even if threads are running
                processing_thread.start()

                return jsonify({"success": True, "video_id": video_id})

            except IOError as e_io: # Catch file save specific errors
                logging.error(f"File save I/O error for video_id '{video_id}': {e_io}", exc_info=True)
                # Clean up if needed
                if save_path and os.path.exists(save_path): os.remove(save_path)
                if video_id: # Only try to pop if video_id was generated
                    with status_lock: video_processing_status.pop(video_id, None)
                    save_status()
                return jsonify({"success": False, "error": f"File save failed: {e_io}"}), 500
            except Exception as e: # Catch other unexpected errors
                 logging.error(f"Error during file upload or processing start for video_id '{video_id}': {e}", exc_info=True)
                 # Clean up if needed
                 if save_path and os.path.exists(save_path): os.remove(save_path)
                 if video_id:
                     with status_lock: video_processing_status.pop(video_id, None)
                     save_status()
                 return jsonify({"success": False, "error": f"An internal error occurred: {str(e)}"}), 500
        else:
             # Should not happen if checks above are correct, but as a safeguard
             return jsonify({"success": False, "error": "Invalid file state."}), 400

    # GET request
    return render_template('upload.html')


@app.route('/download_source/<video_id>')
def download_source_video(video_id):
    """Allows converter servers to download the original uploaded video."""
    # Sanitize video_id input slightly (prevent path traversal)
    safe_video_id = "".join(c for c in video_id if c.isalnum() or c == '-')
    if safe_video_id != video_id:
        logging.error(f"Invalid characters detected in video_id for download: {video_id}")
        abort(400, description="Invalid video ID format.")

    status_info = get_video_status(safe_video_id)
    if status_info['status'] == 'not_found':
        logging.warning(f"Download request for unknown video_id: {safe_video_id}")
        abort(404, description="Video ID not found.")

    # Find the source file based on the sanitized ID prefix
    source_file_path = None
    expected_prefix = safe_video_id
    try:
        if not os.path.exists(UPLOAD_DIR):
             logging.error(f"Upload directory {UPLOAD_DIR} not found!")
             abort(500, description="Server configuration error (upload dir missing).")

        found_files = []
        for filename in os.listdir(UPLOAD_DIR):
            # Check prefix and ensure it's a file with an extension
            if filename.startswith(expected_prefix) and '.' in filename and os.path.isfile(os.path.join(UPLOAD_DIR, filename)):
                 found_files.append(filename)

        if len(found_files) == 1:
            source_file_path = os.path.join(UPLOAD_DIR, found_files[0])
            logging.info(f"Found unique source file for {safe_video_id}: {source_file_path}")
        elif len(found_files) > 1:
             logging.error(f"Multiple source files found for prefix {safe_video_id}: {found_files}. Aborting download.")
             abort(500, description="Source file ambiguity error.")
        # else: # len == 0, handled below

    except Exception as e:
         logging.error(f"Error searching for source file for {safe_video_id} in {UPLOAD_DIR}: {e}", exc_info=True)
         abort(500, description="Error accessing upload directory.")

    # Check if file was found and is valid before sending
    if source_file_path and os.path.exists(source_file_path): # exists() check might be redundant but safe
        try:
             if os.path.getsize(source_file_path) > 0:
                 logging.info(f"Serving source video {source_file_path} for download request (video_id: {safe_video_id})")
                 return send_from_directory(UPLOAD_DIR, os.path.basename(source_file_path), as_attachment=False)
             else:
                 logging.error(f"Source file found for {safe_video_id} but is empty: {source_file_path}")
                 abort(500, description="Source file is empty.")
        except OSError as e:
             logging.error(f"OS error checking/serving source file {source_file_path} for {safe_video_id}: {e}")
             abort(500, description="Error accessing source file.")
        except Exception as e:
             logging.error(f"Unexpected error serving source file {source_file_path} for {safe_video_id}: {e}", exc_info=True)
             abort(500)
    else:
        # File not found
        logging.warning(f"Source file for video_id {safe_video_id} not found in {UPLOAD_DIR}. Searched for prefix '{expected_prefix}'.")
        current_status = status_info.get('status')
        if current_status not in ['ready', 'error', 'not_found']: # Should exist if processing/uploaded
             logging.error(f"CRITICAL: Source file for {safe_video_id} missing while status is '{current_status}'!")
             abort(500, description=f"Source file inconsistency detected.")
        else:
             abort(404, description="Source video file not found or already processed/deleted.")


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

    # Assuming you have 'index.html' or 'watch.html' in a 'templates' folder
    return render_template('index.html', # or 'watch.html'
                           hls_ready=hls_ready,
                           processing=processing,
                           error=error,
                           video_id=video_id,
                           master_playlist_url=master_playlist_url
                           )


@app.route('/hls/<video_id>/<path:filename>')
def serve_hls(video_id, filename):
    """Serves the master playlist and video segments."""
    # Sanitize inputs
    safe_video_id = "".join(c for c in video_id if c.isalnum() or c == '-')
    safe_filename = filename.replace('../', '') # Basic path traversal prevention

    if safe_video_id != video_id or safe_filename != filename:
         logging.error(f"Potential path traversal detected in HLS request: {video_id}/{filename}")
         abort(400)

    video_hls_path = os.path.join(HLS_DIR, safe_video_id)
    if not os.path.isdir(video_hls_path): # Check if the directory exists
         logging.warning(f"Attempt to access non-existent HLS path: {video_hls_path}")
         abort(404)

    logging.debug(f"Serving HLS file: {safe_filename} from {video_hls_path}")
    try:
        # Important: Set correct MIME types
        if safe_filename.endswith('.m3u8'):
            mime_type = 'application/vnd.apple.mpegurl'
        elif safe_filename.endswith('.ts'):
            mime_type = 'video/mp2t'
        else:
            mime_type = None # Let Flask guess

        # Use send_from_directory for security
        response = send_from_directory(video_hls_path, safe_filename, mimetype=mime_type)
        # Add CORS headers - crucial if player is on a different domain than this server
        response.headers.add('Access-Control-Allow-Origin', '*')
        # Add cache control? HLS segments are usually immutable
        # response.headers.add('Cache-Control', 'public, max-age=31536000')
        return response

    except FileNotFoundError:
        logging.warning(f"HLS file not found: {safe_filename} in {video_hls_path}")
        abort(404)
    except Exception as e:
         logging.error(f"Error serving HLS file {safe_filename} for {safe_video_id}: {e}", exc_info=True)
         abort(500)


@app.route('/')
def index():
    # Redirect to upload page
    return redirect(url_for('upload_video'))


# === Initialization ===
if __name__ == '__main__':
    ensure_dir(UPLOAD_DIR)
    ensure_dir(HLS_DIR)
    load_status() # Load existing status on startup
    # Use host='0.0.0.0' to make it accessible on the network
    # Use port provided by environment (e.g., Railway, Render) or default to 5000
    port = int(os.environ.get('PORT', 5000))
    # Debug=False for production, True for development
    # threaded=True is important for background tasks
    app.run(debug=False, host='0.0.0.0', port=port, threaded=True)

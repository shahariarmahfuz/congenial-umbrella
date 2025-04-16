import os
import subprocess
import requests
import threading
from flask import Flask, render_template, send_from_directory, abort, Response

app = Flask(__name__)

# --- Configuration ---
VIDEO_URL = "https://www.dropbox.com/scl/fi/dx4v458ut0ko9fyt8avdk/7TMX-8LHS-2VG5-WU77_HD.mp4?rlkey=sr7pionn5z4cfbbu6l58un49d&raw=1"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, 'downloads')
HLS_DIR = os.path.join(BASE_DIR, 'static', 'hls') # Serve from static for simplicity
DOWNLOADED_FILENAME = "source_video.mp4"
MASTER_PLAYLIST_NAME = "master.m3u8"

# Define desired output resolutions and bitrates (adjust as needed)
# Format: (height, video_bitrate, audio_bitrate)
RESOLUTIONS = [
    (360, '800k', '96k'),
    (480, '1400k', '128k'),
    (720, '2800k', '128k') # Assuming source is at least 720p
]

# --- State Variables ---
# Use files to track state across potential worker restarts in Gunicorn
PROCESSING_LOCK_FILE = os.path.join(BASE_DIR, 'processing.lock')
HLS_READY_FILE = os.path.join(HLS_DIR, '.hls_ready') # Marker file
DOWNLOAD_COMPLETE_FILE = os.path.join(DOWNLOAD_DIR, '.download_complete')
PROCESSING_ERROR_FILE = os.path.join(BASE_DIR, 'processing.error')

# --- Helper Functions ---

def ensure_dir(directory):
    """Creates a directory if it doesn't exist."""
    if not os.path.exists(directory):
        os.makedirs(directory)
    print(f"Ensured directory exists: {directory}")

def check_ffmpeg():
    """Checks if ffmpeg is installed and accessible."""
    try:
        subprocess.run(['ffmpeg', '-version'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print("ffmpeg found.")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("ERROR: ffmpeg not found. Please install ffmpeg.")
        with open(PROCESSING_ERROR_FILE, 'w') as f:
            f.write("ffmpeg not found or accessible.")
        return False

def download_video(url, dest_path):
    """Downloads the video file."""
    if os.path.exists(DOWNLOAD_COMPLETE_FILE):
        print(f"Video already downloaded: {dest_path}")
        return True
    print(f"Starting download from {url} to {dest_path}...")
    try:
        with requests.get(url, stream=True, timeout=300) as r: # Added timeout
            r.raise_for_status()
            ensure_dir(os.path.dirname(dest_path))
            with open(dest_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print("Download complete.")
        # Create marker file on successful download
        with open(DOWNLOAD_COMPLETE_FILE, 'w') as f:
             f.write('done')
        return True
    except requests.exceptions.RequestException as e:
        error_msg = f"Error downloading video: {e}"
        print(f"ERROR: {error_msg}")
        with open(PROCESSING_ERROR_FILE, 'w') as f:
            f.write(error_msg)
        if os.path.exists(dest_path):
             os.remove(dest_path) # Clean up partial download
        return False
    except Exception as e:
        error_msg = f"An unexpected error occurred during download: {e}"
        print(f"ERROR: {error_msg}")
        with open(PROCESSING_ERROR_FILE, 'w') as f:
            f.write(error_msg)
        if os.path.exists(dest_path):
             os.remove(dest_path) # Clean up partial download
        return False

def transcode_to_hls(input_path, output_dir, resolutions):
    """Transcodes video to multiple resolutions in HLS format."""
    if os.path.exists(HLS_READY_FILE):
        print("HLS files already exist. Skipping transcoding.")
        return True

    if not os.path.exists(input_path):
        error_msg = f"Input video file not found: {input_path}"
        print(f"ERROR: {error_msg}")
        with open(PROCESSING_ERROR_FILE, 'w') as f:
            f.write(error_msg)
        return False

    print("Starting HLS transcoding...")
    ensure_dir(output_dir)
    master_playlist_content = "#EXTM3U\n#EXT-X-VERSION:3\n"
    ffmpeg_cmds = []

    for height, v_bitrate, a_bitrate in resolutions:
        res_output_dir = os.path.join(output_dir, str(height))
        ensure_dir(res_output_dir)
        playlist_path = os.path.join(res_output_dir, 'playlist.m3u8')
        segment_path = os.path.join(res_output_dir, 'segment%03d.ts')

        # Construct ffmpeg command for this resolution
        # -vf "scale=-2:{height}": Scale to height, maintain aspect ratio
        # -crf 23: Constant Rate Factor (quality, lower is better, 18-28 is common)
        # -preset veryfast: Encoding speed vs compression (faster means larger files)
        # -maxrate, -bufsize: Constrain bitrate for streaming
        # -hls_time 10: Segment duration in seconds
        # -hls_list_size 0: Keep all segments in the playlist
        # -hls_segment_filename: Pattern for segment files
        # -c:a aac: Audio codec
        # -ar 48000: Audio sample rate
        # -b:a: Audio bitrate
        cmd = [
            'ffmpeg', '-i', input_path,
            '-vf', f'scale=-2:{height}',
            '-c:v', 'libx264', '-crf', '23', '-preset', 'veryfast', # Changed to veryfast for speed
            '-b:v', v_bitrate, '-maxrate', v_bitrate, '-bufsize', f'{int(v_bitrate[:-1])*2}k',
            '-c:a', 'aac', '-ar', '48000', '-b:a', a_bitrate,
            '-f', 'hls',
            '-hls_time', '6', # Shorter segments might start faster
            '-hls_list_size', '0',
            '-hls_segment_filename', segment_path,
            playlist_path
        ]
        ffmpeg_cmds.append(cmd)

        # Add entry to master playlist
        # Get resolution width (useful for BANDWIDTH calculation, but ffmpeg handles it)
        # For simplicity, we use bitrate directly for bandwidth info
        # Bandwidth is in bits per second
        bandwidth = int(v_bitrate[:-1]) * 1000 + int(a_bitrate[:-1]) * 1000
        master_playlist_content += f'#EXT-X-STREAM-INF:BANDWIDTH={bandwidth},RESOLUTION=x{height}\n'
        master_playlist_content += f'{height}/playlist.m3u8\n'

    # Run ffmpeg commands sequentially
    try:
        for i, cmd in enumerate(ffmpeg_cmds):
            print(f"Running ffmpeg for {resolutions[i][0]}p...")
            print(f"Command: {' '.join(cmd)}") # Log the command
            result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            print(f"ffmpeg output for {resolutions[i][0]}p: {result.stdout.decode()}")
            print(f"ffmpeg stderr for {resolutions[i][0]}p: {result.stderr.decode()}") # Log stderr too
        print("Transcoding successful for all resolutions.")
    except subprocess.CalledProcessError as e:
        error_msg = f"Error during ffmpeg transcoding:\nSTDOUT: {e.stdout.decode()}\nSTDERR: {e.stderr.decode()}"
        print(f"ERROR: {error_msg}")
        with open(PROCESSING_ERROR_FILE, 'w') as f:
            f.write(error_msg)
        return False
    except Exception as e:
        error_msg = f"An unexpected error occurred during transcoding: {e}"
        print(f"ERROR: {error_msg}")
        with open(PROCESSING_ERROR_FILE, 'w') as f:
            f.write(error_msg)
        return False

    # Write the master playlist file
    master_playlist_path = os.path.join(output_dir, MASTER_PLAYLIST_NAME)
    try:
        with open(master_playlist_path, 'w') as f:
            f.write(master_playlist_content)
        print(f"Master playlist created: {master_playlist_path}")
         # Create marker file on successful completion
        with open(HLS_READY_FILE, 'w') as f:
             f.write('ready')
        return True
    except IOError as e:
        error_msg = f"Error writing master playlist file: {e}"
        print(f"ERROR: {error_msg}")
        with open(PROCESSING_ERROR_FILE, 'w') as f:
             f.write(error_msg)
        return False

def run_processing():
    """Runs the download and transcoding process."""
    if os.path.exists(PROCESSING_LOCK_FILE):
        print("Processing is already running or finished.")
        return

    # Create lock file
    try:
        with open(PROCESSING_LOCK_FILE, 'w') as f:
            f.write('locked')
        print("Created processing lock file.")

        # Clear previous error state
        if os.path.exists(PROCESSING_ERROR_FILE):
            os.remove(PROCESSING_ERROR_FILE)

        # Check ffmpeg first
        if not check_ffmpeg():
            print("ffmpeg check failed. Aborting processing.")
            # Lock file will remain, error file is written by check_ffmpeg
            return # Don't remove lock file here

        download_path = os.path.join(DOWNLOAD_DIR, DOWNLOADED_FILENAME)

        # 1. Download
        if not download_video(VIDEO_URL, download_path):
            print("Download failed. Aborting processing.")
            # Lock file will remain, error file written by download_video
            return # Don't remove lock file here

        # 2. Transcode
        if not transcode_to_hls(download_path, HLS_DIR, RESOLUTIONS):
            print("Transcoding failed.")
            # Lock file will remain, error file written by transcode_to_hls
            return # Don't remove lock file here

        print("Processing completed successfully.")
        # HLS_READY_FILE is created by transcode_to_hls on success
        # Keep the lock file to indicate completion without error

    except Exception as e:
        error_msg = f"Unexpected error in run_processing: {e}"
        print(f"ERROR: {error_msg}")
        with open(PROCESSING_ERROR_FILE, 'w') as f:
            f.write(error_msg)
        # Keep the lock file but ensure error is recorded

    finally:
        # Optional: Clean up downloaded file after successful transcoding
        # if os.path.exists(HLS_READY_FILE) and os.path.exists(download_path):
        #     try:
        #         os.remove(download_path)
        #         print(f"Cleaned up downloaded file: {download_path}")
        #         if os.path.exists(DOWNLOAD_COMPLETE_FILE):
        #              os.remove(DOWNLOAD_COMPLETE_FILE)
        #     except OSError as e:
        #         print(f"Warning: Could not remove downloaded file: {e}")
        pass # Keep lock file even on error to prevent retries until manual reset

# --- Flask Routes ---

@app.route('/')
def index():
    """Serves the main HTML page."""
    error = None
    processing = False
    hls_ready = os.path.exists(HLS_READY_FILE)

    if os.path.exists(PROCESSING_ERROR_FILE):
        try:
            with open(PROCESSING_ERROR_FILE, 'r') as f:
                error = f.read()
        except Exception as e:
            error = f"Could not read error file: {e}"
    elif not hls_ready and os.path.exists(PROCESSING_LOCK_FILE):
         # If lock exists but ready file doesn't, it's processing (or failed without error file)
         processing = True
    elif not hls_ready and not os.path.exists(PROCESSING_LOCK_FILE):
        # Should not happen if start_processing was called, maybe initial state
        error = "Processing not started or state unclear. Check server logs."


    print(f"Index route: hls_ready={hls_ready}, processing={processing}, error={bool(error)}")
    return render_template('index.html', hls_ready=hls_ready, processing=processing, error=error)

@app.route('/hls/<path:filename>')
def serve_hls_files(filename):
    """Serves HLS playlist and segment files."""
    hls_base_dir = os.path.join(BASE_DIR, 'static') # Relative to app.py location
    print(f"Serving HLS file: {filename} from {hls_base_dir}")
    # Security: Basic check to prevent path traversal
    if '..' in filename or filename.startswith('/'):
         abort(404)
    try:
        # send_from_directory expects directory relative to app root or absolute
        # Our HLS_DIR is within static, so we serve from 'static' folder
        return send_from_directory(hls_base_dir, os.path.join('hls', filename), conditional=True) # Add conditional=True for caching
    except FileNotFoundError:
        print(f"File not found: {os.path.join(hls_base_dir, 'hls', filename)}")
        abort(404)
    except Exception as e:
        print(f"Error serving file {filename}: {e}")
        abort(500)

# --- Application Startup ---
def start_processing_thread():
    """Starts the video processing in a background thread."""
    if not os.path.exists(PROCESSING_LOCK_FILE):
         print("Starting processing thread...")
         processing_thread = threading.Thread(target=run_processing)
         processing_thread.daemon = True # Allow app to exit even if thread is running
         processing_thread.start()
    else:
         print("Lock file exists. Assuming processing already done or in progress.")

# Run processing logic once when the app starts (or in a separate thread)
# Gunicorn typically pre-forks workers, so this might run multiple times.
# Using file locks helps manage this.
start_processing_thread()

if __name__ == '__main__':
    # This block is mainly for local development
    # Ensure directories exist before Flask tries to serve from them
    ensure_dir(DOWNLOAD_DIR)
    ensure_dir(HLS_DIR)
    app.run(debug=True, host='0.0.0.0', port=5000) # Use a different port for local dev
                  

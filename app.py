import os
import subprocess
# import requests # Requests আর ব্যবহৃত হচ্ছে না, তবে রাখছি যদি ভবিষ্যতে লাগে
import threading
import logging
import time
import shutil
import uuid
import math # <<<--- প্রস্থ গণনার জন্য math.floor ব্যবহার করা যেতে পারে
from flask import Flask, render_template, send_from_directory, abort, Response, request, redirect, url_for, flash

# === Logging Configuration ===
# লগিং কনফিগারেশন: অ্যাপ্লিকেশন এবং প্রসেসিংয়ের ধাপগুলো লগ করার জন্য
logging.basicConfig(
    level=logging.INFO, # লগ লেভেল INFO সেট করা হয়েছে
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s', # লগের ফরম্যাট
    datefmt='%Y-%m-%d %H:%M:%S' # তারিখ ও সময়ের ফরম্যাট
)

# === Flask App Initialization ===
# Flask অ্যাপ্লিকেশন ইনিশিয়ালাইজেশন
app = Flask(__name__)
# Flash মেসেজ (যেমন 'Upload successful!') দেখানোর জন্য একটি সিক্রেট কী দরকার
app.secret_key = os.urandom(24) # প্রতিটি সেশনে ভিন্ন কী অথবা একটি নির্দিষ্ট স্ট্রিং ব্যবহার করুন

# === Configuration Constants ===
# কনফিগারেশন ধ্রুবক: অ্যাপ্লিকেশনটির বিভিন্ন সেটিংস ও পাথ
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) # অ্যাপ্লিকেশনের মূল ডিরেক্টরি
UPLOAD_DIR = os.path.join(BASE_DIR, 'uploads') # আপলোড করা ভিডিও ফাইল রাখার ডিরেক্টরি
STATIC_DIR = os.path.join(BASE_DIR, 'static') # স্ট্যাটিক ফাইল (CSS, JS) রাখার ডিরেক্টরি
HLS_DIR = os.path.join(STATIC_DIR, 'hls') # জেনারেট করা HLS ফাইল (m3u8, ts) রাখার মূল ডিরেক্টরি
ALLOWED_EXTENSIONS = {'mp4', 'mov', 'avi', 'mkv', 'webm'} # অনুমোদিত ভিডিও ফাইলের এক্সটেনশন
SOURCE_VIDEO_BASENAME = "source" # প্রতিটি ভিডিওর আপলোড করা মূল ফাইলের বেস নাম
MASTER_PLAYLIST_NAME = "master.m3u8" # মাস্টার HLS প্লেলিস্টের ফাইলের নাম

# State filenames (relative to each video's HLS directory)
# প্রতিটি ভিডিওর অবস্থা নির্দেশক ফাইলের নাম
PROCESSING_LOCK_FILENAME = ".processing.lock" # প্রসেসিং চললে এই ফাইল থাকবে
HLS_READY_FILENAME = ".hls_ready" # প্রসেসিং সফল হলে এই ফাইল থাকবে
PROCESSING_ERROR_FILENAME = ".processing.error" # প্রসেসিং ব্যর্থ হলে এই ফাইল থাকবে

# Define desired output resolutions and bitrates (height, video_bitrate, audio_bitrate)
# কাঙ্ক্ষিত আউটপুট রেজোলিউশন ও বিটরেট
RESOLUTIONS = [
    (360, '800k', '96k'),    # 360p, 800kbps ভিডিও, 96kbps অডিও
    (480, '1400k', '128k'),   # 480p, 1400kbps ভিডিও, 128kbps অডিও
    (720, '2800k', '128k')    # 720p, 2800kbps ভিডিও, 128kbps অডিও
]
FFMPEG_TIMEOUT = 1800 # প্রতিটি ffmpeg কমান্ডের জন্য সর্বোচ্চ সময় (সেকেন্ডে), 30 মিনিট

# === Helper Functions ===
# সহায়ক ফাংশনসমূহ

def ensure_dir(directory):
    """প্রয়োজন অনুযায়ী ডিরেক্টরি তৈরি করে।"""
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
            logging.info(f"ডিরেক্টরি তৈরি করা হয়েছে: {directory}")
        except OSError as e:
            logging.error(f"ডিরেক্টরি তৈরি করতে ব্যর্থ: {directory}: {e}")
            raise # ত্রুটি পুনরায় রেইজ করুন

def check_command(command_name):
    """নির্দিষ্ট কমান্ড (ffmpeg বা ffprobe) সিস্টেমে ইনস্টল এবং অ্যাক্সেসযোগ্য কিনা তা পরীক্ষা করে।"""
    try:
        # কমান্ডের ভার্সন চেক করার চেষ্টা করে
        result = subprocess.run([command_name, '-version'], check=True, capture_output=True, text=True, timeout=10)
        logging.info(f"{command_name} পরীক্ষা সফল।")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
        # যদি কমান্ড না পাওয়া যায় বা অন্য কোনো ত্রুটি ঘটে
        logging.error(f"{command_name} পরীক্ষা ব্যর্থ: {e}")
        return False

def get_video_dimensions(video_path):
    """ffprobe ব্যবহার করে ভিডিওর প্রস্থ (width) এবং উচ্চতা (height) বের করে।"""
    # প্রথমে ffprobe আছে কিনা তা নিশ্চিত করুন
    if not check_command('ffprobe'):
        logging.error("ভিডিওর ডাইমেনশন পেতে ffprobe প্রয়োজন কিন্তু এটি উপলব্ধ নেই।")
        return None, None

    # ffprobe কমান্ড তৈরি করুন
    command = [
        'ffprobe',
        '-v', 'error',               # শুধুমাত্র ত্রুটি দেখান
        '-select_streams', 'v:0',    # প্রথম ভিডিও স্ট্রিম নির্বাচন করুন
        '-show_entries', 'stream=width,height', # প্রস্থ ও উচ্চতা দেখান
        '-of', 'csv=s=x:p=0',         # আউটপুট ফরম্যাট: widthxheight
        video_path                   # ইনপুট ভিডিও ফাইলের পাথ
    ]
    try:
        # কমান্ডটি চালান
        result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=30)
        output = result.stdout.strip() # আউটপুট থেকে অতিরিক্ত স্পেস বাদ দিন
        # আউটপুট পার্স করুন
        if 'x' in output:
            width, height = map(int, output.split('x')) # প্রস্থ ও উচ্চতা বের করুন
            logging.info(f"ভিডিওর ডাইমেনশন সনাক্ত করা হয়েছে ({os.path.basename(video_path)}): {width}x{height}")
            return width, height
        else:
            # যদি আউটপুট প্রত্যাশিত ফরম্যাটে না থাকে
            logging.error(f"ffprobe আউটপুট থেকে ডাইমেনশন পার্স করা যায়নি: '{output}'")
            return None, None
    except subprocess.CalledProcessError as e:
        # যদি ffprobe কমান্ড ব্যর্থ হয়
        logging.error(f"ffprobe ব্যর্থ হয়েছে ({video_path}, return code {e.returncode}): {e.stderr}")
        return None, None
    except FileNotFoundError:
        # যদি ffprobe কমান্ড খুঁজে না পাওয়া যায়
        logging.error("ডাইমেনশন পাওয়ার সময় ffprobe কমান্ড খুঁজে পাওয়া যায়নি।")
        return None, None
    except Exception as e:
        # অন্যান্য অপ্রত্যাশিত ত্রুটি
        logging.error(f"ভিডিও ডাইমেনশন পেতে ত্রুটি ({video_path}): {e}", exc_info=True)
        return None, None

def allowed_file(filename):
    """আপলোড করা ফাইলের এক্সটেনশন অনুমোদিত কিনা তা পরীক্ষা করে।"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def clear_hls_directory_contents(directory_path):
    """ট্রান্সকোডিং শুরু করার আগে নির্দিষ্ট HLS ডিরেক্টরির ভিতরের সমস্ত ফাইল ও ফোল্ডার মুছে ফেলে।"""
    if not os.path.isdir(directory_path):
        logging.warning(f"HLS ডিরেক্টরি মুছে ফেলার জন্য খুঁজে পাওয়া যায়নি: {directory_path}")
        return
    logging.info(f"HLS ডিরেক্টরির কনটেন্ট মুছে ফেলা হচ্ছে: {directory_path}")
    try:
        # ডিরেক্টরির ভিতরের প্রতিটি আইটেমের জন্য
        for item in os.listdir(directory_path):
            item_path = os.path.join(directory_path, item)
            try:
                # যদি আইটেমটি একটি ডিরেক্টরি হয়, তবে সেটি এবং তার ভিতরের সবকিছু মুছে ফেলুন
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                # যদি আইটেমটি একটি ফাইল হয়, তবে সেটি মুছে ফেলুন
                else:
                    os.remove(item_path)
            except Exception as e:
                logging.error(f"আইটেমটি মুছে ফেলা যায়নি {item_path}: {e}")
    except Exception as e:
        # যদি ডিরেক্টরির তালিকা পেতে বা মুছতে সমস্যা হয়
        logging.error(f"ডিরেক্টরি তালিকাভুক্ত বা পরিষ্কার করা যায়নি {directory_path}: {e}")

# === Core Processing Functions ===
# মূল প্রসেসিং ফাংশনসমূহ

def transcode_to_hls(video_id, input_path, output_base_dir, resolutions):
    """ভিডিওকে HLS ফরম্যাটে ট্রান্সকোড করে এবং মাস্টার প্লেলিস্টে সঠিক রেজোলিউশন ব্যবহার করে।"""
    error_file_path = os.path.join(output_base_dir, PROCESSING_ERROR_FILENAME) # ত্রুটি ফাইলের পাথ

    # --- মূল ভিডিওর ডাইমেনশন পান ---
    original_width, original_height = get_video_dimensions(input_path)
    if not original_width or not original_height:
        # যদি ডাইমেনশন না পাওয়া যায়, ত্রুটি লগ করুন এবং ব্যর্থ হোন
        error_msg = f"[{video_id}] ভিডিও ডাইমেনশন পাওয়া যায়নি ({input_path})। ট্রান্সকোডিং সম্ভব নয়।"
        logging.error(error_msg)
        try:
            with open(error_file_path, 'w') as f: f.write(error_msg)
        except IOError as e:
             logging.error(f"[{video_id}] ডাইমেনশন ত্রুটি ফাইল লিখতে ব্যর্থ: {error_file_path}: {e}")
        return False
    # --- ডাইমেনশন পাওয়া শেষ ---

    # ইনপুট ফাইল আছে এবং খালি নয় তা নিশ্চিত করুন
    if not os.path.exists(input_path) or os.path.getsize(input_path) == 0:
        error_msg = f"[{video_id}] ইনপুট ভিডিও ফাইল খুঁজে পাওয়া যায়নি বা খালি: {input_path}"
        logging.error(error_msg)
        try:
            with open(error_file_path, 'w') as f: f.write(error_msg)
        except IOError as e:
             logging.error(f"[{video_id}] ইনপুট ফাইল ত্রুটি ফাইল লিখতে ব্যর্থ: {error_file_path}: {e}")
        return False

    logging.info(f"[{video_id}] HLS ট্রান্সকোডিং শুরু হচ্ছে (সোর্স রেজোলিউশন: {original_width}x{original_height}) ফাইল: {input_path} থেকে ডিরেক্টরি: {output_base_dir}...")
    ensure_dir(output_base_dir) # ভিডিওর নির্দিষ্ট HLS ডিরেক্টরি তৈরি করুন

    master_playlist_content = "#EXTM3U\n#EXT-X-VERSION:3\n" # মাস্টার প্লেলিস্টের শুরু
    ffmpeg_commands = [] # ffmpeg কমান্ডগুলো রাখার তালিকা
    resolution_details_for_master = [] # মাস্টার প্লেলিস্টের জন্য রেজোলিউশনের তথ্য (শুধুমাত্র সফলগুলো থাকবে)

    # --- প্রতিটি কাঙ্ক্ষিত রেজোলিউশনের জন্য কমান্ড প্রস্তুত করুন এবং প্রস্থ গণনা করুন ---
    for target_height, v_bitrate, a_bitrate in resolutions:
        # >>> গুরুত্বপূর্ণ চেক: যদি টার্গেট রেজোলিউশন মূল ভিডিওর চেয়ে বেশি হয়, তবে সেটি বাদ দিন <<<
        if target_height > original_height + 10: # +10 একটি ছোট মার্জিন সহনশীলতার জন্য
             logging.warning(f"[{video_id}] {target_height}p বাদ দেওয়া হচ্ছে কারণ এটি মূল উচ্চতা ({original_height}p) থেকে বেশি।")
             continue # পরবর্তী রেজোলিউশনে যান

        # মূল অ্যাসপেক্ট রেশিও ব্যবহার করে টার্গেট প্রস্থ গণনা করুন
        aspect_ratio = original_width / original_height
        # প্রস্থ গণনা করুন এবং নিশ্চিত করুন এটি একটি জোড় সংখ্যা (ভিডিও কোডেকের জন্য গুরুত্বপূর্ণ)
        target_width = math.floor(target_height * aspect_ratio / 2.0) * 2
        if target_width == 0: target_width = 2 # প্রস্থ শূন্য হওয়া এড়ান

        logging.info(f"[{video_id}] গণনা করা টার্গেট রেজোলিউশন: {target_width}x{target_height}")

        # ফাইল ও ডিরেক্টরির পাথ নির্ধারণ করুন
        res_output_dir = os.path.join(output_base_dir, str(target_height)) # যেমন: static/hls/uuid/360
        ensure_dir(res_output_dir)
        relative_playlist_path = os.path.join(str(target_height), 'playlist.m3u8') # মাস্টার প্লেলিস্টের সাপেক্ষে পাথ
        segment_path_pattern = os.path.join(res_output_dir, 'segment%03d.ts') # সেগমেন্ট ফাইলের প্যাটার্ন
        absolute_playlist_path = os.path.join(res_output_dir, 'playlist.m3u8') # ffmpeg এর জন্য প্লেলিস্টের পাথ

        # ffmpeg স্কেল ফিল্টার (-2 ব্যবহার করলে ffmpeg স্বয়ংক্রিয়ভাবে প্রস্থ গণনা করে)
        scale_filter = f'scale=-2:{target_height}'

        # ffmpeg কমান্ড তৈরি করুন
        cmd = [
            'ffmpeg', '-i', input_path,           # ইনপুট ফাইল
            '-vf', scale_filter,                 # ভিডিও ফিল্টার (স্কেলিং)
            '-c:v', 'libx264', '-crf', '23', '-preset', 'veryfast', # ভিডিও কোডেক ও সেটিংস
            '-b:v', v_bitrate, '-maxrate', v_bitrate, '-bufsize', f'{int(v_bitrate[:-1])*2}k', # ভিডিও বিটরেট কন্ট্রোল
            '-c:a', 'aac', '-ar', '48000', '-b:a', a_bitrate, # অডিও কোডেক ও সেটিংস
            '-f', 'hls',                         # আউটপুট ফরম্যাট HLS
            '-hls_time', '6',                    # প্রতিটি সেগমেন্টের দৈর্ঘ্য (সেকেন্ড)
            '-hls_list_size', '0',               # প্লেলিস্টে সব সেগমেন্ট রাখুন
            '-hls_segment_filename', segment_path_pattern, # সেগমেন্ট ফাইলের নাম প্যাটার্ন
            '-hls_flags', 'delete_segments',     # আগের সেগমেন্ট মুছে নতুন করে শুরু করুন
            absolute_playlist_path               # আউটপুট প্লেলিস্ট ফাইলের পাথ
        ]
        # এই রেজোলিউশনের জন্য প্রয়োজনীয় তথ্য সংরক্ষণ করুন (কমান্ড এবং প্লেলিস্টের বিবরণ)
        current_resolution_details = {
            'bandwidth': int(v_bitrate[:-1]) * 1000 + int(a_bitrate[:-1]) * 1000,
            'width': target_width,
            'height': target_height,
            'playlist_path': relative_playlist_path
        }
        # --- ffmpeg কমান্ডগুলো চালান ---
        # এই লুপটি প্রতিটি রেজোলিউশনের জন্য ffmpeg চালানোর চেষ্টা করবে
        logging.info(f"[{video_id}] {target_height}p এর জন্য ffmpeg চালানো হচ্ছে...")
        logging.debug(f"[{video_id}] কমান্ড: {' '.join(cmd)}")
        start_time_res = time.time() # এই রেজোলিউশনের সময় গণনা শুরু
        try:
            # কমান্ড চালান ও আউটপুট ক্যাপচার করুন
            result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=FFMPEG_TIMEOUT)
            end_time_res = time.time() # এই রেজোলিউশনের সময় গণনা শেষ
            logging.info(f"[{video_id}] {target_height}p এর জন্য ffmpeg সফলভাবে শেষ হয়েছে ({end_time_res - start_time_res:.2f} সেকেন্ড)।")
            # সফল হলে, এই রেজোলিউশনের বিবরণ মাস্টার প্লেলিস্টের জন্য যোগ করুন
            resolution_details_for_master.append(current_resolution_details)
            # সফল হলেও stderr লগ করুন (ওয়ার্নিং থাকতে পারে)
            if result.stderr:
                 logging.debug(f"[{video_id}] ffmpeg stderr ({target_height}p):\n{result.stderr[-1000:]}") # শেষ ১০০০ অক্ষর

        # >>> গুরুত্বপূর্ণ ত্রুটি হ্যান্ডলিং: যদি ffmpeg ব্যর্থ হয় <<<
        except subprocess.CalledProcessError as e:
            error_msg = (f"[{video_id}] {target_height}p এর জন্য ট্রান্সকোডিং ব্যর্থ (ffmpeg exit code {e.returncode})।\n"
                         f"ইনপুট: {input_path}\nCommand: {' '.join(e.cmd)}\n"
                         f"STDERR (last 1000 chars):\n...{e.stderr[-1000:]}")
            logging.error(error_msg)
            try:
                with open(error_file_path, 'w') as f: f.write(error_msg) # ত্রুটি ফাইল লিখুন
            except IOError as io_err:
                logging.error(f"[{video_id}] ffmpeg ত্রুটি ফাইল লিখতে ব্যর্থ: {error_file_path}: {io_err}")
            # >>> এখানে লুপ ব্রেক করা হচ্ছে, তাই পরবর্তী রেজোলিউশনগুলো চেষ্টা করা হবে না <<<
            break
        except subprocess.TimeoutExpired as e:
            error_msg = (f"[{video_id}] {target_height}p এর জন্য ট্রান্সকোডিং টাইমআউট ({FFMPEG_TIMEOUT} সেকেন্ড)।\n"
                         f"ইনপুট: {input_path}\nCommand: {' '.join(e.cmd)}")
            logging.error(error_msg)
            try:
                with open(error_file_path, 'w') as f: f.write(error_msg) # ত্রুটি ফাইল লিখুন
            except IOError as io_err:
                 logging.error(f"[{video_id}] টাইমআউট ত্রুটি ফাইল লিখতে ব্যর্থ: {error_file_path}: {io_err}")
            # >>> এখানে লুপ ব্রেক করা হচ্ছে, তাই পরবর্তী রেজোলিউশনগুলো চেষ্টা করা হবে না <<<
            break
        except Exception as e:
            error_msg = f"[{video_id}] {target_height}p এর জন্য ট্রান্সকোডিংয়ের সময় অপ্রত্যাশিত ত্রুটি: {e}\nInput: {input_path}"
            logging.error(error_msg, exc_info=True)
            try:
                 with open(error_file_path, 'w') as f: f.write(error_msg) # ত্রুটি ফাইল লিখুন
            except IOError as io_err:
                logging.error(f"[{video_id}] অপ্রত্যাশিত ত্রুটি ফাইল লিখতে ব্যর্থ: {error_file_path}: {io_err}")
            # >>> এখানে লুপ ব্রেক করা হচ্ছে, তাই পরবর্তী রেজোলিউশনগুলো চেষ্টা করা হবে না <<<
            break
        # --- একটি রেজোলিউশনের জন্য ffmpeg চালানো শেষ ---

    # --- রেজোলিউশন লুপ শেষ ---

    # যদি কোনো রেজোলিউশন সফলভাবে তৈরি না হয় (লিস্ট খালি থাকে)
    if not resolution_details_for_master:
         # এই বার্তাটি তখনই আসবে যদি প্রথম রেজোলিউশনটিই ব্যর্থ হয় বা কোনোটিই উপযুক্ত না হয়
         logging.error(f"[{video_id}] কোনো রেজোলিউশন সফলভাবে তৈরি হয়নি। মাস্টার প্লেলিস্ট তৈরি করা সম্ভব নয়।")
         # ত্রুটি ফাইল আগে তৈরি হয়ে থাকার কথা, তাই এখানে আবার লেখার দরকার নেই যদি না কোনো নতুন ত্রুটি ঘটে
         return False # ব্যর্থ রিটার্ন করুন

    # --- মাস্টার প্লেলিস্ট তৈরি করুন (শুধুমাত্র সফল রেজোলিউশনগুলো দিয়ে) ---
    logging.info(f"[{video_id}] সফলভাবে তৈরি হওয়া রেজোলিউশনগুলো দিয়ে মাস্টার প্লেলিস্ট তৈরি করা হচ্ছে...")
    for detail in resolution_details_for_master:
        # গণনা করা প্রস্থ ও উচ্চতা ব্যবহার করুন
        master_playlist_content += f'#EXT-X-STREAM-INF:BANDWIDTH={detail["bandwidth"]},RESOLUTION={detail["width"]}x{detail["height"]}\n'
        master_playlist_content += f'{detail["playlist_path"]}\n' # রিলেটিভ পাথ যোগ করুন

    master_playlist_path = os.path.join(output_base_dir, MASTER_PLAYLIST_NAME) # মাস্টার ফাইলের পাথ
    ready_file_path = os.path.join(output_base_dir, HLS_READY_FILENAME) # রেডি ফাইলের পাথ
    try:
        # মাস্টার প্লেলিস্ট ফাইল লিখুন
        with open(master_playlist_path, 'w') as f:
            f.write(master_playlist_content)
        logging.info(f"[{video_id}] মাস্টার প্লেলিস্ট সফলভাবে তৈরি হয়েছে: {master_playlist_path}")

        # রেডি ফাইল তৈরি করুন (প্রসেসিং সফল নির্দেশক)
        with open(ready_file_path, 'w') as f:
             f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
        logging.info(f"[{video_id}] HLS প্রসেসিং সম্পন্ন। রেডি মার্কার তৈরি হয়েছে: {ready_file_path}")
        # মোট সময় লগ করার প্রয়োজন নেই কারণ এটি লুপের মধ্যে পরিবর্তিত হতে পারে
        return True # সফল রিটার্ন করুন
    except IOError as e:
        # যদি প্লেলিস্ট বা রেডি ফাইল লিখতে সমস্যা হয়
        error_msg = f"[{video_id}] মাস্টার প্লেলিস্ট বা রেডি মার্কার লিখতে ব্যর্থ: {e}"
        logging.error(error_msg)
        try:
            # যদি আগে কোনো ত্রুটি ফাইল না থাকে, তবে এটি লিখুন
            if not os.path.exists(error_file_path):
                 with open(error_file_path, 'w') as f: f.write(error_msg)
        except IOError as io_err:
             logging.error(f"[{video_id}] প্লেলিস্ট লেখা ত্রুটি ফাইল লিখতে ব্যর্থ: {error_file_path}: {io_err}")
        return False # ব্যর্থ রিটার্ন করুন
    # --- মাস্টার প্লেলিস্ট তৈরি শেষ ---


def run_processing_job(video_id, uploaded_video_path, hls_output_dir):
    """নির্দিষ্ট আপলোড করা ভিডিও ট্রান্সকোড করার মূল থ্রেড ফাংশন।"""
    lock_file_path = os.path.join(hls_output_dir, PROCESSING_LOCK_FILENAME)   # লক ফাইলের পাথ
    error_file_path = os.path.join(hls_output_dir, PROCESSING_ERROR_FILENAME) # ত্রুটি ফাইলের পাথ
    ready_file_path = os.path.join(hls_output_dir, HLS_READY_FILENAME)     # রেডি ফাইলের পাথ

    logging.info(f"[{video_id}] প্রসেসিং থ্রেড শুরু হয়েছে।")

    try:
        # এই ভিডিও আইডির জন্য আগের HLS কনটেন্ট ও স্টেট ফাইল মুছে ফেলুন
        clear_hls_directory_contents(hls_output_dir)
        if os.path.exists(error_file_path): os.remove(error_file_path)
        if os.path.exists(ready_file_path): os.remove(ready_file_path)

        # ffmpeg উপলব্ধ কিনা তা পরীক্ষা করুন (ffprobe আগে চেক করা হয়েছে)
        if not check_command('ffmpeg'):
             error_msg = f"[{video_id}] ffmpeg পরীক্ষা ব্যর্থ। প্রসেসিং বাতিল করা হচ্ছে।"
             logging.critical(error_msg)
             with open(error_file_path, 'w') as f: f.write(error_msg)
             return # থ্রেড থেকে প্রস্থান করুন

        # ট্রান্সকোডিং শুরু করুন (যেখানে ডাইমেনশন গণনা অন্তর্ভুক্ত)
        if not transcode_to_hls(video_id, uploaded_video_path, hls_output_dir, RESOLUTIONS):
            logging.error(f"[{video_id}] ট্রান্সকোডিং ধাপ ব্যর্থ হয়েছে।")
            # transcode_to_hls ফাংশন ত্রুটি ফাইল লেখার কথা
            return # থ্রেড থেকে প্রস্থান করুন

        # যদি সব ঠিক থাকে
        logging.info(f"[{video_id}] প্রসেসিং সফলভাবে সম্পন্ন হয়েছে।")

    except Exception as e:
        # যদি থ্রেডে কোনো অপ্রত্যাশিত ত্রুটি ঘটে
        error_msg = f"[{video_id}] প্রসেসিং থ্রেডে মারাত্মক অপ্রত্যাশিত ত্রুটি: {e}"
        logging.error(error_msg, exc_info=True)
        try:
            # যদি আগে কোনো ত্রুটি ফাইল না থাকে তবে এটি লিখুন
            if not os.path.exists(error_file_path):
                 with open(error_file_path, 'w') as f: f.write(error_msg)
        except IOError as io_err: logging.error(f"[{video_id}] মারাত্মক ত্রুটি ফাইল লিখতে ব্যর্থ: {io_err}")

    finally:
        # প্রসেসিং সফল বা ব্যর্থ যাই হোক না কেন, নির্দিষ্ট ভিডিওর লক ফাইলটি মুছে ফেলুন
        if os.path.exists(lock_file_path):
            try:
                os.remove(lock_file_path)
                logging.info(f"[{video_id}] প্রসেসিং লক ফাইল মুছে ফেলা হয়েছে: {lock_file_path}")
            except OSError as e:
                logging.error(f"[{video_id}] প্রসেসিং লক ফাইল মুছতে ব্যর্থ: {e}")
        # ঐচ্ছিক: প্রসেসিংয়ের পর আপলোড করা সোর্স ফাইল মুছে ফেলা (কমেন্ট আউট করা আছে)
        # if os.path.exists(uploaded_video_path):
        #     try: os.remove(uploaded_video_path) ...


# === Flask Routes ===
# Flask অ্যাপ্লিকেশন রুট (URL পাথ এবং সংশ্লিষ্ট ফাংশন) - আগের মতোই

@app.route('/', methods=['GET'])
def index():
    """মূল পাতা দেখায়, যেখানে ভিডিও আপলোড করার ফর্ম থাকে।"""
    logging.info("ইনডেক্স পেজ রেন্ডার করা হচ্ছে (আপলোড ফর্ম)।")
    return render_template('index.html') # index.html টেমপ্লেট দেখান

@app.route('/upload', methods=['POST'])
def upload_file():
    """ভিডিও ফাইল আপলোড হ্যান্ডেল করে, আইডি নির্ধারণ করে এবং প্রসেসিং শুরু করে।"""
    # চেক করুন ফাইল রিকোয়েস্টে আছে কিনা
    if 'video' not in request.files:
        flash('কোন ফাইল অংশ নেই।') # ব্যবহারকারীকে মেসেজ দেখান
        return redirect(url_for('index')) # ইনডেক্স পেজে ফেরত পাঠান

    file = request.files['video'] # ফাইল অবজেক্ট পান
    # যদি ব্যবহারকারী ফাইল সিলেক্ট না করে সাবমিট করে
    if file.filename == '':
        flash('কোন ফাইল নির্বাচন করা হয়নি।')
        return redirect(url_for('index'))

    # যদি ফাইল থাকে এবং এক্সটেনশন অনুমোদিত হয়
    if file and allowed_file(file.filename):
        original_filename = file.filename # আসল ফাইলের নাম
        file_ext = original_filename.rsplit('.', 1)[1].lower() # ফাইলের এক্সটেনশন
        video_id = str(uuid.uuid4()) # ইউনিক ভিডিও আইডি তৈরি করুন
        logging.info(f"আপলোড গৃহীত হয়েছে '{original_filename}', আইডি নির্ধারিত: {video_id}")

        # এই ভিডিওর জন্য নির্দিষ্ট পাথ নির্ধারণ করুন
        video_upload_dir = os.path.join(UPLOAD_DIR, video_id)   # যেমন: uploads/uuid
        video_hls_dir = os.path.join(HLS_DIR, video_id)         # যেমন: static/hls/uuid
        save_path = os.path.join(video_upload_dir, f"{SOURCE_VIDEO_BASENAME}.{file_ext}") # সেভ করার পাথ
        lock_file_path = os.path.join(video_hls_dir, PROCESSING_LOCK_FILENAME) # লক ফাইলের পাথ

        try:
            # প্রয়োজনীয় ডিরেক্টরি তৈরি করুন
            ensure_dir(video_upload_dir)
            ensure_dir(video_hls_dir) # লক ফাইলের জন্য HLS ডিরেক্টরিও দরকার

            # যদি এই আইডির জন্য ইতিমধ্যে প্রসেসিং চলে (খুব বিরল UUID এর ক্ষেত্রে, কিন্তু ভালো অভ্যাস)
            if os.path.exists(lock_file_path):
                flash(f"ভিডিও আইডি {video_id} ইতিমধ্যে প্রসেস করা হচ্ছে।")
                logging.warning(f"[{video_id}] লক ফাইল থাকা অবস্থায় আপলোডের চেষ্টা।")
                return redirect(url_for('video_status', video_id=video_id)) # স্ট্যাটাস পেজে পাঠান

            # আপলোড করা ফাইল সেভ করুন
            file.save(save_path)
            logging.info(f"[{video_id}] ফাইল সেভ করা হয়েছে: {save_path}")

            # থ্রেড শুরু করার *আগে* লক ফাইল তৈরি করুন
            with open(lock_file_path, 'w') as f:
                f.write(f'Processing started at: {time.strftime("%Y-%m-%d %H:%M:%S")} for {original_filename}')
            logging.info(f"[{video_id}] প্রসেসিং লক ফাইল তৈরি হয়েছে: {lock_file_path}")

            # ব্যাকগ্রাউন্ড থ্রেডে ট্রান্সকোডিং প্রসেস শুরু করুন
            logging.info(f"[{video_id}] ব্যাকগ্রাউন্ড প্রসেসিং থ্রেড শুরু হচ্ছে...")
            processing_thread = threading.Thread(
                target=run_processing_job, # যে ফাংশনটি চলবে
                args=(video_id, save_path, video_hls_dir), # ফাংশনের আর্গুমেন্ট
                name=f"ProcessingThread-{video_id}", # থ্রেডের নাম
                daemon=True # অ্যাপ বন্ধ হলে থ্রেডও বন্ধ হবে
            )
            processing_thread.start() # থ্রেড শুরু করুন

            # ব্যবহারকারীকে মেসেজ দেখান এবং স্ট্যাটাস পেজে রিডাইরেক্ট করুন
            flash(f'"{original_filename}" আপলোড সফল! আইডি: {video_id}. ভিডিওটি এখন প্রসেস হচ্ছে।')
            return redirect(url_for('video_status', video_id=video_id))

        except Exception as e:
            # যদি ফাইল সেভ বা থ্রেড শুরু করতে কোনো ত্রুটি হয়
            flash(f'ফাইল সেভ বা প্রসেসিং শুরু করতে ত্রুটি: {e}')
            logging.error(f"[{video_id or 'UNKNOWN'}] আপলোড হ্যান্ডলিংয়ের সময় ত্রুটি: {e}", exc_info=True)
            # ত্রুটি ঘটলে তৈরি হওয়া ফাইল/ডিরেক্টরি পরিষ্কার করার চেষ্টা করুন
            if 'lock_file_path' in locals() and os.path.exists(lock_file_path):
                try: os.remove(lock_file_path)
                except OSError: pass
            if 'video_hls_dir' in locals() and os.path.exists(video_hls_dir):
                 try: shutil.rmtree(video_hls_dir)
                 except OSError: pass
            if 'video_upload_dir' in locals() and os.path.exists(video_upload_dir):
                 try: shutil.rmtree(video_upload_dir)
                 except OSError: pass
            return redirect(url_for('index')) # ইনডেক্স পেজে ফেরত পাঠান

    else:
        # যদি ফাইলের এক্সটেনশন অনুমোদিত না হয়
        flash('অবৈধ ফাইলের প্রকার। অনুমোদিত প্রকারগুলি: ' + ', '.join(ALLOWED_EXTENSIONS))
        return redirect(url_for('index'))


@app.route('/video/<video_id>')
def video_status(video_id):
    """নির্দিষ্ট ভিডিও আইডির স্ট্যাটাস (প্রসেসিং, রেডি, এরর) বা প্লেয়ার দেখায়।"""
    logging.info(f"[{video_id}] স্ট্যাটাস চেকের অনুরোধ এসেছে।")
    video_hls_dir = os.path.join(HLS_DIR, video_id) # এই ভিডিওর HLS ডিরেক্টরি

    # এই ভিডিওর স্টেট ফাইলগুলোর পাথ
    lock_file_path = os.path.join(video_hls_dir, PROCESSING_LOCK_FILENAME)
    ready_file_path = os.path.join(video_hls_dir, HLS_READY_FILENAME)
    error_file_path = os.path.join(video_hls_dir, PROCESSING_ERROR_FILENAME)

    status = 'not_found' # ডিফল্ট স্ট্যাটাস
    error_message = None # ত্রুটির বার্তা
    hls_ready = False    # HLS রেডি কিনা
    processing = False   # প্রসেসিং চলছে কিনা

    # ভিডিওর ডিরেক্টরি আছে কিনা তা পরীক্ষা করুন
    if not os.path.isdir(video_hls_dir) and not os.path.isdir(os.path.join(UPLOAD_DIR, video_id)):
         logging.warning(f"[{video_id}] HLS এবং আপলোড ডিরেক্টরি খুঁজে পাওয়া যায়নি।")
         # স্ট্যাটাস 'not_found' থাকবে
         pass
    # ত্রুটি ফাইল আছে কিনা পরীক্ষা করুন
    elif os.path.exists(error_file_path):
        status = 'error'
        try:
            with open(error_file_path, 'r') as f:
                error_message = f.read() # ত্রুটির বার্তা পড়ুন
            logging.warning(f"[{video_id}] ত্রুটি ফাইল পাওয়া গেছে: {error_file_path}")
        except Exception as e:
            error_message = f"ত্রুটি ফাইল পড়তে সমস্যা: {e}"
            logging.error(f"[{video_id}] {error_message}")
    # রেডি ফাইল আছে কিনা পরীক্ষা করুন
    elif os.path.exists(ready_file_path):
        status = 'ready'
        hls_ready = True
        logging.info(f"[{video_id}] রেডি ফাইল পাওয়া গেছে: {ready_file_path}")
    # লক ফাইল আছে কিনা পরীক্ষা করুন
    elif os.path.exists(lock_file_path):
        status = 'processing'
        processing = True
        logging.info(f"[{video_id}] লক ফাইল পাওয়া গেছে: {lock_file_path}")
    else:
         # ডিরেক্টরি আছে কিন্তু কোনো স্টেট ফাইল নেই (সম্ভবত মুছে ফেলা হয়েছে বা প্রসেসিং অসম্পূর্ণ)
         logging.warning(f"[{video_id}] কোনো স্টেট ফাইল পাওয়া যায়নি ({video_hls_dir})। স্ট্যাটাস 'not_found' ধরা হচ্ছে।")
         status = 'not_found'

    # টেমপ্লেট রেন্ডার করুন এবং স্ট্যাটাস সম্পর্কিত ভেরিয়েবলগুলো পাস করুন
    logging.info(f"[{video_id}] video_status.html রেন্ডার করা হচ্ছে স্ট্যাটাস: {status}")
    return render_template('video_status.html',
                           video_id=video_id,
                           status=status,
                           hls_ready=hls_ready,
                           processing=processing,
                           error=error_message)


@app.route('/hls/<video_id>/<path:filename>')
def serve_hls_files(video_id, filename):
    """নির্দিষ্ট ভিডিও আইডির HLS ফাইল (.m3u8, .ts) সার্ভ করে।"""
    video_hls_dir = os.path.join(HLS_DIR, video_id) # ভিডিওর HLS ডিরেক্টরি
    logging.debug(f"[{video_id}] HLS ফাইলের অনুরোধ: {filename} ডিরেক্টরি থেকে: {video_hls_dir}")

    # নিরাপত্তা পরীক্ষা: ডিরেক্টরি ট্র্যাভার্সাল অ্যাটাক প্রতিরোধ
    if '..' in filename or filename.startswith('/') or '..' in video_id or video_id.startswith('/'):
        logging.warning(f"[{video_id}] ডিরেক্টরি ট্র্যাভার্সাল প্রচেষ্টা ব্লক করা হয়েছে: {filename}")
        abort(403) # Forbidden

    # ফাইলের সম্পূর্ণ পাথ তৈরি করুন
    file_path = os.path.join(video_hls_dir, filename)

    # ভিডিওর HLS ডিরেক্টরি আছে কিনা তা নিশ্চিত করুন
    if not os.path.isdir(video_hls_dir):
        logging.warning(f"[{video_id}] HLS ডিরেক্টরি খুঁজে পাওয়া যায়নি: {video_hls_dir}")
        abort(404) # Not Found

    # অতিরিক্ত নিরাপত্তা পরীক্ষা: নিশ্চিত করুন যে পাথটি সত্যিই HLS ডিরেক্টরির ভিতরে
    abs_hls_dir = os.path.abspath(video_hls_dir)
    abs_file_path = os.path.abspath(file_path)
    if not abs_file_path.startswith(abs_hls_dir):
         logging.error(f"[{video_id}] নিরাপত্তা ঝুঁকি: HLS ডিরেক্টরির বাইরের ফাইল অ্যাক্সেসের চেষ্টা: {abs_file_path}")
         abort(403) # Forbidden

    # নির্দিষ্ট ফাইলটি ঐ ডিরেক্টরিতে আছে কিনা তা পরীক্ষা করুন
    if not os.path.isfile(file_path):
        logging.warning(f"[{video_id}] HLS ফাইল খুঁজে পাওয়া যায়নি: {file_path}")
        abort(404) # Not Found

    try:
        # ফাইলটি সার্ভ করুন (conditional=True ক্যাশিং উন্নত করে)
        return send_from_directory(video_hls_dir, filename, conditional=True)
    except FileNotFoundError:
        # যদিও isfile চেক করা হয়েছে, এটি একটি অতিরিক্ত নিরাপত্তা স্তর
        logging.warning(f"[{video_id}] send_from_directory দ্বারা HLS ফাইল খুঁজে পাওয়া যায়নি: {file_path}")
        abort(404)
    except Exception as e:
        # ফাইল সার্ভ করার সময় অন্য কোনো ত্রুটি ঘটলে
        logging.error(f"[{video_id}] HLS ফাইল সার্ভ করতে ত্রুটি ({filename}): {e}", exc_info=True)
        abort(500) # Internal Server Error

# === Application Startup ===
# অ্যাপ্লিকেশন শুরু হওয়ার সময় করণীয়

# প্রয়োজনীয় কমান্ডগুলো উপলব্ধ কিনা তা পরীক্ষা করুন
logging.info("প্রয়োজনীয় কমান্ড পরীক্ষা করা হচ্ছে (ffmpeg, ffprobe)...")
ffmpeg_ok = check_command('ffmpeg')
ffprobe_ok = check_command('ffprobe')
if not ffmpeg_ok:
    logging.critical("ffmpeg প্রয়োজন কিন্তু উপলব্ধ নেই। প্রসেসিং ব্যর্থ হবে।")
if not ffprobe_ok:
     logging.critical("ffprobe ডাইমেনশন সনাক্তকরণের জন্য প্রয়োজন কিন্তু উপলব্ধ নেই। প্রসেসিং ব্যর্থ হতে পারে বা রেজোলিউশন ট্যাগ সঠিক নাও হতে পারে।")

# অ্যাপ্লিকেশন চালু হওয়ার সময় প্রয়োজনীয় ডিরেক্টরিগুলো তৈরি করুন
logging.info("প্রয়োজনীয় ডিরেক্টরি নিশ্চিত করা হচ্ছে...")
ensure_dir(BASE_DIR)
ensure_dir(UPLOAD_DIR)
ensure_dir(STATIC_DIR)
ensure_dir(HLS_DIR)

# === Main Execution Block ===
# মূল এক্সিকিউশন ব্লক

if __name__ == '__main__':
    # অ্যাপ্লিকেশনটি চালান
    logging.info("Flask অ্যাপ্লিকেশন শুরু হচ্ছে...")
    # debug=False প্রোডাকশন বা স্থিতিশীলতার জন্য ভালো
    app.run(host='0.0.0.0', port=8000, debug=False)

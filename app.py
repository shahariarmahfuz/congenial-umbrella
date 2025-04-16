import os
import uuid
import subprocess
import requests
from flask import Flask, render_template

app = Flask(__name__)

VIDEO_FOLDER = 'static/videos'
VIDEO_URL = 'https://www.dropbox.com/scl/fi/dx4v458ut0ko9fyt8avdk/7TMX-8LHS-2VG5-WU77_HD.mp4?rlkey=sr7pionn5z4cfbbu6l58un49d&raw=1'

if not os.path.exists(VIDEO_FOLDER):
    os.makedirs(VIDEO_FOLDER)

@app.route('/')
def index():
    video_id = str(uuid.uuid4())
    video_path = os.path.join(VIDEO_FOLDER, f'{video_id}.mp4')
    hls_dir = os.path.join(VIDEO_FOLDER, video_id)

    os.makedirs(hls_dir, exist_ok=True)

    # Download video from Dropbox
    with requests.get(VIDEO_URL, stream=True) as r:
        with open(video_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    # Convert to HLS (240p, 360p, 720p)
    master_playlist = os.path.join(hls_dir, 'master.m3u8')
    cmd = f"""
    ffmpeg -i "{video_path}" -filter_complex \
    "[0:v]split=3[v1][v2][v3]; \
    [v1]scale=w=426:h=240[v1out]; \
    [v2]scale=w=640:h=360[v2out]; \
    [v3]scale=w=1280:h=720[v3out]" \
    -map [v1out] -c:v:0 libx264 -b:v:0 400k -map a:0 -c:a:0 aac -f hls -hls_time 6 -hls_playlist_type vod {hls_dir}/240p.m3u8 \
    -map [v2out] -c:v:1 libx264 -b:v:1 800k -map a:0 -c:a:1 aac -f hls -hls_time 6 -hls_playlist_type vod {hls_dir}/360p.m3u8 \
    -map [v3out] -c:v:2 libx264 -b:v:2 1500k -map a:0 -c:a:2 aac -f hls -hls_time 6 -hls_playlist_type vod {hls_dir}/720p.m3u8
    """

    subprocess.call(cmd, shell=True)

    # Write master playlist
    with open(master_playlist, 'w') as m3u8:
        m3u8.write("#EXTM3U\n")
        m3u8.write("#EXT-X-STREAM-INF:BANDWIDTH=500000,RESOLUTION=426x240\n240p.m3u8\n")
        m3u8.write("#EXT-X-STREAM-INF:BANDWIDTH=1000000,RESOLUTION=640x360\n360p.m3u8\n")
        m3u8.write("#EXT-X-STREAM-INF:BANDWIDTH=2000000,RESOLUTION=1280x720\n720p.m3u8\n")

    video_url = f'/{hls_dir}/master.m3u8'

    return render_template('index.html', video_url=video_url)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

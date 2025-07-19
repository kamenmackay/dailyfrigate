import argparse
import aiohttp
import asyncio
import ffmpeg
import os
from concurrent.futures import ProcessPoolExecutor
from tempfile import TemporaryDirectory
from tqdm import tqdm
from functools import lru_cache
import datetime
import requests

# Parse command-line arguments
parser = argparse.ArgumentParser(
    description="Process clips with specified timestamp, camera, zone, and label"
)
parser.add_argument("timestamp", help="ISO 8601 timestamp")
parser.add_argument("camera", help="Camera name")
parser.add_argument("zone", help="Zone name")
parser.add_argument("label", help="Label name")
parser.add_argument(
    "--output-dir", help="Output directory (default: current directory)", default="."
)
args = parser.parse_args()

num_pools = 10
temp_root_dir = "/Users/kmackay/nobackup/testy/"
eventUrl = "http://lenny:5000/api/events/"

def cvt_to_epoch(datestring):
    tstamp = datetime.datetime.fromisoformat(datestring)
    tstampepoch = tstamp
    return tstampepoch

def fetchClipInfo(startDate, camera, zone, label):
    start_date_dt = datetime.datetime.strptime(startDate, "%Y-%m-%d")
    end_date_dt = start_date_dt + datetime.timedelta(days=1, seconds=-1)
    after = int(start_date_dt.timestamp())
    before = int(end_date_dt.timestamp())
    request_url = f"http://lenny:5000/api/events?has_clip=1&limit=500&label={label}&camera={camera}&zone={zone}&before={before}&after={after}"
    response = requests.get(request_url)
    if response.ok:
        return response.json()
    else:
        response.raise_for_status()

@lru_cache(maxsize=128)
def get_clip_duration(url):
    try:
        probe = ffmpeg.probe(url)
        return float(probe["streams"][0]["duration"])
    except ffmpeg._run.Error:
        return None

async def download_thumbnail(event_id, idx, session):
    url = f"http://lenny:5000/api/events/{event_id}/thumbnail.jpg"
    async with session.get(url) as response:
        if response.status == 200:
            thumbnail_path = f"thumbnail_{idx}.jpg"
            with open(thumbnail_path, "wb") as f:
                f.write(await response.read())
            return thumbnail_path
        else:
            print(f"Failed to download thumbnail for event {event_id}")
            return None

async def download_thumbnails(clip_info_list):
    async with aiohttp.ClientSession() as session:
        tasks = [download_thumbnail(clip_info[0].split("/")[-2], idx, session) for clip_info, idx in clip_info_list]
        print(tasks)
        return await asyncio.gather(*tasks)

def process_clip(clip_info, thumbnail_path):
    clip, idx = clip_info
    duration = get_clip_duration(clip)
    if duration is None or duration > 90:
        print(f"Error: Failed to get duration for clip {idx+1}/{len(idList)}: {clip}")
        return None
    try:
        if thumbnail_path:
            newClip = (
                ffmpeg
                .filter([ffmpeg.input(clip), ffmpeg.input(thumbnail_path)], 'overlay', x='W-w-10', y=10)
                .drawtext(text=f"{idx+1}/{len(idList)}", x='W-w-10', y='h+20', fontsize=48, fontcolor="red")
            )
            return newClip
        else:
            newClip = (
                ffmpeg
                .input(clip)
                .drawtext(text=f"{idx+1}/{len(idList)}", x=10, y=10, fontsize=48, fontcolor="red")
            )
            return newClip
    except ffmpeg.Error as e:
        print(f"Error: Failed to process clip {idx+1}/{len(idList)}: {clip}")
        print(e.stderr)
        return None

def process_chunk(chunk, chunk_id, temp_dir, thumbnails):
    with ProcessPoolExecutor() as executor:
        clip_streams = list(
            tqdm(
                executor.map(process_clip, chunk, thumbnails),
                desc=f"Processing chunk {chunk_id+1}",
                total=len(chunk),
                ncols=100,
            )
        )
    validclips = [clip for clip in clip_streams if clip is not None]
    joined = ffmpeg.concat(*validclips, unsafe=True)
    temp_file = os.path.join(temp_dir, f"temp_{chunk_id}.mp4")
    print(temp_file)
    print(joined)
    print(f"Joining {len(validclips)} bits from this chunk")
    out = ffmpeg.output(
        joined,
        temp_file,
        vcodec="libx265",
        pix_fmt="yuv420p",
        **{
            "x265-params": f"pools={num_pools}",
            "preset": "slow",
            "tag": "hvc1"
        },
    ).global_args("-fflags", "+igndts", "-vsync", "2", "-threads", "1")
    try:
        ffmpeg.run(out, capture_stderr=True, capture_stdout=True)
        print(ffmpeg.compile)
    except ffmpeg.Error as e:
        print(f"Error: Failed to join clips in chunk {chunk_id+1}")
        print(e.stderr)
    return temp_file

async def main():
    clipList = fetchClipInfo(args.timestamp, args.camera, args.zone, args.label)
    clipList.sort(key=lambda x: x["start_time"])
    idList = [(eventUrl + x["id"] + "/clip.mp4", idx) for idx, x in enumerate(clipList)]
    chunk_size = 100
    chunks = [idList[i : i + chunk_size] for i in range(0, len(idList), chunk_size)]

    temp_dir_obj = TemporaryDirectory(dir="/Users/kmackay/nobackup/testy/")
    temp_dir = temp_dir_obj.name
    temp_files = []

    for i, chunk in enumerate(chunks):
        # Add debugging information
        print(f"Processing chunk {i+1}/{len(chunks)}")
        print("Chunk content:", chunk)

        thumbnails = await download_thumbnails(chunk)
        temp_file = process_chunk(chunk, i, temp_dir, thumbnails)
        temp_files.append(temp_file)

    print("Joining temporary files")
    temp_file_inputs = [ffmpeg.input(temp_file) for temp_file in temp_files]

    try:
        final_joined = ffmpeg.concat(*temp_file_inputs, unsafe=True)
    except ValueError:
        print("No clips to be processed")
        return

    output_file_name = f"{args.timestamp}-{args.camera}-{args.zone}-{args.label}.mp4"
    output_path = os.path.join(args.output_dir, output_file_name)

    try:
        ffmpeg.output(final_joined, output_path).run()
        print(f"Output saved to {output_path}")
    except ffmpeg.Error as e:
        print(f"Error: Failed to save the final output to {output_path}")
        print(e.stderr)

if __name__ == "__main__":
    asyncio.run(main())

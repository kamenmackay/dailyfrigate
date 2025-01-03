import argparse
import dailyfrigate_refactor
import ffmpeg
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from tempfile import TemporaryDirectory
import shutil
from tqdm import tqdm
from functools import cache

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
clipList = dailyfrigate_refactor.fetchClipInfo(
    args.timestamp, args.camera, args.zone, args.label
)

# Sort the clipList based on timestamps
clipList.sort(key=lambda x: x["start_time"])

# Modify the idList to include both the URL and the index
idList = [(eventUrl + x["id"] + "/clip.mp4", idx) for idx, x in enumerate(clipList)]
print(f"{len(idList)} items")

@cache
def get_clip_duration(url):
    try:
        probe = ffmpeg.probe(url)
        return float(probe["streams"][0]["duration"])
    except ffmpeg._run.Error as e:
        return None

@cache
def process_clip(clip_info):
    clip, idx = clip_info

    duration = get_clip_duration(clip)
    if duration is None:
        pass
        return None
    try:
        newClip = ffmpeg.input(clip).drawtext(text=f"{idx+1}/{len(idList)}", x=10, y=10, fontsize=48, fontcolor="white")
        return newClip
    except ffmpeg.Error as e:
        pass
        print(e.stderr)
        return None

    

def process_chunk(chunk, chunk_id, temp_dir):
    with ThreadPoolExecutor() as executor:
        clip_streams = list(
            tqdm(
                executor.map(process_clip, chunk),
                desc=f"Processing chunk {chunk_id+1}",
                total=len(chunk),
                ncols=100,
            )
        )

    validclips = [clip for clip in clip_streams if clip is not None]

    joined = ffmpeg.concat(*validclips, unsafe=True)
    temp_file = os.path.join(temp_dir, f"temp_{chunk_id}.mp4")

    out = ffmpeg.output(
        joined,
        temp_file,
        vcodec="libx265",
        pix_fmt="yuv420p",
        **{
            "x265-params": f"pools={num_pools}",
            "preset": "slow",
            "tag": "hvc1",
            "crf": "28"
     
        },
    ).global_args("-fflags", "+igndts", "-vsync", "2", "-threads", "1")
    try:
        ffmpeg.run(out, capture_stderr=True, capture_stdout=True)
    except ffmpeg.Error as e:
        print(e.stderr)

    return temp_file


chunk_size = 100
chunks = [idList[i : i + chunk_size] for i in range(0, len(idList), chunk_size)]

temp_dir_obj = TemporaryDirectory(dir="/Users/kmackay/nobackup/testy/")
temp_dir = temp_dir_obj.name
temp_files = []

for i, chunk in enumerate(chunks):
    temp_file = process_chunk(chunk, i, temp_dir)
    temp_files.append(temp_file)

print("Joining temporary files")
temp_file_inputs = [ffmpeg.input(temp_file) for temp_file in temp_files]
final_joined = ffmpeg.concat(*temp_file_inputs, unsafe=True)

# Build the output file name and path
output_file_name = f"{args.timestamp}-{args.camera}-{args.zone}-{args.label}.mp4"
output_path = os.path.join(args.output_dir, output_file_name)

# Save the final output to the specified directory
try:
    ffmpeg.output(final_joined, output_path).run()
except ffmpeg.Error as e:
    print(e.stderr)

print(f"Output saved to {output_path}")

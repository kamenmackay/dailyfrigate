import os
import tempfile
import ffmpeg
import argparse
from datetime import datetime


def concatenate_videos(input_dir, output_file):
    with tempfile.NamedTemporaryFile(
        mode="w+", delete=False, dir="/tmp", suffix=".txt"
    ) as tmp_file:
        if not os.path.exists(input_dir):
            return False
        files = [f for f in os.listdir(input_dir) if f.endswith(".mp4") ]
        if not files:
            return False
        sorted_files = sorted(files)

        for index, f in enumerate(sorted_files):
            full_path = os.path.join(input_dir, f)
            f"Clip {index + 1}"
            tmp_file.write(f"file '{full_path}'\n")
            # tmp_file.write("duration 00:00:05\n")  # Adjust duration as needed

        tmp_file.write(
            f"file '{sorted_files[-1]}'\n"
        )  # Add last file again to compensate for duration line
        tmp_file_name = tmp_file.name

    
        ffmpeg.input(tmp_file_name, format="concat", safe=0).output(output_file, c="v", codec="copy").global_args("-fflags", "+igndts", "-threads", "1").run()
    

    os.remove(tmp_file_name)


def process_directory(camera, day, destdir):
    for dt in [f"{hour:02}" for hour in range(24)]:
        input_dir = f"/Volumes/cams/recordings/{day}/{dt}/{camera}/"
        output_file = f"{destdir}/{day}/{camera}-{day}-{dt}.mp4"
        concatenate_videos(input_dir, output_file)


def main():
    parser = argparse.ArgumentParser(
        description="Concatenate video files for a specific camera and day."
    )
    parser.add_argument("--camera", required=True, help="Name of the camera")
    parser.add_argument("--day", required=True, help="Day in YYYY-MM-DD format")

    args = parser.parse_args()

    camera = args.camera
    day = args.day
    destdir = "/Volumes/exports/"

    # Validate day format
    try:
        datetime.strptime(day, "%Y-%m-%d")
    except ValueError:
        parser.error("Incorrect data format, should be YYYY-MM-DD")

    os.makedirs(os.path.join(destdir, day), exist_ok=True)
    process_directory(camera, day, destdir)


if __name__ == "__main__":
    main()

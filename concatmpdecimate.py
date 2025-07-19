import ffmpeg
import glob
import tempfile
import datetime
import os
import argparse

def concatDirectory(inputdir):
    input_files = sorted(glob.glob(f"{inputdir}/*.mp4"))
    with tempfile.NamedTemporaryFile(delete=False, mode="w", newline="") as temp_file:
        for file in input_files:
            temp_file.write(f"file '{file}'\n")
        temp_file_path = temp_file.name
    print("Building list of files to concatenate")
    input_list = ffmpeg.input(temp_file_path, format="concat", safe=0)
    return input_list

def makeItOne(concattedfile, output_file):
    output = ffmpeg.output(
        concattedfile, output_file, vcodec="copy"
    )
    ffmpeg.run(output)
    return output_file

def delFrames(input_file, output_file):
    inputvid = ffmpeg.input(input_file)
    filtered_video = inputvid.filter("mpdecimate", hi=7800, lo=1600, frac=0.1).filter(
        "setpts", "N/FRAME_RATE/TB"
    )
    out = ffmpeg.output(filtered_video, output_file, vcodec="libx265", preset="ultrafast", tag="hvc1", pix_fmt="yuv420p",r=20,**{'b:v': '4000k'})
    # out = ffmpeg.output(filtered_video, output_file, vcodec="hevc_videotoolbox", tag="hvc1", pix_fmt="yuv420p",r=20,**{'b:v': '2000k','prio_speed': "false", 'power_efficient': -1, 'profile': 1, 'qmin': 20, 'qmax': 40})
    ffmpeg.run(out)

def getOutputFileName(output_dir, prefix, date, hour):
    # now = datetime.datetime.now()
    # timestamp = now.strftime("%H%M%S")
    return os.path.join(output_dir, f"{prefix}_{date}_{hour:02}.mp4")


def processHourFiles(base_dir, date, zone, output_dir):
    for hour in range(24):
        hour_dir = f"{base_dir}/{date}/{hour:02}/{zone}"
        print(f"Processing directory: {hour_dir}")

        if not os.path.exists(hour_dir):
            print(f"Creating {hour_dir}")
            os.mkdir(hour_dir)
            continue

        concatted = concatDirectory(hour_dir)

        temp_output_file = getOutputFileName(output_dir, "concatted", date, hour)
        makeItOne(concatted, temp_output_file)

        final_output_file = getOutputFileName(output_dir, "deduped", date, hour)
        delFrames(temp_output_file, final_output_file)

        print(f"Concatenated file for hour {hour:02}: {temp_output_file}")
        print(f"Deduplicated file for hour {hour:02}: {final_output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process video files by zone and date.")
    parser.add_argument("zone", type=str, help="The zone (e.g., frontgarden)")
    parser.add_argument("date", type=str, help="The date (e.g., 2024-07-24)")
    parser.add_argument("output_dir", type=str, help="The output directory")
    args = parser.parse_args()

    base_dir = "/Volumes/cams/recordings"

    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)
    processHourFiles(base_dir, args.date, args.zone, args.output_dir)

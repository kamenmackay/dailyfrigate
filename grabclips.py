from distutils.log import log
import requests
import subprocess
import ffmpeg
import os
import argparse
from eliot import start_action, to_file, log_call
from datetime import datetime

to_file(open("isClipValid.log", "w+"))
from signal import signal, SIGINT
from sys import exit
import datetime
import time
from tqdm import tqdm
from signal import pause
import json
import configparser

# Parse the timestamp (2022-10-08) to unix time
# Scrape the api to download the requested clips
# Concatenate all the downloaded clips into one big file
# Re-encode the big clip to x265 format for space savings

config = configparser.ConfigParser()
config.read("dailyfrigate.cfg")
clip_path = config["General"]["clip_path"]
output_path = config["General"]["output_path"]
frigateurl = config["General"]["frigateurl"]
ffmpegbin = config["General"]["ffmpegbin"]


@log_call
def handler(signal_received, frame):
    # Handle any cleanup here
    try:
        print("SIGINT or CTRL-C detected. Exiting gracefully")
        cleanup(fileList)
        exit(0)
    except:
        print("burp!")
        exit(5)


signal(SIGINT, handler)
fileList = []
streamList = []
textfile = open("concat.txt", "w")


# Put these into a config file already!


@log_call
def cvt_to_epoch(datestring):
    tstamp = datetime.datetime.fromisoformat(datestring)
    tstampepoch = tstamp
    return tstampepoch


def fetchClipInfo(startDate, camera, zone, label):
    print(startDate)
    # zone = "road"
    # camera = "frontgate"
    before = cvt_to_epoch(startDate)
    # print(type(before))
    after = before - datetime.timedelta(days=1)
    requestxt = f"http://lenny:5000/api/events?has_clip=1&limit=99999&label={label}&camera={camera}&zone={zone}&before={before.timestamp()}&after={after.timestamp()}"
    request = requests.get(requestxt)
    print(request)
    jsonbody = request.json()
    return jsonbody


@log_call
def isClipValid(input):
    """Validates that clips to be processed actually exist"""
    # time.sleep(.5)
    # print(input)
    filename = str(input) + "-" + str(label) + ".mp4"
    fullPath = output_path + filename
    try:
        vidinfo = ffmpeg.probe(
            "http://lenny:5000/api/events/" + str(input) + "/clip.mp4"
        )

    except ffmpeg.Error:
        print(f"{input} clip not found!")
        pass

    else:
        streamList.append(input)

    return streamList


@log_call
def cvt265(clip):
    time.sleep(1.5)
    filename = clip_path + str(clip) + ".mp4"
    validClip = "http://lenny:5000/api/events/" + str(clip) + "/clip.mp4"

    ffmpeg_command = [
        ffmpegbin,
        #       "ffmpeg",
        "-y",
        # "-f",
        # "concat",
        # "-safe",
        # "0",
        "-i",
        validClip,
        "-video_track_timescale",
        "10240",
        "-vf",
        "scale=800:600:flags=lanczos",
        "-c:v",
        "libx265",
        "-preset",
        "ultrafast",
        # "copy",
        "-x265-params",
        "pools=2",
        "-crf",
        "22",
        "-tag:v",
        "hvc1",
        # "-c",
        # "copy",
        filename,
    ]
    # print(ffmpeg_command)
    pipe = subprocess.run(
        ffmpeg_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    # print("done crunching")
    # print(pipe.stdout)
    # print(pipe.stderr)
    fileList.append(filename)
    # print(input)
    return fileList

    # print(ffmpeg_command)


@log_call
def concatvid():

    print(f"Concatenating {len(fileList)} clips together...")

    try:
        ffmpeg_command = [
            ffmpegbin,
            #           "ffmpeg",
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-fflags",
            "+igndts",
            "-i",
            "concat.txt",
            "-map",
            "0:0",
            # "-map",
            # "0:2",
            # "-x265-params",
            # "pools=1",
            # "-crf",
            # "22",
            # "-tag:v",
            # "hvc1",
            "-c",
            "copy",
            output_path
            + "/"
            + camera
            + "/"
            + zone
            + "/"
            + label
            + "/"
            + clipDate
            + "-"
            + camera
            + "-"
            + zone
            + "-"
            + label
            + ".mp4",
        ]
    except:
        pass

    # print(ffmpeg_command)
    pipe = subprocess.run(
        ffmpeg_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    print(pipe.stdout)
    print(pipe.stderr)


#    print(mp4out)


@log_call
def cleanup(delFiles):
    for x in delFiles:
        print(f"Removing {x}")
        os.remove(x)
    # os.remove("concat.txt")


def sendToInflux(msg, start, end):
    msg = "hey"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    params = {
        "db": "events",
        "precision": "ms",
    }

    data = 'events title="grabclips",text="flrugg",tags="blurgs"'

    response = requests.post(
        "http://lenny:8086/write", params=params, headers=headers, data=data
    )
    print(response.json())


@log_call
def sendtopushover(msg):
    requests.post(
        "https://api.pushover.net/1/messages.json",
        data={
            "token": "a2wa72bmvmchj2v6q9ymaz8ugm9ca4",
            "user": "ejZLT7WdcfryR1hLF9AhfjJqa8Ysu9",
            "sound": "pianobar",
            "message": " " + msg,
        },
    )


@log_call
def main():

    parser = argparse.ArgumentParser(description="Process some video.")
    parser.add_argument("clipdate", type=str)
    parser.add_argument("camera", type=str)
    parser.add_argument("zone", type=str)
    parser.add_argument("label", type=str)
    args = parser.parse_args()
    global clipDate
    clipDate = args.clipdate
    global label
    label = args.label
    global camera
    camera = args.camera
    global zone
    zone = args.zone

    t1 = datetime.datetime.now()
    clipInfo = fetchClipInfo(clipDate, camera, zone, label)
    # # Reverse the list so the earliest clips are at the start of the concatted video
    clipList = {event["id"] for event in clipInfo}
    clipList = sorted(clipList)
    startTime = time.time_ns()
    print(f"There are {len(clipList)} clips to be processed")

    # for x in clipList:
    #     isClipValid(x)

    listy = (isClipValid(x) for x in clipList)

    # print(len(listy))

    for b in tqdm(listy):
        if b == type(None):
            continue
    #      print(b['format']['filename'])

    # print(streamList)

    for x in tqdm(streamList):
        cvt265(x)

    with start_action(action_type="make_concat_file"):
        for element in fileList:
            textfile.write("file '" + element + "'\n")
        textfile.close()

    with start_action(action_type="make_big_file"):
        concatvid()

    with start_action(action_type="cleanup_files"):
        cleanup(fileList)
        endTime = time.time_ns()
        # sendToInflux("flurg",startTime,endTime)
        # print(fileList)
        t2 = datetime.datetime.now()
        sendtopushover(
            f"{clipDate} {camera} {zone} {label} re-encoding job is done. \n {len(fileList)} clips were processed. It took {t2 -t1} to complete."
        )


if __name__ == "__main__":
    main()

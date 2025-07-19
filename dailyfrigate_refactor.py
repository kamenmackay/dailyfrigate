from distutils.log import log
import requests
import subprocess
import ffmpeg
import os
import argparse
from eliot import start_action, to_file, log_call
from datetime import datetime


from signal import signal, SIGINT
from sys import exit
import datetime
import time
from tqdm import tqdm
import configparser
import structlog
import logging

# Parse the timestamp (2022-10-08) to unix time
# Scrape the api to download the requested clips
# Concatenate all the downloaded clips into one big file
# Re-encode the big clip to x265 format for space savings
to_file(open("dailyfrigate.log", "w+"))
config = configparser.ConfigParser()
config.read("dailyfrigate.cfg")
clip_path = config["General"]["clip_path"]
output_path = config["General"]["output_path"]
frigateurl = config["General"]["frigateurl"]
ffmpegbin = config["General"]["ffmpegbin"]


structlog.configure(
    processors=[
        # If log level is too low, abort pipeline and throw away log entry.
        structlog.stdlib.filter_by_level,
        # Add the name of the logger to event dict.
        structlog.stdlib.add_logger_name,
        # Add log level to event dict.
        structlog.stdlib.add_log_level,
        # Perform %-style formatting.
        structlog.stdlib.PositionalArgumentsFormatter(),
        # Add a timestamp in ISO 8601 format.
        structlog.processors.TimeStamper(fmt="iso"),
        # If the "stack_info" key in the event dict is true, remove it and
        # render the current stack trace in the "stack" key.
        structlog.processors.StackInfoRenderer(),
        # If the "exc_info" key in the event dict is either true or a
        # sys.exc_info() tuple, remove "exc_info" and render the exception
        # with traceback into the "exception" key.
        structlog.processors.format_exc_info,
        # If some value is in bytes, decode it to a unicode str.
        structlog.processors.UnicodeDecoder(),
        # Add callsite parameters.
        structlog.processors.CallsiteParameterAdder(
            {
                structlog.processors.CallsiteParameter.FILENAME,
                structlog.processors.CallsiteParameter.FUNC_NAME,
                structlog.processors.CallsiteParameter.LINENO,
            }
        ),
        # Render the final event dict as JSON.
        structlog.processors.JSONRenderer(),
    ],
    # `wrapper_class` is the bound logger that you get back from
    # get_logger(). This one imitates the API of `logging.Logger`.
    wrapper_class=structlog.stdlib.BoundLogger,
    # `logger_factory` is used to create wrapped loggers that are used for
    # OUTPUT. This one returns a `logging.Logger`. The final value (a JSON
    # string) from the final processor (`JSONRenderer`) will be passed to
    # the method of the same name as that you've called on the bound logger.
    logger_factory=structlog.stdlib.LoggerFactory(),
    # Effectively freeze configuration after creating the first bound
    # logger.
    cache_logger_on_first_use=True,
)

log = structlog.get_logger()
logging.basicConfig(
    filename="da.log",
    format="%(message)s",
    # stream=sys.stdout,
    level=logging.INFO,
)


@log_call
def handler(signal_received, frame):
    # Handle any cleanup here
    try:
        log.info("SIGINT or CTRL-C detected. Exiting gracefully")
        cleanup(fileList)
        exit(0)
    except:
        log.info("burp!")
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
    # after = cvt_to_epoch(startDate)
    print(before)
    # print(type(before))

    after = before - datetime.timedelta(days=1)
    requestxt = f"http://lenny:5000/api/events?has_clip=1&limit=500&label={label}&camera={camera}&zone={zone}&before={before.timestamp()}&after={after.timestamp()}"
    # requestxt = f"http://lenny:5000/api/events?has_clip=1&limit=99999&label={label}&camera={camera}&zone={zone}&before={before.timestamp()}&after={after.timestamp()}"
    request = requests.get(requestxt)
    # print(request.json())
    jsonbody = request.json()
    log.info(
        f"Fetching {len(jsonbody)} clips info for {startDate} for {camera} {zone} {label}",
        startDate=startDate,
    )
    return jsonbody


def getDuration(clip):
    pass


@log_call
def isClipValid(input):
    """Validates that clips to be processed actually exist"""
    # time.sleep(.5)
    # print(input)
    filename = str(input) + "-" + str(label) + ".mp4"
    output_path + filename
    try:
        ffmpeg.probe(
            "http://lenny:5000/api/events/" + str(input) + "/clip.mp4", rw_timeout=20000000
        )
        # print(vidinfo)
        streamList.append(input)
        log.info(f"Validated {input}")
        return streamList

    except ffmpeg.Error:
        log.warning(f"{input} clip not found!")
        pass

    else:
        pass


@log_call
def cvt265(clip):
    # time.sleep(1.5)
    filename = clip_path + str(clip) + ".mp4"
    validClip = "http://lenny:5000/api/events/" + str(clip) + "/clip.mp4"
    clipText = str(clipCount) + "/" + str(len(clipList))
    ffmpeg_command = [
        ffmpegbin,
        #       "ffmpeg",
        "-y",
        # "-f",
        # "concat",
        # "-safe",
        # "0",
        "-hwaccel",
        "auto",
        "-i",
        validClip,
        "-video_track_timescale",
        "10240",
#        "-vf",
#        "scale=800:600:flags=lanczos",
        "-vf",
        "drawtext=text='" + clipText + "':x=10:y=10:fontsize=48:fontcolor=white",
        "-c:v",
        "libx265",
        "-preset",
        "ultrafast",
        # "copy",
        "-x265-params",
        "pools=1",
        "-crf",
        "22",
        "-tag:v",
        "hvc1",
        # "-c",
        # "copy",
        filename,
    ]
    # print(ffmpeg_command)
    subprocess.run(ffmpeg_command,stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    # print("done crunching")
    # print(pipe.stdout)
    # print(pipe.stderr)
    fileList.append(filename)
    log.info(f"Downloaded {filename} to {clip_path}")
    # print(input)
    return fileList

    # print(ffmpeg_command)


@log_call
def cvth265(clip):
    # time.sleep(1.5)

    try:
        st = time.time()
        filename = clip_path + str(clip) + ".mp4"
        validClip = "http://lenny:5000/api/events/" + str(clip) + "/clip.mp4"
        clipText = str(clipCount) + "/" + str(len(blurgh))
        ffmpeg_command = [
            ffmpegbin,
            #       "ffmpeg",
            "-y",
            # "-f",
            # "concat",
            # "-safe",
            # "0",
            "-hwaccel",
            "auto",
            "-i",
            validClip,
            "-video_track_timescale",
            "10240",
            "-vf",
            "scale=800:600:flags=lanczos",
            "-vf",
            "drawtext=text='" + clipText + "':x=10:y=10:fontsize=48:fontcolor=white",
            "-c:v",
            "hevc_videotoolbox",
            # "-preset",
            # "ultrafast",
            "-b:v",
            "15000k",
            "-profile",
            "main10",
            "-tag:v",
            "hvc1",
            filename,
        ]
        # print(ffmpeg_command)
        subprocess.run(
            ffmpeg_command, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
        )
        et = time.time()
        log.info(f"Processed {filename}", clip=filename, duration=et - st)
        # print("done crunching")
        # print(pipe.stdout)
        # print(pipe.stderr)
        fileList.append(filename)
        # print(input)
        return fileList
    except:
        log.warning(f"Barfed on {filename}")
        exit(3)


@log_call
def concatvid():
    log.info(f"Concatenating {len(fileList)} clips together...")

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
        subprocess.run(ffmpeg_command)
        log.info(
            f"Concatenated {len(fileList)} clips together to {output_path}/{camera}/{zone}/{label}/{clipDate}/{camera}/{zone}/{label}.mp4"
        )
    except:
        print("some sort of error")

    # print(ffmpeg_command)
    # pipe = subprocess.run(
    #     ffmpeg_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    # )

    # print(pipe.stdout)
    # print(pipe.stderr)


#    print(mp4out)


@log_call
def cleanup(delFiles):
    for x in delFiles:
        try:
            print(f"Removing {x}")
            os.remove(x)
        except FileNotFoundError:
            continue
    # os.remove("concat.txt")


def sendToInflux(msg: str, start, end):
    # msg = "hey"
    # title,msg,tags=str,str,str
    title = "grabclips"
    tags = "bigbob,flurp"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    params = {
        "db": "events",
        "precision": "ms",
    }

    data2 = f'events title="{title}",text="{msg}",tags="{tags}",timeEnd={end}'

    requests.post(
        "http://lenny:8086/write", params=params, headers=headers, data=data2
    )
    # print(response.raw())


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


if __name__ == "__main__":
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
    cvtStart = int(time.time() * 1000000000)
    print(cvtStart)
    clipInfo = fetchClipInfo(clipDate, camera, zone, label)
    # # Reverse the list so the earliest clips are at the start of the concatted video
    global clipList
    clipList = [event["id"] for event in clipInfo if event["has_clip"] is True]
    clipList = sorted(list(clipList))
    startTime = time.time_ns()

    # for x in clipList:
    #     isClipValid(x)
    global clipCount

    # clipDuration = { }
    if clipList:
        print(f"There are {len(clipList)} clips to be processed")
        print(f"Validating {len(clipList)} clips")
        for clip in tqdm(clipList, desc="Validating"):
            # Validate the clips first
            # clipCount += 1
            blurgh = isClipValid(clip)
            # cvt265(clip)

#        log.info(f"Processing {len(blurgh)} clips")

        clipCount = 0
        for vClip in tqdm(blurgh):
            # Iterate through the validated clips
            clipCount += 1
            cvt265(vClip)

        with start_action(action_type="make_concat_file"):
            for element in fileList:
                textfile.write("file '" + element + "'\n")
            textfile.close()

        with start_action(action_type="make_big_file"):
            concatvid()
            # cleanup(fileList)

        t2 = datetime.datetime.now()

        # cvtEnd = int(time.time() * 1000000000)
        sendtopushover(
            f"{clipDate} {camera} {zone} {label} re-encoding job is done. \n {len(fileList)} clips were processed. It took {t2 -t1} to complete."
        )

    # msgString = f"{camera} {zone} {label} re-encoding job is done. \n {len(fileList)} clips were processed. It took {t2 -t1} to complete."
    # sendToInflux(msgString,cvtStart,cvtEnd)
    else:
        log.critical(f"No clips to process for {clipDate} for {camera} {zone} {label}")

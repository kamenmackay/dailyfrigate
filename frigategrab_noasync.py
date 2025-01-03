import dailyfrigate_refactor
import ffmpeg

num_pools = 5
eventUrl = "http://lenny:5000/api/events/"
clipList = dailyfrigate_refactor.fetchClipInfo(
    "2024-06-24", "backgarden", "grassyarea", "person"
)
idList = sorted({eventUrl + x["id"] + "/clip.mp4" for x in clipList})
# print(sorted(idList))

print(f"{len(idList)} items")

idListLen = len(idList)


def get_clip_duration(url):
    try:

        probe = ffmpeg.probe(
            url,
        )
        duration = float(probe["streams"][0]["duration"])
        print(duration)
        return duration
    except ffmpeg._run.Error as e:
        # Handle the error gracefully
        print(f"Caught ffmpeg error: {e}")
        return None


# ffInput = [ ffmpeg.input(x).drawtext(text=f"blurgh",x=10,y=10,fontsize=48,fontcolor='white') for x in idList ]

clipCount = 0
validclips = []
for clip in idList:
    print(f"Checking duration of {clip}")

    if get_clip_duration(clip) is None:
        break
    if get_clip_duration(clip) < 60:
        try:
            clipCount += 1

        except AttributeError:
            print("Skipping due to bad attribute")
            pass
        except Exception:
            print("Exception!")
            pass
        else:
            numberedclip = ffmpeg.input(clip).drawtext(
                text=f"{clipCount}/{idListLen}",
                x=10,
                y=10,
                fontsize=48,
                fontcolor="white",
            )
            print(f"Appending clip {clipCount} to valid clips")
            validclips.append(numberedclip)
    else:
        print(f"Skipping clip {clipCount}")
        pass


# print(validclips)


print("Joining")
joined = ffmpeg.concat(*validclips, unsafe=True)


num_pools = 10
# print(type(joined))
print("Writing to final destination")
deduped = joined.filter("mpdecimate")
out = ffmpeg.output(
    deduped,
    "test.mp4",
    vcodec="libx265",
    # preset="ultrafast",
    **{"x265-params": f"pools={num_pools}", "preset": "ultrafast", "tag": "hvc1"},
).global_args("-fflags", "+igndts", "-vsync", "2")

try:
    ffmpeg.run(out)
except ffmpeg.Error as e:
    print(e.stderr)

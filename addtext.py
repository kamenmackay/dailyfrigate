import ffmpeg

def add_text_to_video(input_video_path, output_video_path, text):
    # Set up the drawtext filter
    drawtext = "text=" + text + ""
    txt = "blrugh"
    # Open the input video file
    input_video = ffmpeg.input(input_video_path)

    # Add the drawtext filter to the video
    video_with_text = input_video.filter('drawtext', f"text={str(txt)}:fontsize=48:fontcolor=white")

    # Save the output video file
    output_video = ffmpeg.output(video_with_text, output_video_path)

    # Run the ffmpeg command
    ffmpeg.run(output_video)

    # Read the output video file into a bytes object
    with open(output_video_path, 'rb') as f:
        video_bytes = f.read()

    return video_bytes


add_text_to_video('/Users/kmackay/nobackup/housevids/tmp/1679662074.003437-g4sda6.mp4','test.mp4', "hi!")
from dataclasses import dataclass
from datetime import datetime, timedelta
import requests
import os
import argparse
from typing import List, Optional, Tuple
import logging
import tempfile
import platform
import ffmpeg
from tqdm import tqdm

@dataclass
class FrigateClip:
    """Represents a single clip from Frigate"""
    id: str
    camera: str
    label: str
    zone: Optional[str]
    score: float
    start_time: float
    end_time: float
    box: Optional[Tuple[float, float, float, float]] = None
    download_path: Optional[str] = None
    snapshot_path: Optional[str] = None
    pip_path: Optional[str] = None
    
    @property
    def duration(self) -> float:
        return self.end_time - self.start_time
    
    @property
    def formatted_score(self) -> str:
        return f"{int(self.score * 100)}%"

def setup_logging(debug: bool = False):
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def get_events_for_date(base_url: str, date: str, camera: str, 
                       zone: Optional[str] = None, label: Optional[str] = None) -> List[FrigateClip]:
    start_date = datetime.strptime(date, "%Y-%m-%d")
    end_date = start_date + timedelta(days=1)
    params = {
        'before': int(end_date.timestamp()),
        'after': int(start_date.timestamp()),
        'cameras': camera,
        'has_clip': 1
    }
    
    if zone:
        params['zones'] = zone
    if label:
        params['labels'] = label

    response = requests.get(f"{base_url}/api/events", params=params)
    response.raise_for_status()
    
    events = response.json()
    clips = sorted([
        FrigateClip(
            id=event['id'],
            camera=event['camera'],
            label=event.get('label', ''),
            zone=event.get('zones', [None])[0],
            score=event.get('score', 0.0),
            start_time=event['start_time'],
            end_time=event.get('end_time', event['start_time']),
            box=event.get('box')
        ) for event in events
    ], key=lambda x: x.start_time)
    return clips

def process_clip(base_url: str, clip: FrigateClip, temp_dir: str, is_macos: bool):
    # Download clip
    clip_path = os.path.join(temp_dir, f"clip_{clip.id}.mp4")
    response = requests.get(f"{base_url}/api/events/{clip.id}/clip.mp4", stream=True)
    response.raise_for_status()
    with open(clip_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    clip.download_path = clip_path

    # Download thumbnail
    snapshot_path = os.path.join(temp_dir, f"snapshot_{clip.id}.jpg")
    response = requests.get(
        f"{base_url}/api/events/{clip.id}/snapshot.jpg",
        params={'bbox': 1, 'quality': 95}
    )
    response.raise_for_status()
    
    with open(snapshot_path, 'wb') as f:
        f.write(response.content)
    clip.snapshot_path = snapshot_path

    # Get video dimensions
    probe = ffmpeg.probe(clip_path)
    video_info = next(s for s in probe['streams'] if s['codec_type'] == 'video')
    main_width = int(video_info['width'])
    
    # Get snapshot dimensions
    snapshot_probe = ffmpeg.probe(snapshot_path)
    snapshot_info = next(s for s in snapshot_probe['streams'] if s['codec_type'] == 'video')
    snapshot_height = int(snapshot_info['height'])
    
    pip_width = main_width // 8
    x_position = main_width - pip_width - 10
    y_position = 10

    # Create PiP version with overlay
    pip_path = os.path.join(temp_dir, f"pip_{clip.id}.mp4")
    
    main = ffmpeg.input(clip_path)
    overlay = (
        ffmpeg.input(snapshot_path)
        .filter('scale', pip_width, -1)
        .filter('drawtext',
                text=f"{clip.label}: {clip.formatted_score}",
                fontcolor='white',
                fontsize=f'{pip_width//12}',
                x=f'{x_position}',
                y=f'{y_position+pip_width}',
                shadowcolor='black',
                shadowx=2,
                shadowy=2)
    )
    
    video = ffmpeg.overlay(main, overlay, x=x_position, y=y_position)
    
    if is_macos:
        stream = (
            video
            .output(pip_path, acodec='copy', vcodec='hevc_videotoolbox',
                   video_bitrate='5000k', tag='hvc1', r=20)
        )
    else:
        stream = (
            video
            .output(pip_path, acodec='copy', vcodec='libx265',
                   crf=23, preset='medium', tag='hvc1', r=20)
        )

    stream.overwrite_output().run(capture_stdout=True, capture_stderr=True)
    clip.pip_path = pip_path

def archive_events(base_url: str, date: str, camera: str, output_dir: str = "archives",
                  zone: Optional[str] = None, label: Optional[str] = None, 
                  debug: bool = False):
    logger = setup_logging(debug)
    os.makedirs(output_dir, exist_ok=True)
    is_macos = platform.system() == 'Darwin'
    
    # Create output filename
    components = [date, camera]
    if zone:
        components.append(zone)
    if label:
        components.append(label)
    
    output_filename = '-'.join(components) + '.mp4'
    output_path = os.path.join(output_dir, output_filename)
    
    if os.path.exists(output_path):
        logger.warning(f"Archive {output_filename} already exists. Skipping.")
        return

    clips = get_events_for_date(base_url, date, camera, zone, label)
    if not clips:
        logger.info(f"No events found for the specified criteria on {date}")
        return
    
    logger.info(f"Found {len(clips)} events matching criteria")
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            logger.info(f"Processing {len(clips)} clips...")
            
            if debug:
                total_duration = sum(clip.duration for clip in clips)
                logger.debug(f"Total duration: {total_duration:.2f} seconds")
            
            file_list_path = os.path.join(temp_dir, "files.txt")
            
            for clip in tqdm(clips, desc="Processing clips", unit="clip"):
                process_clip(base_url, clip, temp_dir, is_macos)
            
            # Create concat file
            with open(file_list_path, 'w') as f:
                for clip in clips:
                    f.write(f"file '{clip.pip_path}'\n")
            
            logger.info(f"Creating final video...")
            
            # Final concatenation with x265
            (
                ffmpeg
                .input(file_list_path, format='concat', safe=0)
                .output(output_path, vcodec='libx265', preset='medium',
                       crf=23, tag='hvc1', r=20, acodec='aac')
                .overwrite_output()
                .run(capture_stdout=True, capture_stderr=True)
            )
            
        logger.info(f"Successfully archived events to {output_filename}")
        
    except ffmpeg.Error as e:
        logger.error(f"Error during ffmpeg processing: {e.stderr.decode()}")
        if os.path.exists(output_path):
            os.remove(output_path)
    except Exception as e:
        logger.error(f"Error during archiving: {str(e)}")
        if os.path.exists(output_path):
            os.remove(output_path)

def main():
    parser = argparse.ArgumentParser(description='Archive Frigate events')
    parser.add_argument('--base-url', required=True, help='Base URL of Frigate instance')
    parser.add_argument('--date', required=True, help='Date to archive (YYYY-MM-DD)')
    parser.add_argument('--camera', required=True, help='Camera name')
    parser.add_argument('--zone', help='Zone name (optional)')
    parser.add_argument('--label', help='Label to filter by (optional)')
    parser.add_argument('--output-dir', default='archives', help='Output directory')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    archive_events(
        args.base_url, 
        args.date, 
        args.camera,
        args.output_dir,
        args.zone,
        args.label,
        args.debug
    )

if __name__ == "__main__":
    main()
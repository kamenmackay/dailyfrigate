from dataclasses import dataclass
from datetime import datetime, timedelta
import requests
import os
import argparse
from typing import List, Optional, Tuple
import logging
import tempfile
import json
import platform
from PIL import Image
from io import BytesIO
import ffmpeg

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
    
    @classmethod
    def from_event(cls, event: dict) -> 'FrigateClip':
        return cls(
            id=event['id'],
            camera=event['camera'],
            label=event.get('label', ''),
            zone=event.get('zones', [None])[0],
            score=event.get('score', 0.0),
            start_time=event['start_time'],
            end_time=event.get('end_time', event['start_time']),
            box=event.get('box'),
        )

class FrigateArchiver:
    def __init__(self, base_url: str, output_dir: str = "archives", debug: bool = False):
        self.base_url = base_url.rstrip('/')
        self.output_dir = output_dir
        self.debug = debug
        self.session = requests.Session()
        self.is_macos = platform.system() == 'Darwin'
        
        os.makedirs(output_dir, exist_ok=True)
        
        logging.basicConfig(
            level=logging.DEBUG if debug else logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def get_events_for_date(self, date: str, camera: str, zone: Optional[str] = None, 
                          label: Optional[str] = None) -> List[FrigateClip]:
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

        response = requests.get(f"{self.base_url}/api/events", params=params)
        response.raise_for_status()
        
        events = response.json()
        clips = [FrigateClip.from_event(event) for event in events]
        clips.sort(key=lambda x: x.start_time)
        return clips

    def crop_snapshot(self, snapshot_data: bytes, clip: FrigateClip) -> bytes:
        if not clip.box:
            return snapshot_data

        img = Image.open(BytesIO(snapshot_data))
        width, height = img.size

        x, y, w, h = clip.box
        x1 = int(x * width)
        y1 = int(y * height)
        x2 = int((x + w) * width)
        y2 = int((y + h) * height)

        pad_x = int((x2 - x1) * 0.1)
        pad_y = int((y2 - y1) * 0.1)

        x1 = max(0, x1 - pad_x)
        y1 = max(0, y1 - pad_y)
        x2 = min(width, x2 + pad_x)
        y2 = min(height, y2 + pad_y)

        cropped = img.crop((x1, y1, x2, y2))

        output = BytesIO()
        cropped.save(output, format='JPEG', quality=95)
        return output.getvalue()

    def process_clip(self, clip: FrigateClip, temp_dir: str):
        # Download clip
        clip_path = os.path.join(temp_dir, f"clip_{clip.id}.mp4")
        response = requests.get(f"{self.base_url}/api/events/{clip.id}/clip.mp4", stream=True)
        response.raise_for_status()
        with open(clip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        clip.download_path = clip_path

        # Download thumbnail
        snapshot_path = os.path.join(temp_dir, f"snapshot_{clip.id}.jpg")
        response = requests.get(
            f"{self.base_url}/api/events/{clip.id}/thumbnail.jpg",
            params={'bbox': 1, 'quality': 95}
        )
        response.raise_for_status()
        
        cropped_data = self.crop_snapshot(response.content, clip)
        with open(snapshot_path, 'wb') as f:
            f.write(cropped_data)
        clip.snapshot_path = snapshot_path

        # Get video info
        probe = ffmpeg.probe(clip_path)
        video_info = next(s for s in probe['streams'] if s['codec_type'] == 'video')
        main_width = int(video_info['width'])
        main_height = int(video_info['height'])
        
        pip_width = main_width // 4
        pip_height = main_height // 4
        x_position = main_width - pip_width - 10
        y_position = 10

        # Create PiP version with overlay
        pip_path = os.path.join(temp_dir, f"pip_{clip.id}.mp4")
        
        main = ffmpeg.input(clip_path)
        overlay = (
            ffmpeg.input(snapshot_path)
            .filter('scale', pip_width, pip_height, force_original_aspect_ratio='decrease')
            .filter('pad', pip_width, pip_height, -1, -1, color='white')
            # .filter('drawtext', text=f"{clip.label}: {clip.formatted_score}",
            #        fontcolor='white', fontsize=14,
            #        x='(w-text_w)/2', y='h-th-5')
        )
        
        video = ffmpeg.overlay(main, overlay, x=x_position, y=y_position)
        
        if self.is_macos:
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

    def archive_events(self, date: str, camera: str, zone: Optional[str] = None, 
                      label: Optional[str] = None):
        components = [date, camera]
        if zone:
            components.append(zone)
        if label:
            components.append(label)
        
        output_filename = '-'.join(components) + '.mp4'
        output_path = os.path.join(self.output_dir, output_filename)
        
        if os.path.exists(output_path):
            self.logger.warning(f"Archive {output_filename} already exists. Skipping.")
            return

        clips = self.get_events_for_date(date, camera, zone, label)
        if not clips:
            self.logger.info(f"No events found for the specified criteria on {date}")
            return
        
        self.logger.info(f"Found {len(clips)} events matching criteria")
        
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                self.logger.info(f"Processing {len(clips)} clips...")
                
                if self.debug:
                    total_duration = sum(clip.duration for clip in clips)
                    self.logger.debug(f"Total duration: {total_duration:.2f} seconds")
                
                file_list_path = os.path.join(temp_dir, "files.txt")
                
                for i, clip in enumerate(clips, 1):
                    self.process_clip(clip, temp_dir)
                    if i % 5 == 0:
                        self.logger.info(f"Processed {i}/{len(clips)} clips...")
                
                # Create concat file
                with open(file_list_path, 'w') as f:
                    for clip in clips:
                        f.write(f"file '{clip.pip_path}'\n")
                
                self.logger.info(f"Creating final video...")
                
                # Final concatenation with x265
                (
                    ffmpeg
                    .input(file_list_path, format='concat', safe=0)
                    .output(output_path, vcodec='libx265', preset='medium',
                           crf=23, tag='hvc1', r=20, acodec='aac')
                    .overwrite_output()
                    .run(capture_stdout=True, capture_stderr=True)
                )
                
            self.logger.info(f"Successfully archived events to {output_filename}")
            
        except ffmpeg.Error as e:
            self.logger.error(f"Error during ffmpeg processing: {e.stderr.decode()}")
            if os.path.exists(output_path):
                os.remove(output_path)
        except Exception as e:
            self.logger.error(f"Error during archiving: {str(e)}")
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
    
    archiver = FrigateArchiver(args.base_url, args.output_dir, args.debug)
    archiver.archive_events(args.date, args.camera, args.zone, args.label)

if __name__ == "__main__":
    main()
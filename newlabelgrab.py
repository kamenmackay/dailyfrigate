#!/usr/bin/env python3
# -*- python-script -*-
# type: script
"""
dependencies:
  - dataclasses
  - datetime
  - requests
  - argparse
  - typing
  - pillow
  - io
"""

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
import time
import subprocess

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
    duration_sec: Optional[float] = None
    
    @property
    def duration(self) -> float:
        return self.end_time - self.start_time
    
    @property
    def formatted_score(self) -> str:
        return f"{int(self.score * 100)}%"
    
    @property
    def formatted_datetime(self) -> str:
        """Return a formatted datetime string for chapter titles"""
        dt = datetime.fromtimestamp(self.start_time)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    
    @property
    def chapter_title(self) -> str:
        """Generate a title for the chapter"""
        return f"{self.label} ({self.formatted_score}) - {self.formatted_datetime}"
    
    @classmethod
    def from_event(cls, event: dict) -> 'FrigateClip':
        return cls(
            id=event['id'],
            camera=event.get('camera', ''),
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
            'has_clip': 1,
            'limit': -1
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

    def process_clip(self, clip: FrigateClip, temp_dir: str):
        # Download clip
        try:
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
                f"{self.base_url}/api/events/{clip.id}/snapshot.jpg",
                params={'bbox': 1, 'quality': 95}
            )
            response.raise_for_status()
            
            with open(snapshot_path, 'wb') as f:
                f.write(response.content)
            clip.snapshot_path = snapshot_path

            # Get video info
            probe = ffmpeg.probe(clip_path)
            video_info = next(s for s in probe['streams'] if s['codec_type'] == 'video')
            main_width = int(video_info['width'])
            main_height = int(video_info['height'])
            
            # Store actual duration from the clip
            clip.duration_sec = float(video_info.get('duration', clip.duration))
            
            # Get snapshot dimensions
            snapshot_probe = ffmpeg.probe(snapshot_path)
            snapshot_info = next(s for s in snapshot_probe['streams'] if s['codec_type'] == 'video')
            snapshot_height = int(snapshot_info['height'])
            
            pip_width = main_width // 8  # Made smaller (1/8 of main video)
            x_position = main_width - pip_width - 10
            y_position = 10

            # Calculate text padding
            text_pad = snapshot_height // 4  # Space for text below image

            # Create PiP version with overlay
            pip_path = os.path.join(temp_dir, f"pip_{clip.id}.mp4")
            
            main = ffmpeg.input(clip_path)
            overlay = (
                ffmpeg.input(snapshot_path)
                .filter('scale', pip_width, -1)  # Scale width, keep aspect ratio
                .filter('drawtext',
                        text=f"{clip.label}: {clip.formatted_score}",
                        fontcolor='white',
                        fontsize=f'{pip_width//12}',
                        x=f'{x_position}',
                        y=f'{y_position+pip_width}',  # Position text directly below thumbnail
                        shadowcolor='black',
                        shadowx=2,
                        shadowy=2)
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
        except Exception as e:
            self.logger.warning(f"Failed to process clip {clip.id}: {str(e)}")
            # Make sure attributes are set to None in case of failure
            clip.download_path = None
            clip.snapshot_path = None
            clip.pip_path = None

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
                chapters_file_path = os.path.join(temp_dir, "chapters.txt")
                
                for i, clip in enumerate(clips, 1):
                    try:
                        self.process_clip(clip, temp_dir)
                    except Exception as e:
                        self.logger.error(f"Failed to process clip {i}: {str(e)}")
                    if i % 5 == 0:
                        self.logger.info(f"Processed {i}/{len(clips)} clips...")
                
                # Count successfully processed clips
                successful_clips = [clip for clip in clips if clip.pip_path is not None]
                if not successful_clips:
                    self.logger.error("No clips were successfully processed. Aborting.")
                    return
                    
                self.logger.info(f"Successfully processed {len(successful_clips)}/{len(clips)} clips")
                
                # Create concat file
                with open(file_list_path, 'w') as f:
                    for clip in clips:
                        if clip.pip_path:  # Only include clips with a valid pip_path
                            f.write(f"file '{clip.pip_path}'\n")
                        else:
                            self.logger.warning(f"Skipping clip {clip.id} in concat as it has no pip_path")
                
                # Create chapters file in ffmpeg metadata format
                # Format: [CHAPTER]\nTIMEBASE=1/1000\nSTART=starttime_ms\nEND=endtime_ms\ntitle=Chapter Title
                current_time_ms = 0

                with open(chapters_file_path, 'w') as f:
                    for i, clip in enumerate(clips):
                        # Skip clips that failed processing
                        if not clip.pip_path or not clip.duration_sec:
                            continue
                        
                        start_time_ms = current_time_ms
                        duration_ms = int(clip.duration_sec * 1000)
                        end_time_ms = start_time_ms + duration_ms
                        
                        # For chapter index, start at 0 and ensure it's formatted properly
                        f.write(f"[CHAPTER]\n")
                        f.write(f"TIMEBASE=1/1000\n")
                        f.write(f"START={start_time_ms}\n")
                        f.write(f"END={end_time_ms}\n")
                        f.write(f"title={clip.chapter_title}\n\n")
                        
                        # Update current time for next clip
                        current_time_ms = end_time_ms
                
                self.logger.info(f"Creating final video with {len(successful_clips)} chapters...")
                
                # Final concatenation with x265 and chapters metadata
                metadata_file = os.path.join(temp_dir, "metadata.txt")
                with open(metadata_file, 'w') as f:
                    f.write(f";FFMETADATA1\n")
                    
                    # Add global metadata if needed
                    f.write(f"title=Frigate Archive {date} - {camera}\n")
                    if zone:
                        f.write(f"comment=Zone: {zone}\n")
                    f.write("\n")  # Add a blank line for better separation
                    
                    # Read and append chapters
                    with open(chapters_file_path) as chapters:
                        f.write(chapters.read())
                
                # Concatenate videos first into a temporary file
                temp_concat = os.path.join(temp_dir, "temp_concat.mp4")
                try:
                    (
                        ffmpeg
                        .input(file_list_path, format='concat', safe=0)
                        .output(temp_concat, vcodec='copy', acodec='copy')
                        .overwrite_output()
                        .run(capture_stdout=True, capture_stderr=True)
                    )
                except ffmpeg.Error as e:
                    self.logger.error(f"Error during concat: {e.stderr.decode() if hasattr(e, 'stderr') else str(e)}")
                    raise
                
                # Then add chapters metadata to the final output using the alternative method
                # Use a direct ffmpeg command as python-ffmpeg doesn't support metadata input well
                if self.is_macos:
                    ffmpeg_cmd = [
                        'ffmpeg',
                        '-i', temp_concat,
                        '-i', metadata_file,
                        '-map_metadata', '1',
                        '-codec', 'copy',
                        '-tag:v', 'hvc1',
                        '-y',
                        output_path
                    ]
                else:
                    ffmpeg_cmd = [
                        'ffmpeg',
                        '-i', temp_concat,
                        '-i', metadata_file,
                        '-map_metadata', '1',
                        '-codec', 'copy',
                        '-tag:v', 'hvc1',
                        '-y',
                        output_path
                    ]
                
                # Execute the command
                try:
                    subprocess.run(ffmpeg_cmd, check=True, capture_output=True)
                except subprocess.CalledProcessError as e:
                    self.logger.error(f"Error during final processing: {e.stderr.decode() if e.stderr else str(e)}")
                    raise ffmpeg.Error(e.stderr if e.stderr else str(e))
                
            self.logger.info(f"Successfully archived events with chapters to {output_filename}")
            
        except ffmpeg.Error as e:
            error_msg = e.stderr.decode() if hasattr(e, 'stderr') else str(e)
            self.logger.error(f"Error during ffmpeg processing: {error_msg}")
            if os.path.exists(output_path):
                os.remove(output_path)
        except Exception as e:
            self.logger.error(f"Error during archiving: {str(e)}")
            if os.path.exists(output_path):
                os.remove(output_path)

def main():
    parser = argparse.ArgumentParser(description='Archive Frigate events with chapters')
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
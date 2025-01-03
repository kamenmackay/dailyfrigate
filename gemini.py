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
    zone: Optional[str] = None
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
            f"{self.base_url}/api/events/{clip.
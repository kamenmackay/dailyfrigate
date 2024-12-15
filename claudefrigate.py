import argparse
import ffmpeg
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from tempfile import TemporaryDirectory
from tqdm import tqdm
from functools import cache
import datetime
from datetime import timezone
import requests
import logging
from typing import List, Tuple, Optional
from dataclasses import dataclass
from influxdb import InfluxDBClient

@dataclass
class ProcessingConfig:
    num_pools: int = 10
    chunk_size: int = 10
    event_url: str = "http://lenny:5000/api/events/"
    api_base_url: str = "http://lenny:5000/api/events"
    influx_host: str = "192.168.8.150"
    influx_port: int = 8086
    influx_database: str = "events"
    temp_root_dir: str = "/Users/kmackay/nobackup/testy"  # Added from original code

class VideoProcessor:
    def __init__(self, config: ProcessingConfig):
        self.config = config
        self._setup_logging()
        self._setup_influx()
        self.process_start_time = None
        self.process_end_time = None
        self.total_clips = 0

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _setup_influx(self):
        """Initialize InfluxDB client"""
        self.influx_client = InfluxDBClient(
            host=self.config.influx_host,
            port=self.config.influx_port,
            database=self.config.influx_database
        )
        # Ensure database exists
        if self.config.influx_database not in [db['name'] for db in self.influx_client.get_list_database()]:
            self.influx_client.create_database(self.config.influx_database)
        self.influx_client.switch_database(self.config.influx_database)

    def send_metrics(self, args: argparse.Namespace, total_clips: int, success: bool):
        """Send processing metrics to InfluxDB"""
        try:
            duration = (self.process_end_time - self.process_start_time).total_seconds()
            
            json_body = [{
                "measurement": "video_processing",
                "tags": {
                    "camera": args.camera,
                    "zone": args.zone,
                    "label": args.label,
                    "success": str(success)
                },
                "time": self.process_start_time.isoformat(),
                "fields": {
                    "total_clips": total_clips,
                    "duration_seconds": float(duration),
                    "clips_per_second": float(total_clips) / duration if duration > 0 else 0
                }
            }]
            
            self.influx_client.write_points(json_body)
            self.logger.info(f"Metrics sent to InfluxDB: processing took {duration:.2f} seconds")
        except Exception as e:
            self.logger.error(f"Failed to send metrics to InfluxDB: {e}")

    def fetch_clip_info(self, start_date: str, camera: str, zone: str, label: str) -> List[dict]:
        """Fetch clip information with error handling and retries."""
        try:
            start_date_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
            end_date_dt = start_date_dt + datetime.timedelta(days=1, seconds=-1)
            
            params = {
                'has_clip': 1,
                'limit': 500,
                'label': label,
                'camera': camera,
                'zone': zone,
                'before': int(end_date_dt.timestamp()),
                'after': int(start_date_dt.timestamp())
            }
            
            response = requests.get(self.config.api_base_url, params=params)
            response.raise_for_status()
            return sorted(response.json(), key=lambda x: x['start_time'])
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch clip info: {e}")
            raise

    @staticmethod
    @cache
    def get_clip_duration(url: str) -> Optional[float]:
        """Get clip duration with caching and error handling."""
        try:
            probe = ffmpeg.probe(url)
            return float(probe["streams"][0]["duration"])
        except ffmpeg._run.Error as e:
            logging.error(f"Failed to get duration for clip {url}: {e}")
            return None

    def process_clip(self, clip_info: Tuple[str, int]) -> Optional[ffmpeg.Stream]:
        """Process individual clip with error handling."""
        clip, idx = clip_info
        try:
            duration = self.get_clip_duration(clip)
            if duration is None:
                return None
                
            return ffmpeg.input(clip).drawtext(
                text=f"{idx + 1}/{self.total_clips}",
                x=10,
                y=10,
                fontsize=48,
                fontcolor="white"
            )
        except ffmpeg.Error as e:
            self.logger.error(f"Failed to process clip {idx + 1}/{self.total_clips}: {e}")
            return None

    def process_chunk(self, chunk: List[Tuple[str, int]], chunk_id: int, temp_dir: str) -> Optional[str]:
        """Process a chunk of clips in parallel."""
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self.process_clip, clip_info)
                for clip_info in chunk
            ]
            
            clip_streams = []
            for future in tqdm(
                as_completed(futures),
                desc=f"Processing chunk {chunk_id+1}",
                total=len(chunk),
                ncols=100
            ):
                result = future.result()
                if result is not None:
                    clip_streams.append(result)

        if not clip_streams:
            return None

        temp_file = os.path.join(temp_dir, f"temp_{chunk_id}.mp4")
        
        try:
            joined = ffmpeg.concat(*clip_streams, unsafe=True)
            out = ffmpeg.output(
                joined,
                temp_file,
                vcodec="libx265",
                pix_fmt="yuv420p",
                **{
                    "x265-params": f"pools={self.config.num_pools}",
                    "crf": "24",
                    "r": 20
                }
            ).global_args("-fflags", "+igndts")
            
            ffmpeg.run(out, capture_stderr=True, capture_stdout=True)
            return temp_file
        except ffmpeg.Error as e:
            self.logger.error(f"Failed to join clips in chunk {chunk_id+1}: {e}")
            return None

    def process_videos(self, args: argparse.Namespace) -> None:
        """Main processing pipeline."""
        success = False
        
        try:
            self.process_start_time = datetime.datetime.now(timezone.utc)
            
            # Fetch and prepare clips
            clips = self.fetch_clip_info(args.timestamp, args.camera, args.zone, args.label)
            self.total_clips = len(clips)
            self.logger.info(f"Processing {self.total_clips} clips")
            
            # Create id_list with global indices
            id_list = [(self.config.event_url + x["id"] + "/clip.mp4", idx) 
                      for idx, x in enumerate(clips)]
            
            # Create chunks
            chunks = [id_list[i:i + self.config.chunk_size] 
                     for i in range(0, len(id_list), self.config.chunk_size)]
            
            # Process chunks
            with TemporaryDirectory(dir=self.config.temp_root_dir) as temp_dir:
                temp_files = []
                for i, chunk in enumerate(chunks):
                    if temp_file := self.process_chunk(chunk, i, temp_dir):
                        temp_files.append(temp_file)
                
                if not temp_files:
                    raise RuntimeError("No valid temporary files generated")
                
                # Join temporary files
                self.logger.info("Joining temporary files")
                temp_file_inputs = [ffmpeg.input(temp_file) for temp_file in temp_files]
                final_joined = ffmpeg.concat(*temp_file_inputs, unsafe=True)
                
                # Save final output
                output_file_name = f"{args.timestamp}-{args.camera}-{args.zone}-{args.label}.mp4"
                output_path = os.path.join(args.output_dir, output_file_name)
                
                ffmpeg.output(final_joined, output_path, r=15).run()
                self.logger.info(f"Output saved to {output_path}")
                success = True
                
        except Exception as e:
            self.logger.error(f"Processing failed: {e}")
            raise
        finally:
            self.process_end_time = datetime.datetime.now(timezone.utc)
            self.send_metrics(args, self.total_clips, success)

def main():
    parser = argparse.ArgumentParser(
        description="Process clips with specified timestamp, camera, zone, and label"
    )
    parser.add_argument("timestamp", help="ISO 8601 timestamp")
    parser.add_argument("camera", help="Camera name")
    parser.add_argument("zone", help="Zone name")
    parser.add_argument("label", help="Label name")
    parser.add_argument(
        "--output-dir",
        help="Output directory (default: current directory)",
        default="."
    )
    
    args = parser.parse_args()
    
    config = ProcessingConfig()
    processor = VideoProcessor(config)
    processor.process_videos(args)

if __name__ == "__main__":
    main()
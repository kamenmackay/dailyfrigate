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

# OpenTelemetry imports
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, SERVICE_VERSION, DEPLOYMENT_ENVIRONMENT, Resource
import socket
import warnings
import urllib3.exceptions
import signal
import sys
import shutil
import atexit

# JSON formatter for structured logging
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "function": record.funcName,
            "line": record.lineno,
            "message": record.getMessage(),
            "module": record.module,
            "pathname": record.pathname,
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
            
        # Add extra fields if present
        if hasattr(record, 'extra_fields'):
            log_entry.update(record.extra_fields)
            
        return json.dumps(log_entry)

# Global logger setup
logger = logging.getLogger(__name__)

def setup_tracing(enable_tracing: bool = False):
    """Set up OpenTelemetry tracing if enabled"""
    if not enable_tracing:
        # Use NoOpTracer when tracing is disabled
        from opentelemetry.trace import NoOpTracer
        return NoOpTracer()
    
    # Set up OpenTelemetry tracing
    resource = Resource(attributes={
        SERVICE_NAME: "frigate-archiver",
        SERVICE_VERSION: "1.0.0",
        DEPLOYMENT_ENVIRONMENT: os.getenv("ENVIRONMENT", "development"),
        "host.name": socket.gethostname(),
        "service.instance.id": f"{socket.gethostname()}-{os.getpid()}",
    })

    trace.set_tracer_provider(TracerProvider(resource=resource))
    tracer = trace.get_tracer(__name__)

    # Configure exporters based on environment variables
    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318/v1/traces")
    enable_console_export = os.getenv("OTEL_CONSOLE_EXPORT", "false").lower() == "true"

    # Add OTLP exporter for Grafana/Tempo (using HTTP instead of gRPC)
    if otlp_endpoint:
        try:
            # Suppress urllib3 connection warnings
            warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)
            
            otlp_exporter = OTLPSpanExporter(
                endpoint=otlp_endpoint,
                timeout=5  # 5 second timeout
            )
            otlp_processor = BatchSpanProcessor(otlp_exporter)
            trace.get_tracer_provider().add_span_processor(otlp_processor)
            print(f"OpenTelemetry: Sending traces to {otlp_endpoint}")
        except Exception as e:
            # Silently continue without OTLP export - don't spam logs
            pass

    # Add console exporter for debugging (optional)
    if enable_console_export:
        try:
            console_exporter = ConsoleSpanExporter()
            console_processor = BatchSpanProcessor(console_exporter)
            trace.get_tracer_provider().add_span_processor(console_processor)
            print("OpenTelemetry: Console export enabled")
        except Exception as e:
            print(f"OpenTelemetry: Failed to initialize console exporter: {e}")
            print("OpenTelemetry: Continuing without console export")
    
    return tracer

# Initialize with no-op tracer by default
tracer = setup_tracing(False)

# Global variables for cleanup
_temp_directories = set()
_cleanup_handlers = []

def register_temp_directory(temp_dir: str):
    """Register a temporary directory for cleanup"""
    global _temp_directories
    _temp_directories.add(temp_dir)

def cleanup_temp_files():
    """Clean up all registered temporary directories"""
    global _temp_directories
    logger = logging.getLogger(__name__)
    
    for temp_dir in _temp_directories.copy():
        try:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                logger.info(f"Cleaned up temporary directory: {temp_dir}")
        except Exception as e:
            logger.error(f"Failed to cleanup {temp_dir}: {e}")
    
    _temp_directories.clear()
    
    # Run any additional cleanup handlers
    for handler in _cleanup_handlers:
        try:
            handler()
        except Exception as e:
            logger.error(f"Error in cleanup handler: {e}")

def signal_handler(signum, frame):
    """Handle interrupt signals (Ctrl+C)"""
    logger = logging.getLogger(__name__)
    logger.info(f"\nReceived signal {signum}. Cleaning up temporary files...")
    
    cleanup_temp_files()
    
    logger.info("Cleanup complete. Exiting...")
    sys.exit(0)

def setup_signal_handlers():
    """Set up signal handlers for graceful shutdown"""
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Register cleanup to run on normal exit too
    atexit.register(cleanup_temp_files)

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

def setup_session_and_logging(debug: bool = False) -> Tuple[requests.Session, logging.Logger]:
    """Set up HTTP session and logging configuration"""
    session = requests.Session()
    
    # Configure global logger with JSON formatter
    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())
    
    # Clear any existing handlers and add our JSON handler
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    
    # Also configure root logger to use JSON format
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.DEBUG if debug else logging.INFO)
    
    return session, logger

def get_events_for_date(base_url: str, session: requests.Session, date: str, camera: str, 
                       zone: Optional[str] = None, label: Optional[str] = None) -> List[FrigateClip]:
    """Get events for a specific date from Frigate API"""
    with tracer.start_as_current_span("get_events_for_date") as span:
        span.set_attribute("date", date)
        span.set_attribute("camera", camera)
        if zone:
            span.set_attribute("zone", zone)
        if label:
            span.set_attribute("label", label)
        
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

        with tracer.start_as_current_span("frigate_api_request") as api_span:
            api_span.set_attribute("url", f"{base_url}/api/events")
            api_span.set_attribute("params", str(params))
            api_span.set_attribute("http.method", "GET")
            api_span.set_attribute("http.url", f"{base_url}/api/events")
            
            start_time = time.time()
            response = session.get(f"{base_url}/api/events", params=params)
            response.raise_for_status()
            api_duration = time.time() - start_time
            
            api_span.set_attribute("duration_seconds", api_duration)
            api_span.set_attribute("http.status_code", response.status_code)
            api_span.set_attribute("response.size_bytes", len(response.content))
            
            # Add performance classification
            if api_duration > 1.0:
                api_span.set_attribute("performance.classification", "slow")
            elif api_duration > 0.5:
                api_span.set_attribute("performance.classification", "medium")
            else:
                api_span.set_attribute("performance.classification", "fast")
        
        events = response.json()
        clips = [FrigateClip.from_event(event) for event in events]
        clips.sort(key=lambda x: x.start_time)
        
        span.set_attribute("events_count", len(events))
        span.set_attribute("clips_count", len(clips))
        
        return clips

def validate_clip_exists(base_url: str, session: requests.Session, clip_id: str) -> bool:
    """Check if clip exists and has content before downloading"""
    try:
        response = requests.head(f"{base_url}/api/events/{clip_id}/clip.mp4")
        if response.status_code != 200:
            return False
        
        # Check if content-length exists and is reasonable (> 1KB)
        content_length = response.headers.get('content-length')
        if content_length and int(content_length) < 1024:
            return False
            
        return True
    except Exception:
        return False


def download_clip(base_url: str, session: requests.Session, clip_id: str, temp_dir: str) -> str:
    """Download a clip from Frigate API"""
    with tracer.start_as_current_span("download_clip") as download_span:
        clip_path = os.path.join(temp_dir, f"clip_{clip_id}.mp4")
        download_span.set_attribute("clip_path", clip_path)
        
        start_time = time.time()
        
        # Download clip using direct requests (exactly like newlabelgrab.py)
        clip_url = f"{base_url}/api/events/{clip_id}/clip.mp4"
        response = requests.get(clip_url, stream=True)
        response.raise_for_status()
        with open(clip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        final_size = os.path.getsize(clip_path)
        logger.debug(f"Downloaded clip {clip_id}: {final_size} bytes")
        
        # No validation - just continue regardless of file size
        logger.debug(f"Clip {clip_id} downloaded: {final_size} bytes (no validation)")
        
        download_duration = time.time() - start_time
        download_span.set_attribute("download_duration_seconds", download_duration)
        download_span.set_attribute("file_size_bytes", final_size)
        download_span.set_attribute("bytes_written", final_size)
        
        return clip_path


def download_snapshot(base_url: str, session: requests.Session, clip_id: str, temp_dir: str) -> str:
    """Download a snapshot from Frigate API"""
    with tracer.start_as_current_span("download_snapshot") as snapshot_span:
        snapshot_path = os.path.join(temp_dir, f"snapshot_{clip_id}.jpg")
        snapshot_span.set_attribute("snapshot_path", snapshot_path)
        
        start_time = time.time()
        response = session.get(
            f"{base_url}/api/events/{clip_id}/snapshot.jpg",
            params={'bbox': 1, 'quality': 95}
        )
        response.raise_for_status()
        
        with open(snapshot_path, 'wb') as f:
            f.write(response.content)
        
        snapshot_duration = time.time() - start_time
        snapshot_span.set_attribute("download_duration_seconds", snapshot_duration)
        snapshot_span.set_attribute("file_size_bytes", os.path.getsize(snapshot_path))
        
        return snapshot_path


def analyze_video(clip_path: str, snapshot_path: str) -> Tuple[int, int, int, float]:
    """Analyze video and snapshot dimensions and duration"""
    with tracer.start_as_current_span("video_analysis") as analysis_span:
        start_time = time.time()
        
        # Debug: Check if files exist and their sizes
        logger.debug(f"Analyzing clip: {clip_path}, exists: {os.path.exists(clip_path)}, size: {os.path.getsize(clip_path) if os.path.exists(clip_path) else 'N/A'}")
        logger.debug(f"Analyzing snapshot: {snapshot_path}, exists: {os.path.exists(snapshot_path)}, size: {os.path.getsize(snapshot_path) if os.path.exists(snapshot_path) else 'N/A'}")
        
        try:
            probe = ffmpeg.probe(clip_path)
        except ffmpeg.Error as e:
            logger.error(f"ffprobe failed for {clip_path}: {e}")
            logger.error(f"ffprobe stderr: {e.stderr.decode() if e.stderr else 'No stderr'}")
            logger.error(f"ffprobe stdout: {e.stdout.decode() if e.stdout else 'No stdout'}")
            raise
            
        video_info = next((s for s in probe['streams'] if s['codec_type'] == 'video'), None)
        if video_info is None:
            raise ValueError(f"No video stream found in {clip_path}")
        main_width = int(video_info['width'])
        main_height = int(video_info['height'])
        duration_sec = float(video_info.get('duration', 0))
        
        # Get snapshot dimensions
        try:
            snapshot_probe = ffmpeg.probe(snapshot_path)
        except ffmpeg.Error as e:
            logger.error(f"ffprobe failed for snapshot {snapshot_path}: {e}")
            logger.error(f"ffprobe stderr: {e.stderr.decode() if e.stderr else 'No stderr'}")
            logger.error(f"ffprobe stdout: {e.stdout.decode() if e.stdout else 'No stdout'}")
            raise
            
        snapshot_info = next((s for s in snapshot_probe['streams'] if s['codec_type'] == 'video'), None)
        if snapshot_info is None:
            raise ValueError(f"No video stream found in snapshot {snapshot_path}")
        snapshot_height = int(snapshot_info['height'])
        
        analysis_duration = time.time() - start_time
        analysis_span.set_attribute("analysis_duration_seconds", analysis_duration)
        analysis_span.set_attribute("video_width", main_width)
        analysis_span.set_attribute("video_height", main_height)
        analysis_span.set_attribute("actual_duration", duration_sec)
        
        return main_width, main_height, snapshot_height, duration_sec


def create_pip_video(clip_path: str, snapshot_path: str, clip: FrigateClip, temp_dir: str, 
                    clip_number: int, total_clips: int, is_macos: bool, format: str = 'mp4') -> str:
    """Create picture-in-picture video with overlay"""
    with tracer.start_as_current_span("video_processing") as processing_span:
        main_width, main_height, snapshot_height, duration_sec = analyze_video(clip_path, snapshot_path)
        
        pip_width = main_width // 8  # Made smaller (1/8 of main video)
        x_position = main_width - pip_width - 10
        y_position = 10

        # Create PiP version with overlay
        pip_path = os.path.join(temp_dir, f"pip_{clip.id}.{format}")
        processing_span.set_attribute("output_path", pip_path)
        processing_span.set_attribute("video.input_resolution", f"{main_width}x{main_height}")
        processing_span.set_attribute("video.duration_seconds", duration_sec)
        
        # Calculate complexity score for performance analysis
        complexity_score = (main_width * main_height * duration_sec) / 1000000
        processing_span.set_attribute("video.complexity_score", complexity_score)
        
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
        
        # Add clip counter in lower left corner
        counter_text = f"{clip_number}/{total_clips}"
        counter_font_size = main_width // 40  # Responsive font size
        counter_padding = 20  # Padding from edges
        
        video = video.filter('drawtext',
                           text=counter_text,
                           fontcolor='white',
                           fontsize=counter_font_size,
                           x=counter_padding,
                           y=f'main_h-text_h-{counter_padding}',  # Bottom with padding
                           shadowcolor='black',
                           shadowx=2,
                           shadowy=2)
        
        if format == 'webm':
            # WebM format with VP9 codec - use software encoding (vp9_videotoolbox not widely available)
            stream = (
                video
                .output(pip_path, acodec='libopus', vcodec='libvpx-vp9',
                    crf=30, b='0', r=20)
            )
            processing_span.set_attribute("encoder", "libvpx-vp9")
        elif is_macos:
            stream = (
                video
                .output(pip_path, acodec='copy', vcodec='hevc_videotoolbox',
                    video_bitrate='5000k', tag='hvc1', r=20)
            )
            processing_span.set_attribute("encoder", "hevc_videotoolbox")
        else:
            stream = (
                video
                .output(pip_path, acodec='copy', vcodec='libx265',
                    crf=23, preset='medium', tag='hvc1', r=20)
            )
            processing_span.set_attribute("encoder", "libx265")

        start_time = time.time()
        stream.overwrite_output().run(capture_stdout=True, capture_stderr=True)
        processing_duration = time.time() - start_time
        
        processing_span.set_attribute("processing_duration_seconds", processing_duration)
        output_size = os.path.getsize(pip_path)
        processing_span.set_attribute("output_file_size_bytes", output_size)
        
        # Calculate processing efficiency metrics
        processing_rate = duration_sec / processing_duration if processing_duration > 0 else 0
        processing_span.set_attribute("video.processing_rate", processing_rate)  # seconds of video per second of processing
        
        compression_ratio = os.path.getsize(clip_path) / output_size if output_size > 0 else 0
        processing_span.set_attribute("video.compression_ratio", compression_ratio)
        
        # Performance classification for Grafana alerting
        if processing_rate < 0.5:  # Takes more than 2x real-time
            processing_span.set_attribute("performance.classification", "slow")
        elif processing_rate < 1.0:  # Takes more than real-time
            processing_span.set_attribute("performance.classification", "medium")
        else:
            processing_span.set_attribute("performance.classification", "fast")
        
        return pip_path


def process_single_clip(base_url: str, session: requests.Session, clip: FrigateClip, temp_dir: str, 
                       clip_number: int, total_clips: int, is_macos: bool, logger: logging.Logger, format: str = 'mp4') -> None:
    """Process a single clip: download, analyze, and create PiP video"""
    with tracer.start_as_current_span("process_clip") as span:
        span.set_attribute("clip_id", clip.id)
        span.set_attribute("clip_number", clip_number)
        span.set_attribute("total_clips", total_clips)
        span.set_attribute("clip_duration", clip.duration)
        
        try:
            # Skip validation - HEAD requests interfere with subsequent GET requests on Frigate server
            # if not validate_clip_exists(base_url, session, clip.id):
            #     logger.warning(f"Clip {clip.id} validation failed - skipping")
            #     span.set_attribute("validation_failed", True)
            #     span.set_attribute("success", False)
            #     raise ValueError(f"Clip {clip.id} failed validation")
            
            # Download clip and snapshot
            clip_path = download_clip(base_url, session, clip.id, temp_dir)
            snapshot_path = download_snapshot(base_url, session, clip.id, temp_dir)
            
            # Analyze video to get duration
            _, _, _, duration_sec = analyze_video(clip_path, snapshot_path)
            
            # Create PiP video
            pip_path = create_pip_video(clip_path, snapshot_path, clip, temp_dir, 
                                      clip_number, total_clips, is_macos, format)
            
            # Update clip object
            clip.download_path = clip_path
            clip.snapshot_path = snapshot_path
            clip.pip_path = pip_path
            clip.duration_sec = duration_sec
            
            span.set_attribute("success", True)
                
        except requests.exceptions.RequestException as e:
            span.set_attribute("success", False)
            span.set_attribute("error", str(e))
            span.set_attribute("error_type", "http_error")
            logger.warning(f"HTTP error processing clip {clip.id}: {str(e)}")
            # Make sure attributes are set to None in case of failure
            clip.download_path = None
            clip.snapshot_path = None
            clip.pip_path = None
        except (OSError, IOError) as e:
            span.set_attribute("success", False)
            span.set_attribute("error", str(e))
            span.set_attribute("error_type", "file_io_error")
            logger.warning(f"File I/O error processing clip {clip.id}: {str(e)}")
            # Make sure attributes are set to None in case of failure
            clip.download_path = None
            clip.snapshot_path = None
            clip.pip_path = None
        except subprocess.CalledProcessError as e:
            span.set_attribute("success", False)
            span.set_attribute("error", str(e))
            span.set_attribute("error_type", "ffmpeg_error")
            logger.warning(f"FFmpeg error processing clip {clip.id}: {str(e)}")
            # Make sure attributes are set to None in case of failure
            clip.download_path = None
            clip.snapshot_path = None
            clip.pip_path = None
        except Exception as e:
            span.set_attribute("success", False)
            span.set_attribute("error", str(e))
            span.set_attribute("error_type", "unknown_error")
            
            # Enhanced debug logging for StopIteration and other exceptions
            if isinstance(e, StopIteration):
                import traceback
                traceback_str = traceback.format_exc()
                span.set_attribute("full_traceback", traceback_str)
                logger.warning(f"StopIteration error processing clip {clip.id}: {str(e)}")
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Full traceback for clip {clip.id}:\n{traceback_str}")
            else:
                logger.warning(f"Unknown error processing clip {clip.id}: {type(e).__name__}: {str(e)}")
                if logger.isEnabledFor(logging.DEBUG):
                    import traceback
                    logger.debug(f"Full traceback for clip {clip.id}:\n{traceback.format_exc()}")
            
            # Make sure attributes are set to None in case of failure
            clip.download_path = None
            clip.snapshot_path = None
            clip.pip_path = None

def process_clips(base_url: str, session: requests.Session, clips: List[FrigateClip], 
                 temp_dir: str, is_macos: bool, debug: bool, logger: logging.Logger, format: str = 'mp4') -> List[FrigateClip]:
    """Process multiple clips in parallel"""
    with tracer.start_as_current_span("process_all_clips") as process_span:
        process_start = time.time()
        
        logger.info(f"Processing {len(clips)} clips...")
        
        if debug:
            total_duration = sum(clip.duration for clip in clips)
            logger.debug(f"Total duration: {total_duration:.2f} seconds")
            process_span.set_attribute("total_duration_seconds", total_duration)
        
        for i, clip in enumerate(clips, 1):
            try:
                process_single_clip(base_url, session, clip, temp_dir, i, len(clips), is_macos, logger, format)
            except requests.exceptions.RequestException as e:
                logger.error(f"HTTP error processing clip {i} (ID: {clip.id}): {str(e)}")
            except (OSError, IOError) as e:
                logger.error(f"File I/O error processing clip {i} (ID: {clip.id}): {str(e)}")
            except subprocess.CalledProcessError as e:
                logger.error(f"FFmpeg error processing clip {i} (ID: {clip.id}): {str(e)}")
            except Exception as e:
                # Enhanced debug logging for StopIteration and other exceptions
                if isinstance(e, StopIteration):
                    import traceback
                    logger.error(f"StopIteration error processing clip {i} (ID: {clip.id}): {str(e)}")
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"Full traceback for clip {i} (ID: {clip.id}):\n{traceback.format_exc()}")
                else:
                    logger.error(f"Unknown error processing clip {i} (ID: {clip.id}): {type(e).__name__}: {str(e)}")
                    if logger.isEnabledFor(logging.DEBUG):
                        import traceback
                        logger.debug(f"Full traceback for clip {i} (ID: {clip.id}):\n{traceback.format_exc()}")
            if i % 5 == 0:
                logger.info(f"Processed {i}/{len(clips)} clips...")
        
        process_duration = time.time() - process_start
        process_span.set_attribute("total_processing_duration_seconds", process_duration)
        
        # Count successfully processed clips
        successful_clips = [clip for clip in clips if clip.pip_path is not None]
        logger.info(f"Successfully processed {len(successful_clips)}/{len(clips)} clips")
        
        return successful_clips


def create_concat_file(clips: List[FrigateClip], temp_dir: str, logger: logging.Logger) -> str:
    """Create ffmpeg concat file list"""
    with tracer.start_as_current_span("create_concat_file") as concat_span:
        file_list_path = os.path.join(temp_dir, "files.txt")
        concat_count = 0
        
        with open(file_list_path, 'w') as f:
            for clip in clips:
                if clip.pip_path:  # Only include clips with a valid pip_path
                    f.write(f"file '{clip.pip_path}'\n")
                    concat_count += 1
                else:
                    logger.warning(f"Skipping clip {clip.id} in concat as it has no pip_path")
        
        concat_span.set_attribute("clips_in_concat", concat_count)
        return file_list_path


def create_chapters_file(clips: List[FrigateClip], temp_dir: str) -> str:
    """Create ffmpeg chapters file"""
    with tracer.start_as_current_span("create_chapters_file") as chapters_span:
        chapters_file_path = os.path.join(temp_dir, "chapters.txt")
        current_time_ms = 0
        chapters_count = 0

        with open(chapters_file_path, 'w') as f:
            for i, clip in enumerate(clips):
                # Skip clips that failed processing
                if not clip.pip_path or not clip.duration_sec:
                    continue
                
                start_time_ms = current_time_ms
                duration_ms = int(clip.duration_sec * 1000)
                end_time_ms = start_time_ms + duration_ms
                
                f.write(f"[CHAPTER]\n")
                f.write(f"TIMEBASE=1/1000\n")
                f.write(f"START={start_time_ms}\n")
                f.write(f"END={end_time_ms}\n")
                f.write(f"title={clip.chapter_title}\n\n")
                
                # Update current time for next clip
                current_time_ms = end_time_ms
                chapters_count += 1
        
        chapters_span.set_attribute("chapters_count", chapters_count)
        return chapters_file_path


def create_metadata_file(chapters_file_path: str, temp_dir: str, date: str, camera: str, zone: Optional[str] = None) -> str:
    """Create ffmpeg metadata file with chapters"""
    with tracer.start_as_current_span("create_metadata_file") as metadata_span:
        metadata_file = os.path.join(temp_dir, "metadata.txt")
        with open(metadata_file, 'w') as f:
            f.write(f";FFMETADATA1\n")
            
            # Add global metadata
            f.write(f"title=Frigate Archive {date} - {camera}\n")
            if zone:
                f.write(f"comment=Zone: {zone}\n")
            f.write("\n")  # Add a blank line for better separation
            
            # Read and append chapters
            with open(chapters_file_path) as chapters:
                f.write(chapters.read())
        
        return metadata_file


def concatenate_videos(file_list_path: str, temp_dir: str, logger: logging.Logger, format: str = 'mp4') -> str:
    """Concatenate videos using ffmpeg"""
    with tracer.start_as_current_span("video_concatenation") as concat_span:
        temp_concat = os.path.join(temp_dir, f"temp_concat.{format}")
        concat_span.set_attribute("temp_concat_path", temp_concat)
        
        start_time = time.time()
        try:
            (
                ffmpeg
                .input(file_list_path, format='concat', safe=0)
                .output(temp_concat, vcodec='copy', acodec='copy')
                .overwrite_output()
                .run(capture_stdout=True, capture_stderr=True)
            )
        except ffmpeg.Error as e:
            concat_span.set_attribute("error", str(e))
            logger.error(f"Error during concat: {e.stderr.decode() if hasattr(e, 'stderr') else str(e)}")
            raise
        
        concat_duration = time.time() - start_time
        concat_span.set_attribute("duration_seconds", concat_duration)
        concat_span.set_attribute("output_file_size_bytes", os.path.getsize(temp_concat))
        
        return temp_concat


def add_metadata_to_video(temp_concat: str, metadata_file: str, output_path: str, is_macos: bool, logger: logging.Logger, format: str = 'mp4') -> None:
    """Add metadata (chapters) to final video"""
    with tracer.start_as_current_span("final_metadata_processing") as final_span:
        # Use a direct ffmpeg command as python-ffmpeg doesn't support metadata input well
        ffmpeg_cmd = [
            'ffmpeg',
            '-i', temp_concat,
            '-i', metadata_file,
            '-map_metadata', '1',
            '-codec', 'copy',
            '-y',
            output_path
        ]
        
        # Add format-specific tags
        if format == 'mp4':
            ffmpeg_cmd.insert(-2, '-tag:v')
            ffmpeg_cmd.insert(-2, 'hvc1')
        
        final_span.set_attribute("ffmpeg_command", ' '.join(ffmpeg_cmd))
        
        # Execute the command
        start_time = time.time()
        try:
            subprocess.run(ffmpeg_cmd, check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            final_span.set_attribute("error", str(e))
            logger.error(f"Error during final processing: {e.stderr.decode() if e.stderr else str(e)}")
            raise ffmpeg.Error(e.stderr if e.stderr else str(e))
        
        final_duration = time.time() - start_time
        final_span.set_attribute("duration_seconds", final_duration)
        final_span.set_attribute("final_file_size_bytes", os.path.getsize(output_path))


def archive_events_for_date(base_url: str, session: requests.Session, date: str, camera: str, 
                           output_dir: str, is_macos: bool, debug: bool, logger: logging.Logger,
                           zone: Optional[str] = None, label: Optional[str] = None, format: str = 'mp4') -> None:
    """Archive events for a specific date"""
    with tracer.start_as_current_span("archive_events") as span:
        span.set_attribute("date", date)
        span.set_attribute("camera", camera)
        if zone:
            span.set_attribute("zone", zone)
        if label:
            span.set_attribute("label", label)
        
        components = [date, camera]
        if zone:
            components.append(zone)
        if label:
            components.append(label)
        
        output_filename = '-'.join(components) + f'.{format}'
        output_path = os.path.join(output_dir, output_filename)
        span.set_attribute("output_filename", output_filename)
        
        if os.path.exists(output_path):
            logger.warning(f"Archive {output_filename} already exists. Skipping.")
            span.set_attribute("skipped", True)
            return

        clips = get_events_for_date(base_url, session, date, camera, zone, label)
        if not clips:
            logger.info(f"No events found for the specified criteria on {date}")
            span.set_attribute("no_clips_found", True)
            return
        
        span.set_attribute("clips_count", len(clips))
        logger.info(f"Found {len(clips)} events matching criteria")
        
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                # Register temporary directory for cleanup on interruption
                register_temp_directory(temp_dir)
                
                # Process all clips
                successful_clips = process_clips(base_url, session, clips, temp_dir, is_macos, debug, logger, format)
                
                if not successful_clips:
                    logger.error("No clips were successfully processed. Aborting.")
                    span.set_attribute("success", False)
                    span.set_attribute("error", "No clips successfully processed")
                    sys.exit(1)
                    
                # Check if too many clips failed - indicates Frigate issues
                failed_count = len(clips) - len(successful_clips)
                if failed_count > 2:
                    logger.error(f"Too many clips failed ({failed_count}/{len(clips)}). This suggests issues with Frigate server. Aborting.")
                    span.set_attribute("success", False)
                    span.set_attribute("error", f"Too many failed clips: {failed_count}")
                    sys.exit(1)
                    
                span.set_attribute("successful_clips_count", len(successful_clips))
                
                # Create files for concatenation
                file_list_path = create_concat_file(successful_clips, temp_dir, logger)
                chapters_file_path = create_chapters_file(successful_clips, temp_dir)
                
                logger.info(f"Creating final video with {len(successful_clips)} chapters...")
                
                # Create metadata file
                metadata_file = create_metadata_file(chapters_file_path, temp_dir, date, camera, zone)
                
                # Concatenate videos
                temp_concat = concatenate_videos(file_list_path, temp_dir, logger, format)
                
                # Add metadata to final video
                add_metadata_to_video(temp_concat, metadata_file, output_path, is_macos, logger, format)
            
                span.set_attribute("success", True)
                logger.info(f"Successfully archived events with chapters to {output_filename}")
                
                # Unregister temp directory as it will be cleaned up by context manager
                _temp_directories.discard(temp_dir)
            
        except ffmpeg.Error as e:
            span.set_attribute("success", False)
            span.set_attribute("error", str(e))
            error_msg = e.stderr.decode() if hasattr(e, 'stderr') else str(e)
            logger.error(f"Error during ffmpeg processing: {error_msg}")
            if os.path.exists(output_path):
                os.remove(output_path)
            _temp_directories.discard(temp_dir)
        except Exception as e:
            span.set_attribute("success", False)
            span.set_attribute("error", str(e))
            logger.error(f"Error during archiving: {str(e)}")
            if os.path.exists(output_path):
                os.remove(output_path)
            _temp_directories.discard(temp_dir)

def json_output_mode(base_url: str, session: requests.Session, date: str, camera: str, 
                    zone: Optional[str] = None, label: Optional[str] = None) -> None:
    """Handle JSON output mode"""
    clips = get_events_for_date(base_url, session, date, camera, zone, label)
    clip_details = []
    
    for i, clip in enumerate(clips, 1):
        clip_info = {
            "count": i,
            "total": len(clips),
            "id": clip.id,
            "camera": clip.camera,
            "label": clip.label,
            "zone": clip.zone,
            "score": clip.score,
            "start_time": clip.start_time,
            "end_time": clip.end_time,
            "duration": clip.duration,
            "formatted_datetime": clip.formatted_datetime,
            "clip_url": f"{base_url}/api/events/{clip.id}/clip.mp4",
            "snapshot_url": f"{base_url}/api/events/{clip.id}/snapshot.jpg"
        }
        clip_details.append(clip_info)
    
    # Output to console
    print(json.dumps(clip_details, indent=2))
    
    # Create filename with date, camera, zone, and label
    filename_parts = ["clips", date, camera]
    if zone:
        filename_parts.append(zone)
    if label:
        filename_parts.append(label)
    jsonl_filename = "_".join(filename_parts) + ".jsonl"
    
    with open(jsonl_filename, 'w') as f:
        for clip_info in clip_details:
            f.write(json.dumps(clip_info) + '\n')
    
    print(f"\nClip details written to {jsonl_filename}")


def archive_mode(base_url: str, session: requests.Session, date: Optional[str], 
                date_range: Optional[str], camera: str, output_dir: str, 
                is_macos: bool, debug: bool, logger: logging.Logger,
                zone: Optional[str] = None, label: Optional[str] = None, format: str = 'mp4') -> None:
    """Handle archive mode (single date or date range)"""
    # Handle single date
    if date:
        archive_events_for_date(base_url, session, date, camera, output_dir, is_macos, debug, logger, zone, label, format)
    
    # Handle date range
    if date_range:
        try:
            start_date_str, end_date_str = date_range.split(':')
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            
            if start_date > end_date:
                raise ValueError("Start date must be before or equal to end date")
            
            current_date = start_date
            while current_date <= end_date:
                date_str = current_date.strftime("%Y-%m-%d")
                print(f"Processing {date_str}...")
                archive_events_for_date(base_url, session, date_str, camera, output_dir, is_macos, debug, logger, zone, label, format)
                current_date += timedelta(days=1)
                
        except ValueError as e:
            raise ValueError(f"Invalid date range format. Use YYYY-MM-DD:YYYY-MM-DD. Error: {e}")


def main():
    # Set up signal handlers early
    setup_signal_handlers()
    
    parser = argparse.ArgumentParser(description='Archive Frigate events with chapters')
    parser.add_argument('--base-url', required=True, help='Base URL of Frigate instance')
    parser.add_argument('--date', help='Date to archive (YYYY-MM-DD)')
    parser.add_argument('--date-range', help='Date range to archive (YYYY-MM-DD:YYYY-MM-DD)')
    parser.add_argument('--camera', required=True, help='Camera name')
    parser.add_argument('--zone', help='Zone name (optional)')
    parser.add_argument('--label', help='Label to filter by (optional)')
    parser.add_argument('--output-dir', default='archives', help='Output directory')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--json-output', action='store_true', help='Output clip details in JSON format and exit')
    parser.add_argument('--enable-tracing', action='store_true', help='Enable OpenTelemetry tracing')
    parser.add_argument('--format', choices=['mp4', 'webm'], default='mp4', help='Output video format (default: mp4)')
    
    args = parser.parse_args()
    
    # Set up tracing based on command line flag
    global tracer
    tracer = setup_tracing(args.enable_tracing)
    
    # Validate date arguments
    if not args.date and not args.date_range:
        parser.error("Either --date or --date-range must be specified")
    if args.date and args.date_range:
        parser.error("Cannot specify both --date and --date-range")
    
    # Setup session and logging
    session, logger = setup_session_and_logging(args.debug)
    
    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Determine platform
    is_macos = platform.system() == 'Darwin'
    
    # Handle JSON output mode
    if args.json_output:
        date_to_use = args.date if args.date else args.date_range.split(':')[0] if args.date_range else None
        if not date_to_use:
            parser.error("Need either --date or --date-range for JSON output")
        
        json_output_mode(args.base_url, session, date_to_use, args.camera, args.zone, args.label)
        return
    
    # Handle archive mode
    try:
        archive_mode(args.base_url, session, args.date, args.date_range, args.camera, 
                    args.output_dir, is_macos, args.debug, logger, args.zone, args.label, args.format)
    except ValueError as e:
        parser.error(str(e))

if __name__ == "__main__":
    main()
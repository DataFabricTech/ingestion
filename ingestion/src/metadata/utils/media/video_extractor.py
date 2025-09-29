from pymediainfo import MediaInfo

class MediaInfoMetadataExtractor:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def extract_metadata(self) -> dict:

        media_info = MediaInfo.parse(self.file_path)
        metadata = {}
        for track in media_info.tracks:
            if track.track_type == "General":
                metadata['general.file_size'] = int(getattr(track, 'file_size', 0))
                metadata['general.format'] = getattr(track, 'format', None)
                metadata['general.duration_ms'] = int(getattr(track, 'duration', 0))
                metadata['general.overall_bit_rate'] = int(getattr(track, 'overall_bit_rate', 0))
                metadata['general.frame_count'] = int(getattr(track, 'frame_count', 0))
                metadata['general.frame_rate'] = float(getattr(track, 'frame_rate', 0))

            elif track.track_type == "Video":
                metadata['video.video_codec'] = getattr(track, 'codec_id', None)
                metadata['video.video_format'] = getattr(track, 'format', None)
                metadata['video.width'] = int(getattr(track, 'width', 0))
                metadata['video.height'] = int(getattr(track, 'height', 0))
                metadata['video.bit_depth'] = int(getattr(track, 'bit_depth', 0))
                metadata['video.frame_rate_video'] = float(getattr(track, 'frame_rate', 0))
                metadata['video.color_space'] = getattr(track, 'color_space', None)
                metadata['video.scan_type'] = getattr(track, 'scan_type', None)
                metadata['video.display_aspect_ratio'] = getattr(track, 'display_aspect_ratio', None)
                metadata['video.encoded_library_name'] = getattr(track, 'encoded_library_name', None)

        return metadata
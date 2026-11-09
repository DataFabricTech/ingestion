from pymediainfo import MediaInfo


def _to_int(value, default=0):
    return int(value) if value is not None else default


def _to_float(value, default=0.0):
    return float(value) if value is not None else default


class MediaInfoMetadataExtractor:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def extract_metadata(self) -> dict:

        media_info = MediaInfo.parse(self.file_path)
        metadata = {}
        for track in media_info.tracks:
            if track.track_type == "General":
                metadata['general.file_size'] = _to_int(getattr(track, 'file_size', None))
                metadata['general.format'] = getattr(track, 'format', None)
                metadata['general.duration_ms'] = _to_int(getattr(track, 'duration', None))
                metadata['general.overall_bit_rate'] = _to_int(getattr(track, 'overall_bit_rate', None))
                metadata['general.frame_count'] = _to_int(getattr(track, 'frame_count', None))
                metadata['general.frame_rate'] = _to_float(getattr(track, 'frame_rate', None))

            elif track.track_type == "Video":
                metadata['video.video_codec'] = getattr(track, 'codec_id', None)
                metadata['video.video_format'] = getattr(track, 'format', None)
                metadata['video.width'] = _to_int(getattr(track, 'width', None))
                metadata['video.height'] = _to_int(getattr(track, 'height', None))
                metadata['video.bit_depth'] = _to_int(getattr(track, 'bit_depth', None))
                metadata['video.frame_rate_video'] = _to_float(getattr(track, 'frame_rate', None))
                metadata['video.color_space'] = getattr(track, 'color_space', None)
                metadata['video.scan_type'] = getattr(track, 'scan_type', None)
                metadata['video.display_aspect_ratio'] = getattr(track, 'display_aspect_ratio', None)
                metadata['video.encoded_library_name'] = getattr(track, 'encoded_library_name', None)

        return metadata
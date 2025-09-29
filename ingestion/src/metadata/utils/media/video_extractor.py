import ffmpeg

class FfmpegMetadataExtractor:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def extract_metadata(self) -> dict:

        probe = ffmpeg.probe(self.file_path)
        metadata = {}
        if 'format' in probe:
            metadata.update(self.get_metadata(probe['format'], 'format'))

        if 'streams' in probe:
            for i, stream in enumerate(probe['streams']):
                metadata.update(self.get_metadata(stream, f"stream.{i}"))

    def get_metadata(self, data :dict, key):
        metadata = {}
        for k, v in data.items():
            if k in ('tags', 'disposition'):
                metadata.update(self.get_metadata(data[k], f"{key}.{k}"))
            else:
                metadata[f"{key}.{k}"] = v

        return metadata
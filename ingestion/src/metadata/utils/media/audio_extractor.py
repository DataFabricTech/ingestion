from mutagen.mp3 import MP3
from mutagen.wave import WAVE

class MutagenMetadataExtractor:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def extract_metadata_from_mp3(self) -> dict:

        audio_file = MP3(self.file_path)
        audio_file_info = audio_file.info
        metadata = {}
        for k, v in vars(audio_file_info).items():
            metadata[f"info.{k}"] = v

        if audio_file.tags:
            for k, v in audio_file.tags.items():
                metadata[f"tags.{k}"] = v

        return metadata

    def extract_metadata_from_wav(self) -> dict:

        audio_file = WAVE(self.file_path)
        audio_file_info = audio_file.info
        metadata = {}
        for k, v in vars(audio_file_info).items():
            metadata[f"info.{k}"] = v

        if audio_file.tags:
            for k, v in audio_file.tags.items():
                metadata[f"tags.{k}"] = v

        return metadata

"""오디오 파일 메타데이터 추출 (MP3·WAV).

tinytag(MIT)를 사용한다. 기존 mutagen(GPL-2.0)에서 교체되었으며,
반환 키 이름은 기존과 호환되도록 info.* / tags.* 접두를 유지한다.
"""
from tinytag import TinyTag


class MutagenMetadataExtractor:
    """오디오 메타데이터 추출기.

    클래스명은 호출부 호환을 위해 유지한다(storage_service.py).
    """

    # info.* 로 내보낼 (출력키, tinytag 속성) 매핑
    _INFO_FIELDS = (
        ("length", "duration"),
        ("bitrate", "bitrate"),
        ("sample_rate", "samplerate"),
        ("channels", "channels"),
        ("bits_per_sample", "bitdepth"),
    )

    # tags.* 로 내보낼 (출력키, tinytag 속성) 매핑
    _TAG_FIELDS = (
        ("title", "title"),
        ("artist", "artist"),
        ("albumartist", "albumartist"),
        ("album", "album"),
        ("year", "year"),
        ("track", "track"),
        ("track_total", "track_total"),
        ("disc", "disc"),
        ("disc_total", "disc_total"),
        ("genre", "genre"),
        ("composer", "composer"),
        ("comment", "comment"),
    )

    def __init__(self, file_path: str):
        self.file_path = file_path

    def _extract(self) -> dict:
        tag = TinyTag.get(self.file_path)
        metadata = {}

        for out_key, attr in self._INFO_FIELDS:
            metadata[f"info.{out_key}"] = getattr(tag, attr, None)

        # tinytag는 kbps(float), mutagen은 bps(int) — 기존 수집 데이터와 단위를 맞춘다
        bitrate = metadata.get("info.bitrate")
        if bitrate is not None:
            metadata["info.bitrate"] = int(round(bitrate * 1000))

        for out_key, attr in self._TAG_FIELDS:
            metadata[f"tags.{out_key}"] = getattr(tag, attr, None)

        return metadata

    def extract_metadata_from_mp3(self) -> dict:
        return self._extract()

    def extract_metadata_from_wav(self) -> dict:
        return self._extract()

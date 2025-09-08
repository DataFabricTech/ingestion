import exifread

class ExifreadMetadataExtractor:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def extract_metadata(self) -> dict:

        metadata = {}
        # 이미지 파일 열기 (JPEG, TIFF 권장)
        with open(self.file_path, 'rb') as f:
            tags = exifread.process_file(f)

        # 모든 메타
        for k in tags:
            metadata[k] = tags[k]

        return metadata





from PIL import Image
from PIL.ExifTags import TAGS

class JpgMetadataExtractor:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def extract_metadata(self) -> dict:

        metadata = {}
        with Image.open(self.file_path) as img:
            metadata["format"] = img.format         # JPEG, PNG 등
            metadata["mode"] = img.mode             # RGB, RGBA, L (흑백) 등
            metadata["size"] = img.size             # (width, height)

            # info : dpi-해상도 (tuple: (x_dpi, y_dpi)), comment-주석
            img_info = img.info
            if img_info:
                info_data = {}
                for k, v in img_info.items():
                    if k == 'exif':
                        continue
                    info_data[k] = v
                if len(info_data):
                    metadata["info"] = info_data

            # EXIF 데이터 추출 (JPEG, TIFF에 주로 존재)
            img_exif = img._getexif()
            if img_exif:
                exif_data = {}
                for tag_id, value in img_exif.items():
                    tag = TAGS.get(tag_id, tag_id)
                    exif_data[tag] = value

                if len(exif_data):
                    metadata["exif"] = exif_data

        return metadata





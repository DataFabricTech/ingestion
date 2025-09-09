from PIL import Image
from PIL.ExifTags import TAGS
from io import BytesIO
import base64
import pytesseract
import re

from metadata.utils.logger import ingestion_logger
logger = ingestion_logger()

"""
    확인 완료한 파일 확장자
    - jpg
    - jpeg
    - png
"""
class PilMetadataExtractor:
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
                for k, v in img_info.items():
                    if k == 'exif':
                        continue
                    metadata[f"info.{k}"] = v

            # EXIF 데이터 추출 (JPEG, TIFF에 주로 존재)
            if hasattr(img, '_getexif'):
                img_exif = img._getexif()
                if img_exif:
                    for tag_id, value in img_exif.items():
                        tag = TAGS.get(tag_id, tag_id)
                        metadata[f"exif.{tag}"] = value

            # 텍스트 추출
            try:
                image_text = self.get_text(img)
                if len(image_text) > 0:
                    metadata["image_text"] = image_text
            except Exception as e:
                logger.debug(f"get text fail [{e}], file : {self.file_path}")

        return metadata

    def get_sample_data(self, sample_width: int = 2000, sample_height: int = 2000):

        # 메모리에 저장 (BytesIO 사용)
        buffered = BytesIO()
        with Image.open(self.file_path) as img:

            # EXIF 회전 정보 적용
            try:
                # EXIF 정보에서 회전 값 추출
                # (사이즈 변환하면서 메타정보가 삭제되기 때문에 회전값 확인 후 회전값을 이미지에 재 적용)
                for orientation in TAGS.keys():
                    if TAGS[orientation]=='Orientation':
                        break
                exif=dict(img._getexif().items())
                if exif.get(orientation) == 3:
                    img = img.rotate(180, expand=True)
                elif exif.get(orientation) == 6:
                    img = img.rotate(270, expand=True)
                elif exif.get(orientation) == 8:
                    img = img.rotate(90, expand=True)
            except (AttributeError, KeyError, IndexError):
                # EXIF 정보가 없거나 문제가 발생한 경우에는 예외 처리
                pass

            width, height = img.size

            is_resize = False
            if height > width and height > sample_height: # 세로 이미지
                aspect_ratio = width / height
                sample_width = int(sample_height * aspect_ratio)
                is_resize = True
            elif width > sample_width: # 가로 이미지, 정사각형 (가로기준하면될듯)
                aspect_ratio = height / width
                sample_height = int(sample_width * aspect_ratio)
                is_resize = True
            else: # 사이즈가 작아서 리사이즈 없음
                pass

            if is_resize:
                resized_img = img.resize((sample_width, sample_height))
                resized_img.save(buffered, format="JPEG")
            else:
                img.save(buffered, format="JPEG")

        # base64 인코딩
        return base64.b64encode(buffered.getvalue()).decode("utf-8")

    def get_clean_text(self, text):
        """
        정규 표현식으로 text 정리
        :return: 특수문자제거(영어,한글만 남기기), 연속된 공백을 하나로 줄이기
        """
        # 특수 문자 제거 (영어와 한글만 남기기)
        cleaned_text = re.sub(r'[^a-zA-Z0-9가-힣\s]', '', text)

        # 연속된 공백을 하나로 줄이기
        cleaned_text = re.sub(r'\s+', ' ', cleaned_text)

        return cleaned_text

    def get_text(self, image):
        # 이미지 열기
        # image = Image.open(self.file_path)

        # 이미지 전처리
        image = image.convert('L')  # 흑백 변환 (그레이스케일)
        image = image.point(lambda p: p * 1.2)  # 대비 조정

        # 텍스트 추출
        text = pytesseract.image_to_string(image, lang='kor+eng')  # 필요 시 'kor'로 변경

        # 텍스트 정리
        return self.get_clean_text(text)




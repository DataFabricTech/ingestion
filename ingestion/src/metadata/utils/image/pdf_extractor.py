import subprocess
from pathlib import Path
import base64
import asyncio

from metadata.utils.logger import profiler_logger

logger = profiler_logger()

# PyMuPDF(AGPL-3.0)는 선택 설치다. 미설치 시 PDF 추출만 비활성화된다.
#   pip install 'openmetadata-ingestion[pdf]'
try:
    import fitz  # PyMuPDF (AGPL-3.0) — optional extra [pdf]

    _HAS_PYMUPDF = True
except ImportError:  # pragma: no cover
    fitz = None
    _HAS_PYMUPDF = False


def _pdf_unavailable(feature: str):
    logger.info(
        "%s 비활성화: PyMuPDF가 설치되지 않았습니다. "
        "pip install 'openmetadata-ingestion[pdf]' 로 활성화할 수 있습니다.",
        feature,
    )
    return None

class PdfMetadataExtractor:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def extract_metadata(self) -> dict:
        if not _HAS_PYMUPDF:
            return _pdf_unavailable("PDF 메타데이터 추출")

        with fitz.open(self.file_path) as file:
            return file.metadata


    # async def convert_to_pdf(self):
    def convert_to_pdf(self):

        input_path = Path(self.file_path)
        output_dir = input_path.parent
        output_pdf = output_dir / f"{input_path.stem}.pdf"

        if not input_path.exists():
            raise FileNotFoundError(f"{input_path} not found")

        liberoffice_path = "libreoffice"
        # liberoffice_path = "/Applications/LibreOffice.app/Contents/MacOS/soffice"

        try:
            subprocess.run([
                liberoffice_path,
                "--headless", "--convert-to", "pdf",
                "--outdir", str(output_dir),
                str(input_path)
            ], check=True)

            # process = await asyncio.create_subprocess_exec(
            #     liberoffice_path,
            #     "--headless", "--convert-to", "pdf",
            #     "--outdir", str(output_dir),
            #     str(input_path)
            # )
            #
            # await process.wait()

        except subprocess.CalledProcessError as e:
            logger.error(f"LibreOffice 변환 실패: {e}")
            return None
        except Exception as e:
            logger.error(f"예상치 못한 오류 발생: {e}")
            return None

        return output_pdf


    def get_sample_data(self, input_path, max_side=2000):
        if not _HAS_PYMUPDF:
            return _pdf_unavailable("PDF 샘플 데이터 추출")

        with fitz.open(input_path) as file:

            # 페이지 가져오기 (keywords 가 포함된 페이지를 찾으면 그 다음 페이지를 이미지화한다)
            page_num = 0
            # keywords = ["목차", "Table of Contents", "Contents"]
            # for i, page in enumerate(file):
            #     text = page.get_text()
            #     if any(kw in text for kw in keywords):
            #         page_num = i + 1  # 목차 다음 페이지
            page = file.load_page(page_num)

            # 페이지 원본 크기
            rect = page.rect
            width, height = rect.width, rect.height

            if height > width and height > max_side: # 세로 이미지
                scale = width / height
                is_resize = True
            elif width > max_side: # 가로 이미지, 정사각형 (가로기준하면될듯)
                scale = height / width
                is_resize = True
            else: # 사이즈가 작아서 리사이즈 없음
                is_resize = False

            if is_resize:
                # scale을 이용해 매트릭스 생성 (가로/세로 비율 유지)
                matrix = fitz.Matrix(scale, scale)
            else:
                matrix = None

            # 이미지 생성
            pix = page.get_pixmap(matrix=matrix)

            # 이미지 저장
            sample_data_bytes = pix.tobytes("jpeg")

        # base64 인코딩
        return base64.b64encode(sample_data_bytes).decode("utf-8")
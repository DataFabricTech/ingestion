import fitz
from pathlib import Path
import base64

class PdfMetadataExtractor:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def extract_metadata(self) -> dict:

        with fitz.open(self.file_path) as file:
            return file.metadata


    def convert_to_pdf(self):
        # libreoffice
        import subprocess

        input_path = Path(self.file_path)
        output_dir = input_path.parent
        output_pdf = output_dir / f"{input_path.stem}.pdf"

        if not input_path.exists():
            raise FileNotFoundError(f"{input_path} not found")

        liberoffice_path = "libreoffice"
        # liberoffice_path = "/Applications/LibreOffice.app/Contents/MacOS/soffice"
        subprocess.run([
            liberoffice_path,
            "--headless", "--convert-to", "pdf",
            "--outdir", str(output_dir),
            str(input_path)
        ], check=True)

        return output_pdf


    def get_sample_data(self, input_path, max_side=2000):

        with fitz.open(input_path) as file:

            # 페이지 가져오기 (keywords 가 포함된 페이지를 찾으면 그 다음 페이지를 이미지화한다)
            page_num = 0
            keywords = ["목차", "Table of Contents", "Contents"]
            for i, page in enumerate(file):
                text = page.get_text()
                if any(kw in text for kw in keywords):
                    page_num = i + 1  # 목차 다음 페이지
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
            sample_data_bytes = pix.tobytes("png")

        # base64 인코딩
        return base64.b64encode(sample_data_bytes).decode("utf-8")
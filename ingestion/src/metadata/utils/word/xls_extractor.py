import zipfile
from lxml import etree
import olefile
import re

class XlsMetadataExtractor:

    def is_xls(self, file_path):

        if file_path.endswith(".xls"):
            return True
        else:
            if file_path.endswith(".xlsx"):
                if zipfile.is_zipfile(file_path):
                    return False
                return None
            else:
                return True


    def get_metadata(self, file_path) -> dict:
        if self.is_xls(file_path):
            return self.extract_metadata(file_path)
        return self.extract_xlsx_metadata(file_path)

    def extract_metadata(self, file_path) -> dict:
        # MS-OLEPS 표준 Property IDs
        PID_CODEPAGE     = 1
        PID_TITLE        = 2
        PID_SUBJECT      = 3
        PID_AUTHOR       = 4
        PID_KEYWORDS     = 5
        PID_COMMENTS     = 6
        PID_TEMPLATE     = 7
        PID_LASTAUTHOR   = 8
        PID_REVNUMBER    = 9
        PID_CREATE_DTM   = 12
        PID_LASTSAVE_DTM = 13
        PID_APPNAME      = 18
        PID_SECURITY     = 19

        metadata = {}

        if not olefile.isOleFile(file_path):
            raise ValueError("Not a valid OLE2 (XLS) file.")

        with olefile.OleFileIO(file_path) as ole:
            if ole.exists("\x05SummaryInformation"):
                props = ole.getproperties("\x05SummaryInformation")

                if PID_CODEPAGE in props:
                    metadata['codepage'] = props.get(PID_CODEPAGE)
                if PID_TITLE in props:
                    metadata['title'] = props.get(PID_TITLE).decode()
                if PID_SUBJECT in props:
                    metadata['subject'] = props.get(PID_SUBJECT).decode()
                if PID_AUTHOR in props:
                    metadata['author'] = props.get(PID_AUTHOR).decode()
                if PID_KEYWORDS in props:
                    metadata['keywords'] = props.get(PID_KEYWORDS).decode()
                if PID_COMMENTS in props:
                    metadata['comments'] = props.get(PID_COMMENTS).decode()
                if PID_TEMPLATE in props:
                    metadata['template'] = props.get(PID_TEMPLATE).decode()
                if PID_LASTAUTHOR in props:
                    metadata['lastauthor'] = props.get(PID_LASTAUTHOR).decode()
                if PID_REVNUMBER in props:
                    metadata['revnumber'] = props.get(PID_REVNUMBER).decode()
                if PID_CREATE_DTM in props:
                    metadata['create_dtm'] = props.get(PID_CREATE_DTM)
                if PID_LASTSAVE_DTM in props:
                    metadata['lastsave_dtm'] = props.get(PID_LASTSAVE_DTM)
                if PID_APPNAME in props:
                    metadata['appname'] = props.get(PID_APPNAME).decode()
                if PID_SECURITY in props:
                    metadata['security'] = props.get(PID_SECURITY)

        return metadata


    def extract_xlsx_metadata(self, file_path):
        metadata_dict = {}
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            if 'docProps/core.xml' in zip_ref.namelist():
                core = etree.fromstring(zip_ref.read('docProps/core.xml'))
                for elem in core:
                    tag = etree.QName(elem).localname
                    metadata_dict[tag] = elem.text

            if 'docProps/app.xml' in zip_ref.namelist():
                app = etree.fromstring(zip_ref.read('docProps/app.xml'))
                for elem in app:
                    tag = etree.QName(elem).localname
                    metadata_dict[tag] = elem.text

        return metadata_dict

    def extract_xlsx_metadata(self, file_path):

        sheet_count = 0
        total_rows = 0
        total_columns = 0
        total_cells = 0
        total_text_characters = 0
        total_text_words = 0
        image_count = 0

        metadata_dict = {}

        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            # 1) core.xml, app.xml, custom.xml에서 메타데이터 추출
            for path in ['docProps/core.xml', 'docProps/app.xml', 'docProps/custom.xml']:
                if path in zip_ref.namelist():
                    root = etree.fromstring(zip_ref.read(path))
                    for elem in root.iter():
                        tag = etree.QName(elem).localname
                        if elem.text and tag not in metadata_dict:
                            metadata_dict[tag] = elem.text

            # 2) 시트 개수 및 이름 추출 (xl/workbook.xml)
            if 'xl/workbook.xml' in zip_ref.namelist():
                root = etree.fromstring(zip_ref.read('xl/workbook.xml'))
                ns = {'ns': root.nsmap[None]}  # 기본 네임스페이스
                sheets = root.findall('.//ns:sheets/ns:sheet', ns)
                sheet_count = len(sheets)

            # 3) 시트별 데이터 분석 (xl/worksheets/sheetN.xml)
            sheet_files = [f for f in zip_ref.namelist() if f.startswith('xl/worksheets/sheet') and f.endswith('.xml')]
            for sheet_file in sheet_files:
                sheet_root = etree.fromstring(zip_ref.read(sheet_file))
                ns = {'ns': sheet_root.nsmap[None]}

                rows = sheet_root.findall('.//ns:row', ns)
                total_rows += len(rows)

                # 각 행에서 셀 찾기
                for row in rows:
                    cells = row.findall('ns:c', ns)
                    total_cells += len(cells)
                    for cell in cells:
                        # 셀의 타입이 'inlineStr' 혹은 'str'이면 텍스트 추출
                        cell_type = cell.get('t')
                        if cell_type in ('inlineStr', 'str'):
                            is_elem = cell.find('ns:is', ns)
                            if is_elem is not None:
                                text_elems = is_elem.findall('.//ns:t', ns)
                                for t in text_elems:
                                    text = t.text or ''
                                    total_text_characters += len(text)
                                    total_text_words += len(re.findall(r'\w+', text))
                        # 타입이 's' (공유 문자열)인 경우 추출하려면 sharedStrings.xml을 파싱해야 함 (추가 가능)

                # 열 수 계산: 각 행에서 최대 컬럼 인덱스 확인
                max_col = 0
                for row in rows:
                    cells = row.findall('ns:c', ns)
                    for cell in cells:
                        ref = cell.get('r')  # 셀 참조 예: A1, B2, ...
                        if ref:
                            col_letters = re.match(r'([A-Z]+)', ref).group(1)
                            col_num = 0
                            for i, char in enumerate(reversed(col_letters)):
                                col_num += (ord(char) - ord('A') + 1) * (26 ** i)
                            if col_num > max_col:
                                max_col = col_num
                total_columns = max(total_columns, max_col)

            # 4) 이미지 개수 추출 (xl/drawings/drawingN.xml)
            drawing_files = [f for f in zip_ref.namelist() if f.startswith('xl/drawings/drawing') and f.endswith('.xml')]
            for drawing_file in drawing_files:
                drawing_root = etree.fromstring(zip_ref.read(drawing_file))
                nsmap = drawing_root.nsmap
                # <xdr:pic> 태그 개수 세기 (이미지)
                pic_count = len(drawing_root.findall('.//{http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing}pic'))
                image_count += pic_count

        if sheet_count > 0:
            metadata_dict["sheet_count"] = sheet_count
        if total_rows > 0:
            metadata_dict["total_rows"] = total_rows
        if total_columns > 0:
            metadata_dict["total_columns"] = total_columns
        if total_cells > 0:
            metadata_dict["total_cells"] = total_cells
        if total_text_characters > 0:
            metadata_dict["total_text_characters"] = total_text_characters
        if total_text_words > 0:
            metadata_dict["total_text_words"] = total_text_words
        if image_count > 0:
            metadata_dict["image_count"] = image_count

        return metadata_dict
from hwp5 import filestructure as FS
from hwp5.filestructure import Hwp5File
from hwp5.storage.ole import OleStorage
import xml.etree.ElementTree as ET
import zipfile
import olefile
import zlib
import struct
import re
import unicodedata

class HwpMetadataExtractor:
    FILE_HEADER_SECTION = "FileHeader"
    HWP_SUMMARY_SECTION = "\x05HwpSummaryInformation"
    SECTION_NAME_LENGTH = len("Section")
    BODYTEXT_SECTION = "BodyText"
    HWP_TEXT_TAGS = [67]

    def __init__(self):
        self._compressed = None
        self._valid = None
        self._ole = None

    def is_hwp(self, file_path):

        if file_path.endswith(".hwp"):
            return True
        else:
            if file_path.endswith(".hwpx"):
                if zipfile.is_zipfile(file_path):
                    return False
                return None
            else:
                return True


    def get_metadata(self, file_path) -> dict:
        if self.is_hwp(file_path):
            return self.extract_metadata(file_path)
        return self.extract_hwpx_metadata(file_path)

    def get_hwp_word_count(self, bodytext : Hwp5File):
        character_count = 0
        word_count = 0

        try:
            sections = getattr(bodytext, "section_list", [])
        except Exception as e:
            print(f"[오류] bodytext 접근 실패: {e}")
            return 0, 0

        for section in sections:
            paragraphs = getattr(section, "paragraph_list", [])
            for paragraph in paragraphs:
                # 텍스트가 없는 경우 스킵
                if not hasattr(paragraph, "text") or paragraph.text is None:
                    continue

                line_segments = getattr(paragraph.text, "line_segment_list", [])
                for line_seg in line_segments:
                    text = getattr(line_seg, "text", "")
                    if not isinstance(text, str):
                        continue  # 텍스트가 문자열이 아니면 무시
                    character_count += len(text)
                    word_count += len(text.split())

        return character_count, word_count

    def get_plaintext_lines(self, summary):
        from hwp5.msoleprops import PropertySetStreamTextFormatter
        stream = summary.getPropertySetStream
        formatter = PropertySetStreamTextFormatter()
        return list(formatter.formatTextLines(stream))

    def extract_metadata(self, file_path) -> dict:
        olestg = OleStorage(file_path)
        hwp5file = FS.Hwp5File(olestg)
        summary: FS.HwpSummaryInfo = hwp5file.summaryinfo

        metadata = {}
        if summary.title is not None and len(summary.title) > 0:
            metadata["title"] = summary.title
        if summary.subject is not None and len(summary.subject) > 0:
            metadata["subject"] = summary.subject
        if summary.author is not None and len(summary.author) > 0:
            metadata["author"] = summary.author
        if summary.dateString is not None:
            metadata["date"] = summary.dateString
        if summary.keywords is not None and len(summary.keywords) > 0:
            metadata["keywords"] = summary.keywords
        if summary.comments is not None and len(summary.comments) > 0:
            metadata["comments"] = summary.comments
        if summary.lastSavedBy is not None and len(summary.lastSavedBy) > 0:
            metadata["last_author"] = summary.lastSavedBy
        if summary.createdTime is not None:
            metadata["create_dtm"] = summary.createdTime
        if summary.lastSavedTime is not None:
            metadata["last_save_dtm"] = summary.lastSavedTime
        if summary.lastPrintedTime is not None:
            metadata["last_printed"] = summary.lastPrintedTime
        if summary.numberOfPages is not None:
            metadata["page_count"] = summary.numberOfPages
        if summary.numberOfParagraphs is not None:
            metadata["paragraph_count"] = summary.numberOfParagraphs

        plaintext_lines = self.get_plaintext_lines(summary)
        if plaintext_lines is not None:
            metadata["line_count"] = len(plaintext_lines)

        character_count, word_count = self.get_hwp_word_count(hwp5file.bodytext)
        metadata["character_count"] = character_count
        metadata["word_count"] = word_count

        return metadata


    def extract_hwpx_metadata(self, file_path):
        # HWPX 파일을 ZIP 형식으로 열기
        metadata_dict = {}
        section_files = []
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            # 메타데이터가 포함된 XML 파일 읽기 (보통 meta.xml 파일)
            for file in zip_ref.filelist:
                if file.filename == "Contents/content.hpf":
                    with zip_ref.open("Contents/content.hpf") as content_file:
                        tree = ET.parse(content_file)
                        root = tree.getroot()
                        # XML 네임스페이스 정의
                        metadata_ns = {
                            'opf': 'http://www.idpf.org/2007/opf/',
                            'dc': 'http://purl.org/dc/elements/1.1/'
                        }
                        metadata = root.find('opf:metadata', metadata_ns)
                        if metadata is not None:
                            # 메타데이터 내부의 모든 항목을 순회
                            for meta in metadata:
                                # 태그 이름에서 네임스페이스 제거
                                tag = meta.tag.split('}')[1] if '}' in meta.tag else meta.tag

                                # 속성이 있는 경우
                                if meta.attrib:
                                    # 속성의 name이 있는 경우 그 값을 key로 사용
                                    if 'name' in meta.attrib:
                                        key = meta.attrib['name']
                                        metadata_dict[key] = meta.text
                                    else:
                                        metadata_dict[tag] = meta.text
                                else:
                                    metadata_dict[tag] = meta.text
                        else:
                            print("Metadata not found.")
                elif file.filename.startswith("Contents/section") and file.filename.endswith(".xml"):
                    section_files.append(file.filename)

            para_count = 0
            char_count = 0
            word_count = 0
            image_count = 0
            object_count = 0
            line_count = 0
            section_ns = {'hp': 'http://www.hancom.co.kr/hwpml/2011/paragraph',
                          'hp10': 'http://www.hancom.co.kr/hwpml/2016/paragraph'}

            for section in section_files:
                with zip_ref.open(section) as file:
                    tree = ET.parse(file)
                    root = tree.getroot()

                # 문단 추출 (두 네임스페이스 모두 시도)
                for prefix in section_ns:
                    paras =  root.findall(f'.//{prefix}:para', section_ns)
                    if paras:
                        for para in paras:
                            para_count += 1
                            for run in para.findall(f'.//{prefix}:run', section_ns):
                                text_elem = run.find(f'{prefix}:t', section_ns)
                                if text_elem is not None and text_elem.text:
                                    text = text_elem.text
                                    char_count += len(text)
                                    word_count += len(text.split())
                    else:
                        for run in root.findall(f'.//{prefix}:run', section_ns):
                            text_elem = run.find(f'{prefix}:t', section_ns)
                            if text_elem is not None and text_elem.text:
                                text = text_elem.text
                                char_count += len(text)
                                word_count += len(text.split())

                    # 이미지 개수
                    image_count += len(root.findall(f'.//{prefix}:pic', section_ns))

                    # 도형 개수
                    object_count = len(root.findall(f'.//{prefix}:shape', section_ns))

                    #라인 수 추출
                    line_count += len(root.findall(f'.//{prefix}:lineseg', section_ns))

            if para_count > 0:
                metadata_dict["paragraph_count"] = para_count
            if char_count > 0:
                metadata_dict["character_count"] = char_count
            if word_count > 0:
                metadata_dict["word_count"] = word_count
            if image_count > 0:
                metadata_dict["image_count"] = image_count
            if object_count > 0:
                metadata_dict["object_count"] = object_count
            if line_count > 0:
                metadata_dict["line_count"] = line_count

        return metadata_dict

    def get_sample_data(self, file_path, chunk_size: int = 1000):
        if self.is_hwp(file_path):
            return self.get_sample_data_from_hwp(file_path, chunk_size)
        return self.get_sample_data_from_hwpx(file_path, chunk_size)

    # 파일 불러오기
    def load(self, file_path):
        return olefile.OleFileIO(file_path)

    # hwp 파일인지 확인 header가 없으면 hwp가 아닌 것으로 판단하여 진행 안함
    def is_valid(self, dirs):
        if [self.FILE_HEADER_SECTION] not in dirs:
            return False

        return [self.HWP_SUMMARY_SECTION] in dirs

    # 문서 포맷 압축 여부를 확인
    def is_compressed(self, _ole):
        header = _ole.openstream("FileHeader")
        header_data = header.read()
        return (header_data[36] & 1) == 1

    # bodytext의 section들 목록을 저장
    def get_body_sections(self, dirs):
        m = []
        for d in dirs:
            if d[0] == self.BODYTEXT_SECTION:
                m.append(int(d[1][self.SECTION_NAME_LENGTH:]))

        return ["BodyText/Section" + str(x) for x in sorted(m)]

    # text 추출
    def get_sample_data_from_hwp(self, file_path, chunk_size):
        _ole = self.load(file_path)
        _dirs = _ole.listdir()
        _valid = self.is_valid(_dirs)
        if not _valid:
            raise Exception("Not Valid HwpFile")

        _compressed = self.is_compressed(_ole)
        sections = self.get_body_sections(_dirs)
        text = ""
        for section in sections:
            text += self.get_text_from_section(_ole, _compressed, section)
            text += "\n"

        if chunk_size < 0:
            return text

        return text[0:chunk_size]

    # section 내 text 추출
    def get_text_from_section(self, _ole, is_compressed, section):
        bodytext = _ole.openstream(section)
        data = bodytext.read()

        unpacked_data = zlib.decompress(data, -15) if is_compressed else data
        size = len(unpacked_data)

        i = 0

        text = ""
        while i < size:
            header = struct.unpack_from("<I", unpacked_data, i)[0]
            rec_type = header & 0x3ff
            level = (header >> 10) & 0x3ff
            rec_len = (header >> 20) & 0xfff

            if rec_type in self.HWP_TEXT_TAGS:
                rec_data = unpacked_data[i + 4:i + 4 + rec_len]

                ############## 정제 추가된 부분 #############
                decode_text = rec_data.decode('utf-16')
                # 문자열을 담기 전 정제하기
                res = remove_control_characters(remove_chinese_characters(decode_text))

                text += res
                text += "\n"

            i += 4 + rec_len

        return text

    def get_sample_data_from_hwpx(self, file_path, chunk_size: int = 1000):
        extracted_text = ""
        try:
            # HWPX 파일 열기
            with zipfile.ZipFile(file_path, 'r') as z:
                # Contents/ 디렉터리의 Section*.xml 파일 찾기
                section_files = [f for f in z.namelist() if f.startswith("Contents/section") and f.endswith(".xml")]

                for section_file in section_files:
                    # XML 파일 읽기
                    with z.open(section_file) as file:
                        tree = ET.parse(file)
                        root = tree.getroot()

                        # 텍스트 추출 (hwp:paragraph 태그 안의 텍스트)
                        for para in root.findall(".//{http://www.hancom.co.kr/hwpml/2011/paragraph}p"):
                            texts = para.findall(".//{http://www.hancom.co.kr/hwpml/2011/paragraph}t")
                            for text in texts:
                                extracted_text += text.text if text.text else ""
                            extracted_text += "\n"  # 문단 구분
                            if 0 < chunk_size < len(extracted_text):
                                break
        except Exception as e:
            print(f"오류 발생: {e}")

        return extracted_text

#################### 텍스트 정제 함수 #######################
# 중국어 제거
def remove_chinese_characters(s: str):
    return re.sub(r'[\u4e00-\u9fff]+', '', s)


# 바이트 문자열 제거
def remove_control_characters(s):
    return "".join(ch for ch in s if unicodedata.category(ch)[0] != "C")


def gettext(path: str):

    f = olefile.OleFileIO(path)

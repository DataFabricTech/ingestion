from hwp5 import filestructure as FS
from hwp5.storage.ole import OleStorage
import xml.etree.ElementTree as ET
import zipfile
import olefile
import zlib
import struct
#### 추가 ####
import re
import unicodedata


class HwpMetadataExtractor:
    FILE_HEADER_SECTION = "FileHeader"
    HWP_SUMMARY_SECTION = "\x05HwpSummaryInformation"
    SECTION_NAME_LENGTH = len("Section")
    BODYTEXT_SECTION = "BodyText"
    HWP_TEXT_TAGS = [67]

    def __init__(self, file_path: str):
        self._compressed = None
        self._valid = None
        self._ole = None
        self.file_path = file_path

    def extract_metadata(self) -> dict:
        olestg = OleStorage(self.file_path)
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
        return metadata

    def extract_hwpx_metadata(self):
        # HWPX 파일을 ZIP 형식으로 열기
        metadata_dict = {}
        with zipfile.ZipFile(self.file_path, 'r') as zip_ref:
            # 메타데이터가 포함된 XML 파일 읽기 (보통 meta.xml 파일)
            for file in zip_ref.filelist:
                if file.filename == "Contents/content.hpf":
                    with zip_ref.open("Contents/content.hpf") as content_file:
                        tree = ET.parse(content_file)
                        root = tree.getroot()
                        # XML 네임스페이스 정의
                        ns = {
                            'opf': 'http://www.idpf.org/2007/opf/',
                            'dc': 'http://purl.org/dc/elements/1.1/'
                        }
                        metadata = root.find('opf:metadata', ns)
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
        return metadata_dict

    def get_text_hwpx(self, chunk_size: int = 1000):
        # HWPX 파일을 ZIP 형식으로 열기
        with zipfile.ZipFile(self.file_path, 'r') as zip_ref:
            # text 파일을 찾아서
            for file in zip_ref.filelist:
                if file.filename == "Preview/PrvText.txt":
                    with zip_ref.open("Preview/PrvText.txt") as preview_file:
                        return preview_file.read().decode('utf-8')[:chunk_size]
        return ""

    # 파일 불러오기
    def load(self):
        return olefile.OleFileIO(self.file_path)

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

    # 전체 text 추출
    def get_text(self, chunk_size):
        _ole = self.load()
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

#################### 텍스트 정제 함수 #######################
# 중국어 제거
def remove_chinese_characters(s: str):
    return re.sub(r'[\u4e00-\u9fff]+', '', s)


# 바이트 문자열 제거
def remove_control_characters(s):
    return "".join(ch for ch in s if unicodedata.category(ch)[0] != "C")

# if __name__ == "__main__":
    # extractor = HwpMetadataExtractor("/Users/jblim/2023년도_연차보고서_임준범_231222-1.hwp")
    # metadatas = extractor.extract_metadata()
    # for key, value in metadatas.items():
    #     print(f"{key}: {value}")
    # text = extractor._get_text()
    # print(text)
    # extractor = HwpMetadataExtractor("/Users/jblim/hello world.hwpx")
    # preview_text = extractor.get_text_hwpx()
    # print(f'{preview_text}')


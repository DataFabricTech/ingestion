# Copyright 2024 Mobigen
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Notice!
# This software is based on https://open-metadata.org and has been modified accordingly.

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

from metadata.ml.summarization import Summarization


class HwpMetadataExtractor:
    FILE_HEADER_SECTION = "FileHeader"
    HWP_SUMMARY_SECTION = "\x05HwpSummaryInformation"
    SECTION_NAME_LENGTH = len("Section")
    BODYTEXT_SECTION = "BodyText"
    HWP_TEXT_TAGS = [67]

    def __init__(self, file_path: str):
        self.summarizer = Summarization()
        self._compressed = None
        self._valid = None
        self._ole = None
        self.file_path = file_path

        if zipfile.is_zipfile(file_path):
            self.is_zip = True
        else:
            self.is_zip = False

        if file_path.endswith(".hwp"):
            self.is_hwp = True
        elif file_path.endswith(".hwpx") and self.is_zip:
            self.is_hwp = False
        else:
            self.is_hwp = True

    def get_metadata(self) -> dict:
        if self.is_hwp:
            return self.extract_metadata()
        return self.extract_hwpx_metadata()

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

        sample_data = self.get_sample_data(-1)
        if sample_data is not None:
            str_summary = self.summarizer.summarize(sample_data)
            metadata["Summary"] = str_summary

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

        sample_data = self.get_sample_data(-1)
        if sample_data is not None:
            str_summary = self.summarizer.summarize(sample_data)
            metadata_dict["Summary"] = str_summary

        return metadata_dict

    def get_sample_data(self, chunk_size: int = 1000):
        if self.is_hwp:
            return self.get_sample_data_from_hwp(chunk_size)
        return self.get_sample_data_from_hwpx(chunk_size)

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

    # text 추출
    def get_sample_data_from_hwp(self, chunk_size):
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

    def get_sample_data_from_hwpx(self, chunk_size: int = 1000):
        extracted_text = ""
        try:
            # HWPX 파일 열기
            with zipfile.ZipFile(self.file_path, 'r') as z:
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

# if __name__ == "__main__":
#     summary = Summarization()
#
#     extractor = HwpMetadataExtractor("/Users/jblim/Downloads/사업타당성_검토.hwp")
#     result = summary.summarize(extractor.get_sample_data(-1))
#     print(result)
#     # metadatas = extractor.extract_metadata()
#     # for key, value in metadatas.items():
#     #     print(f"{key}: {value}")
#
#     extractor = HwpMetadataExtractor("/Users/jblim/Downloads/뉴스.hwpx")
#     sampledata = extractor.get_sample_data(-1)
#     result = summary.summarize(sampledata)
#     print(result)
#

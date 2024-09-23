from hwp5 import filestructure as FS
from hwp5.storage.ole import OleStorage
import xml.etree.ElementTree as ET
import zipfile


class HwpMetadataExtractor:
    def __init__(self, file_path: str):
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


# if __name__ == "__main__":
#     extractor = HwpMetadataExtractor("/Users/jblim/2023년도_연차보고서_임준범_231222-1.hwp")
#     metadatas = extractor.extract_metadata()
#     for key, value in metadatas.items():
#         print(f"{key}: {value}")

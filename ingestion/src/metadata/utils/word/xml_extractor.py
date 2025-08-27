import xml.etree.ElementTree as ET

from metadata.ml.summarization import Summarization

class XmlMetadataExtractor:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.summarizer = Summarization()

    def count_elements(self, element, tag_counter):
        for child in element:
            tag_counter[child.tag] = tag_counter.get(child.tag, 0) + 1
            self.count_elements(child, tag_counter)

    def extract_metadata(self) -> dict:

        with open(self.file_path, 'r', encoding='utf-8') as f:
            raw_text = f.read()

        try:
            root = ET.fromstring(raw_text)
        except ET.ParseError:
            return {"error": "Invalid XML"}

        tag_counter = {root.tag: 1}
        self.count_elements(root, tag_counter)

        text_node_count = sum(1 for elem in root.iter() if (elem.text or '').strip())

        metadata = {
            "character_count": len(raw_text),
            "line_count": raw_text.count('\n') + 1,
            "tag_count": sum(tag_counter.values()),
            "tag_distinct_count": len(tag_counter),
            "root_tag": root.tag,
            "text_node_count": text_node_count
        }

        sample_data = self.get_sample_data(-1)
        if sample_data is not None:
            str_summary = self.summarizer.summarize(sample_data)
            metadata['summary'] = str_summary

        return metadata

    def get_sample_data(self, chunk_size: int = 1000) -> str:
        sample_text = ""
        with open(self.file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    sample_text += line
                    if 0 < chunk_size < len(sample_text):
                        break
        return sample_text


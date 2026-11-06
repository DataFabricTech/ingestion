import xml.etree.ElementTree as ET

class XmlMetadataExtractor:

    def count_elements(self, element, tag_counter):
        for child in element:
            tag_counter[child.tag] = tag_counter.get(child.tag, 0) + 1
            self.count_elements(child, tag_counter)

    def get_metadata(self, file_path) -> dict:

        with open(file_path, 'r', encoding='utf-8') as f:
            raw_text = f.read()

        try:
            root = ET.fromstring(raw_text)
        except ET.ParseError:
            return {"error": "Invalid XML"}

        tag_counter = {root.tag: 1}
        self.count_elements(root, tag_counter)

        text_node_count = 0
        word_count = 0
        for elem in root.iter():
            text = (elem.text or '').strip()
            if text:
                text_node_count += 1
                word_count += len(text.split())

        metadata = {
            "character_count": len(raw_text),
            "line_count": raw_text.count('\n') + 1,
            "tag_count": sum(tag_counter.values()),
            "tag_distinct_count": len(tag_counter),
            "root_tag": root.tag,
        }
        if text_node_count > 0:
            metadata["text_node_count"] = text_node_count
        if word_count > 0:
            metadata["word_count"] = word_count


        return metadata

    def get_sample_data(self, file_path, chunk_size: int = 1000) -> str:
        sample_text = ""
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    sample_text += line
                    if 0 < chunk_size < len(sample_text):
                        break
        return sample_text


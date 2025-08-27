import json

from metadata.ml.summarization import Summarization

class JsonMetadataExtractor:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.summarizer = Summarization()

    def extract_metadata(self) -> dict:

        with open(self.file_path, 'r', encoding='utf-8') as f:
            raw_text = f.read()
            # try:
            #     data = json.loads(raw_text)
            # except json.JSONDecodeError:
            #     return {"error": "Invalid JSON format"}

        # 줄 수, 문자 수, 단어 수
        line_count = raw_text.count('\n') + 1
        char_count = len(raw_text)
        word_count = len(raw_text.split())

        # JSON 구조 정보
        # if isinstance(data, dict):
        #     top_level_keys = len(data)    #최상위 항목 수
        #     top_level_type = "dict"       #최상위 자료형
        # elif isinstance(data, list):
        #     top_level_keys = len(data)
        #     top_level_type = "list"
        # else:
        #     top_level_keys = 1
        #     top_level_type = type(data).__name__

        metadata = {
            'character_count' : char_count,
            'word_count' : word_count,
            'line_count' : line_count,
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
                if line.strip():  # 공백 줄은 제외
                    sample_text += line
                    if 0 < chunk_size < len(sample_text):
                        break
        return sample_text


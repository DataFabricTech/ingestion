"""
json은 정형데이터로 처리되어 이 클래스는 사용되지 않는다.
"""
class JsonMetadataExtractor:

    def get_metadata(self, file_path) -> dict:

        with open(file_path, 'r', encoding='utf-8') as f:
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

        return metadata

    def get_sample_data(self, file_path, chunk_size: int = 1000) -> str:
        sample_text = ""
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():  # 공백 줄은 제외
                    sample_text += line
                    if 0 < chunk_size < len(sample_text):
                        break
        return sample_text



class TxtMetadataExtractor:

    def get_metadata(self, file_path) -> dict:

        with open(file_path, 'r', encoding='utf-8') as f:
            text = f.read()

        metadata = {
            'character_count' : len(text),
            'word_count' : len(text.split()),
            'line_count' : len(text.splitlines()),
            'paragraph_count' : len([p for p in text.split('\n\n') if p.strip()])
        }

        return metadata

    def get_sample_data(self, file_path, chunk_size: int = 1000) -> str:
        sample_text = ""
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():  # 비어 있지 않은 줄만 추가
                    sample_text += line
                    if 0 < chunk_size < len(sample_text):
                        break
        return sample_text

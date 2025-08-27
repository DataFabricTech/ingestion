from metadata.ml.summarization import Summarization

class TxtMetadataExtractor:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.summarizer = Summarization()

    def extract_metadata(self) -> dict:

        with open(self.file_path, 'r', encoding='utf-8') as f:
            text = f.read()

        metadata = {
            'character_count' : len(text),
            'word_count' : len(text.split()),
            'line_count' : len(text.splitlines()),
            'paragraph_count' : len([p for p in text.split('\n\n') if p.strip()])
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
                if line.strip():  # 비어 있지 않은 줄만 추가
                    sample_text += line
                    if 0 < chunk_size < len(sample_text):
                        break
        return sample_text

from pydub.utils import mediainfo

class PydubMetadataExtractor:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def extract_metadata(self) -> dict:

        metadata = {}
        info = mediainfo(self.file_path)

        if len(info) > 0:
            for k, v in info.items():

                if k == 'DISPOSITION':
                    for d_k, d_v in v.items():
                        metadata[f"disposition.{d_k}"] = d_v
                elif k == 'TAG':
                    for t_k, t_v in v.items():
                        metadata[f"tag.{t_k}"] = t_v
                else:
                    metadata[k] = v

        return metadata

import OpenEXR

class ExrMetadataExtractor:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def extract_metadata(self) -> dict:

        metadata = {}
        exr_file = OpenEXR.InputFile(self.file_path)
        header = exr_file.header()

        for key, value in header.items():
            metadata[key] = value

        return metadata





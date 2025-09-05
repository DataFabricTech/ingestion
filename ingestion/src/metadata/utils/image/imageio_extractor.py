import imageio.v2 as imageio

class ImageioMetadataExtractor:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def extract_metadata(self) -> dict:

        metadata = {}
        hdr = imageio.imread(self.file_path)

        metadata['shape'] = getattr(hdr, 'shape', None)
        metadata['dtype'] = getattr(hdr, 'dtype', None)

        return metadata





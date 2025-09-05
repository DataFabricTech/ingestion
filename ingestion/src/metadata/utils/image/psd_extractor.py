from psd_tools import PSDImage

class PsdMetadataExtractor:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def extract_metadata(self) -> dict:

        metadata = {}
        psd = PSDImage.open(self.file_path)

        metadata['bbox'] = getattr(psd, 'bbox', None)
        metadata['bottom'] = getattr(psd, 'bottom', None)
        metadata['channels'] = getattr(psd, 'channels', None)
        metadata['color_mode'] = getattr(psd, 'color_mode', None)
        metadata['compatibility_mode'] = getattr(psd, 'compatibility_mode', None)
        metadata['depth'] = getattr(psd, 'depth', None)
        metadata['height'] = getattr(psd, 'height', None)
        metadata['imgae_resources'] = getattr(psd, 'image_resources', None)
        metadata['kind'] = getattr(psd, 'kind', None)
        metadata['left'] = getattr(psd, 'left', None)
        metadata['name'] = getattr(psd, 'name', None)
        metadata['offset'] = getattr(psd, 'offset', None)
        metadata['pil_mode'] = getattr(psd, 'pil_mode', None)
        metadata['right'] = getattr(psd, 'right', None)
        metadata['size'] = getattr(psd, 'size', None)
        metadata['tagged_blocks'] = getattr(psd, 'tagged_blocks', None)
        metadata['top'] = getattr(psd, 'top', None)
        metadata['version'] = getattr(psd, 'version', None)
        metadata['viewbox'] = getattr(psd, 'viewbox', None)
        metadata['visible'] = getattr(psd, 'visible', None)
        metadata['width'] = getattr(psd, 'width', None)

        return metadata





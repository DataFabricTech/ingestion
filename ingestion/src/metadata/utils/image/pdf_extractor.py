from pypdf import PdfReader

class PdfMetadataExtractor:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def extract_metadata(self) -> dict:

        metadata = {}
        reader = PdfReader(self.file_path)
        pdf_metadata = reader.metadata

        if "/Title" in pdf_metadata.keys():
            metadata["title"] = pdf_metadata.get("/Title")
        if "/Author" in pdf_metadata.keys():
            metadata["author"] = pdf_metadata.get("/Author")
        if "/Subject" in pdf_metadata.keys():
            metadata["subject"] = pdf_metadata.get("/Subject")
        if "/Keywords" in pdf_metadata.keys():
            metadata["keywords"] = pdf_metadata.get("/Keywords")
        if "/Creator" in pdf_metadata.keys():
            metadata["creator"] = pdf_metadata.get("/Creator")
        if "/Producer" in pdf_metadata.keys():
            metadata["producer"] = pdf_metadata.get("/Producer")
        if "/CreationDate" in pdf_metadata.keys():
            metadata["created"] = pdf_metadata.get("/CreationDate")
        if "/ModDate" in pdf_metadata.keys():
            metadata["modified"] = pdf_metadata.get("/ModDate")

        return metadata





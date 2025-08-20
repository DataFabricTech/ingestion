import nltk
import ssl
from nltk.data import find
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM

"""
eenzeenee/t5-base-korean-summarization
모델은 추상적 요약을 수행하는 딥러닝 기반 모델입니다.
긴 텍스트를 입력받아 핵심 내용을 새로운 문장으로 생성합니다.
"""
model = AutoModelForSeq2SeqLM.from_pretrained('eenzeenee/t5-base-korean-summarization')
tokenizer = AutoTokenizer.from_pretrained('eenzeenee/t5-base-korean-summarization')

class Summarization:
    prefix = "summarize: "
    pkgs = ["punkt", "punkt_tab"]

    def __init__(self):
        for pkg in self.pkgs:
            try:
                find(pkg)
            except LookupError:
                pkg_download(pkg)


    def summarize(self, text):
        inputs = [self.prefix + text]

        inputs = tokenizer(inputs, max_length=512, truncation=True, return_tensors="pt")
        output = model.generate(**inputs, num_beams=3, do_sample=True, min_length=10, max_length=100)
        decoded_output = tokenizer.batch_decode(output, skip_special_tokens=True)[0]
        result = nltk.sent_tokenize(decoded_output.strip())[0]

        return result

def pkg_download(pkg_name):
    try:
        _create_default_https_context = ssl._create_default_https_context
        ssl._create_default_https_context = ssl._create_unverified_context
        nltk.download(pkg_name)
    finally:
        ssl._create_default_https_context = _create_default_https_context

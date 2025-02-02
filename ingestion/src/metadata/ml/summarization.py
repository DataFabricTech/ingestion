# Copyright 2024 Mobigen
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Notice!
# This software is based on https://open-metadata.org and has been modified accordingly.

import nltk
import ssl
from nltk.data import find
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM

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

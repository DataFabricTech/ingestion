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

# prediction of the type of a column
#     columns: List[Column]

def type_prediction(
    data: DatalakeColumnWrapper, column: Column, sample_size: int = 100
) -> str:
    """
    Predict the type of a column based on the sample data
    """
    import pandas as pd  # pylint: disable=import-outside-toplevel

    if data.dataframes is None:
        raise ValueError("Dataframe is empty")

    df = pd.concat(data.dataframes)
    df = df[column.name].dropna()
    if df.empty:
        return "string"

    if column.data_type is not None:
        return column.data_type

    if column.data_type is None:
        if df.dtype == "object":
            return "string"
        if df.dtype == "int64":
            return "int"
        if df.dtype == "float64":
            return "float"
        if df.dtype == "bool":
            return "boolean"
        if df.dtype == "datetime64":
            return "datetime"
        if df.dtype == "timedelta64":
            return "timedelta"
        if df.dtype == "category":
            return "category"
        if df.dtype == "period":
            return "period"
        if df.dtype == "complex":
            return "complex"
        if df.dtype == "object":
            return "string"
        if df.dtype == "int32":
            return "int"
        if df.dtype == "float32":
            return "float"
        if df.dtype == "int16":
            return "int"
        if df.dtype == "int8":
            return "int"
        if df.dtype == "uint8":
            return "int"
        if df.dtype == "uint16":
            return "int"
        if df.dtype == "uint32":
            return "int"
        if df.dtype == "uint64":
            return "int"
        if df.dtype == "datetime64[ns]":
            return "datetime"
        if df.dtype == "datetime64[ns, UTC]":
            return "datetime"
        if df.dtype == "datetime64[ns, pytz.FixedOffset]":
            return "datetime"
        if df.dtype == "datetime64[ns, tz]":
            return "datetime"

# import torch
# from transformers import BertTokenizer, BertForSequenceClassification
# from transformers import TextClassificationPipeline
#
# # KoBERT 모델과 토크나이저 로드
# model_name = "monologg/kobert"
# tokenizer = BertTokenizer.from_pretrained(model_name)
# model = BertForSequenceClassification.from_pretrained(model_name, num_labels=5)
#
# # 텍스트 분류 파이프라인 설정
# pipeline = TextClassificationPipeline(model=model, tokenizer=tokenizer, framework='pt')
#
# # 예제 데이터
# data = [
#     '1', '2', '3', '4', '1.1', '2.2', '2023-06-25', '2024-12-31',
#     '2023-06-25 14:35:00', '2024-12-31 23:59:59', '안녕하세요', '세계'
# ]
#
# # 각 텍스트의 타입 예측
# predictions = pipeline(data)
#
# # 예측 결과 출력
# for text, pred in zip(data, predictions):
#     print(f"텍스트: {text}, 예측 타입: {pred['label']}")
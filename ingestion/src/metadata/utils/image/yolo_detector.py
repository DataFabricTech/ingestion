import cv2
import base64

"""
 YOLOv8 모델 
| 모델명         | 파라미터 수 | 속도    | 정확도    | 용도            |
| ------------ | ---------| ------- | ------- | -------------- |
| `yolov8n.pt` | 가장 적음  | 매우 빠름 | 낮은 편   | 모바일, 실시간 추론 |
| `yolov8s.pt` | 적음      | 빠름     | 중간     | 일반 추론         |
| `yolov8m.pt` | 중간      | 평균     | 중간 이상 | 균형형            |
| `yolov8l.pt` | 많음      | 느림     | 높음     | 정확도 우선        |
| `yolov8x.pt` | 가장 많음  | 느림     | 매우 높음 | 고정밀 모델링      |
"""
class YoloDetector:
    def __init__(self, file_path: str, model):
        self.file_path = file_path
        self.model = model

    def detected_objects(self):

        # # YOLOv8 모델 불러오기 (사전 학습된 모델)
        # model = YOLO("yolov8s.pt")  # 또는 yolov8s.pt, yolov8m.pt 등

        detected_objects = None
        detected_objects_image = None

        try:
            # 이미지 불러오기
            image = cv2.imread(self.file_path)

            if image is not None:
                # 객체 탐지 수행
                # 이미지를 320 * 320 으로 줄여서 (기본 640) 탐지 -> 속도 개선
                results = self.model(image, imgsz=320)

                # 탐지된 객체 정보 출력
                detected_objects = []
                for box in results[0].boxes:
                    # 클래스 ID → 이름
                    cls_id = int(box.cls[0])
                    class_name = self.model.names[cls_id]
                    # 신뢰도
                    confidence = float(box.conf[0])
                    # 바운딩 박스 좌표 (x1, y1, x2, y2)
                    x1, y1, x2, y2 = map(int, box.xyxy[0].tolist())

                    # print(f"- {class_name} ({confidence:.2f}) 위치: ({x1}, {y1}), ({x2}, {y2})")
                    detected_objects.append({
                        "class" : class_name,
                        "confidence" : f"{confidence:.2f}",
                        "bbox" : f"({x1}, {y1}), ({x2}, {y2})"
                    })


                detected_objects_image = None
                if len(detected_objects) > 0:
                    # 탐지 결과 시각화 (bounding box 그리기)
                    annotated_frame = results[0].plot()

                    success, encoded_image = cv2.imencode('.jpeg', annotated_frame)

                    if success:
                        # 인코딩된 이미지를 base64로 변환
                        detected_objects_image = base64.b64encode(encoded_image).decode('utf-8')

            else:
                raise Exception(f"Cannot load image.")
        except Exception as e:
            raise Exception(f"Cannot detected image {e}")

        return detected_objects, detected_objects_image
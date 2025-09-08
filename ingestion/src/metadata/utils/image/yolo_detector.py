import cv2
from ultralytics import YOLO

class YoloDetector:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def detected_objects(self):

        # YOLOv8 모델 불러오기 (사전 학습된 모델)
        model = YOLO("yolov8n.pt")  # 또는 yolov8s.pt, yolov8m.pt 등

        # 이미지 불러오기
        image = cv2.imread(self.file_path)

        # 객체 탐지 수행
        results = model(image)

        # 탐지된 객체 정보 출력
        detected_objects = []
        for box in results[0].boxes:
            # 클래스 ID → 이름
            cls_id = int(box.cls[0])
            class_name = model.names[cls_id]
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

        return detected_objects
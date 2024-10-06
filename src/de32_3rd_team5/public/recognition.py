import cv2
import streamlit as st
from streamlit_webrtc import webrtc_streamer, VideoHTMLAttributes
import numpy as np
import av
import datetime
import face_recognition  # 얼굴 인식 라이브러리 추가

st.title("recognition")

face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')

save_faces = False
detected_faces_encodings = []  # 저장된 얼굴의 인코딩 목록

def is_duplicate(face_encoding):
    """Check if the current face encoding is already detected and saved."""
    if len(detected_faces_encodings) == 0:
        return False
    
    # 얼굴 인코딩을 비교하여, 일정 임계값 이상으로 유사한 얼굴이 있는지 판별
    matches = face_recognition.compare_faces(detected_faces_encodings, face_encoding, tolerance=0.6)
    
    return any(matches)

def transform(frame: av.VideoFrame):
    global save_faces

    img = frame.to_ndarray(format="bgr24")
    rgb_img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)

    # 얼굴 인식 및 인코딩 생성
    face_locations = face_recognition.face_locations(rgb_img)
    face_encodings = face_recognition.face_encodings(rgb_img, face_locations)

    for (face_encoding, face_location) in zip(face_encodings, face_locations):
        top, right, bottom, left = face_location
        cv2.rectangle(img, (left, top), (right, bottom), (255, 0, 0), 2)


        if save_faces and not is_duplicate(face_encoding):
            face_img = img[top:bottom, left:right]

            enlarged_face = cv2.resize(face_img, ((right - left) * 2, (bottom - top) * 2), interpolation=cv2.INTER_CUBIC)  # Enlarging the face image
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            face_filename = f"face_{timestamp}.jpg"
            
            cv2.imwrite(face_filename, enlarged_face)
            st.write(f"Face saved as: {face_filename}")
            detected_faces_encodings.append(face_encoding)  # 새로운 얼굴 인코딩 저장

    return av.VideoFrame.from_ndarray(img, format="bgr24")

save_faces = st.checkbox("Save Faces")

webrtc_streamer(
    key="streamer",
    video_frame_callback=transform,
    sendback_audio=False,
    video_html_attrs=VideoHTMLAttributes(
        autoPlay=True, controls=True, style={"width": "100%"}, playsinline=True
    ),
    media_stream_constraints={
        "video": {
            "width": {"ideal": 1920}, 
            "height": {"ideal": 1080},
            "facingMode": "user" 
        }
    }
)
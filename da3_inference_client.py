import json
import socket

payload = {
    "image_paths": ["path/to/img1.png", "path/to/img2.png"],
    "video_name": "videoA",
    "file_name": "clip001",
    "export_format": "glb-npz",
}

with socket.create_connection(("127.0.0.1", 8008)) as s:
    s.sendall((json.dumps(payload) + "\n").encode("utf-8"))
    response = s.recv(65536).decode("utf-8")
    print(response)
# https://python.langchain.com/v0.2/docs/how_to/multimodal_prompts/
# added persistent vectordb instead of in memory
# add dir struture https://fastapi.tiangolo.com/tutorial/bigger-applications/
# Fix https://community.deeplearning.ai/t/try-filtering-complex-metadata-from-the-document-using-langchain-community-vectorstores-utils-filter-complex-metadata/628474/4

import logging
from fastapi import FastAPI,File, UploadFile, BackgroundTasks
from typing import List
from pydantic import BaseModel
from fastapi.encoders import jsonable_encoder 
import os        
from helpers import generate_response, load_data
import socketio

logging.basicConfig(format="%(levelname)s     %(message)s", level=logging.INFO)
httpx_logger = logging.getLogger("httpx")
httpx_logger.setLevel(logging.WARNING)
logging.getLogger("uvicorn").setLevel(logging.WARNING)  # Set uvicorn to warning level
logging.getLogger("azure.core.pipeline.policies").setLevel(logging.WARNING)

# Initialize FastAPI app
app = FastAPI()

# Initialize Socket.IO server
sio_server = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins=[], transports=["websocket"])
sio_app = socketio.ASGIApp(socketio_server=sio_server, socketio_path="/socket.io")

connected_clients = set()

# Mount Socket.IO app at /socket.io
app.mount("/socket.io", sio_app)


@app.get("/")
async def root():
    return {"msg": "OK"}

# Socket.IO event handlers
@sio_server.event
async def connect(sid, environ):
    connected_clients.add(sid)
    logging.info(f"Client {sid} connected")
    await sio_server.emit('message', {'data': 'Connected'}, room=sid)

@sio_server.event
async def disconnect(sid):
    connected_clients.remove(sid)
    logging.info(f"Client {sid} disconnected")



# ==============================SOCKET EVENT FOR FILE UPLOAD =========================
@sio_server.event
async def upload_files(sid, data):
    evidence_id = data['evidence_id']
    files = data['files']

    await sio_server.emit('files_saved', {'msg': 'Files uploaded'}, room=sid)
    
    upload_folder = f"docs"
    os.makedirs(upload_folder, exist_ok=True)

    filenames = []
    for file in files:
        filename = file['filename'].replace(" ", "_")
        filename = f"{evidence_id}_{filename}"
        file_path = os.path.join(upload_folder, filename)
        filenames.append(filename)
        with open(file_path, "wb") as buffer:
            buffer.write(file['content'])
        logging.info(f"Saved file: {filename} at {file_path}")


    added_files = await load_data(filenames)

    if added_files:
        await sio_server.emit('processing_complete', {"status" : "Success", 'files': filenames, "attachment_id" : evidence_id, "saved_name" : added_files}, room=sid)
    else:
        await sio_server.emit('processing_complete', {"status" : "Failed", 'files': None, "attachment_id" : None}, room=sid)


class ProjectManagmentUpload(BaseModel):
    uid :str
    auditor_rfe: str
    name : str
    markup: bool

@app.post("/project-management/analyze-upload/{evidence_id}")
async def project_management_upload(evidence_id:str, data: ProjectManagmentUpload):
    data_doc = jsonable_encoder(data)
    rfe = data_doc['auditor_rfe']
    name = data_doc['name']
    name =  os.path.basename(name)
    uid = data_doc['uid']
    markup = data_doc['markup']
    file_name = f"{evidence_id}_{name}"

    logging.info(f"filename {file_name}")
    user_id = f"{uid}-{evidence_id}"
    response = await generate_response(user_id, file_name, rfe, markup)
    return response
    

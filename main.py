# https://python.langchain.com/v0.2/docs/how_to/multimodal_prompts/
# added persistent vectordb instead of in memory
# add dir struture https://fastapi.tiangolo.com/tutorial/bigger-applications/
# Fix https://community.deeplearning.ai/t/try-filtering-complex-metadata-from-the-document-using-langchain-community-vectorstores-utils-filter-complex-metadata/628474/4

import logging
from fastapi import FastAPI,File, UploadFile, BackgroundTasks, Request, HTTPException, Depends
from typing import List
from pydantic import BaseModel
from fastapi.encoders import jsonable_encoder 
import os        
from helpers import generate_response, load_data
import socketio
from cryptography.fernet import Fernet

logging.basicConfig(format="%(levelname)s     %(message)s", level=logging.INFO)
# hack to get rid of langchain logs
httpx_logger = logging.getLogger("httpx")
httpx_logger.setLevel(logging.WARNING)


logging.info(f"KEYYY {os.getenv("SECRET_KEY")} {os.getenv("AZURE_OPENAI_DEPLOYMENT_EMBEDDINGS")}")
# key = os.getenv("SECRET_KEY")
key = "4324324"
KEY = key.encode()
cipher_suite = Fernet(KEY)


app = FastAPI()


async def verify_request(request: Request):
    headers = request.headers
    auth_token = headers.get('Authorization') 
    
    if auth_token:
        try:
            token = auth_token.split(' ')[1]
            logging.info(f'Authorization token: {token}')
            
            decrypted_token = cipher_suite.decrypt(token.encode()).decode()
            
            # Compare decrypted token with expected value
            if decrypted_token != os.getenv("SECRET_TOKEN"):
                raise HTTPException(status_code=403, detail="Invalid token")
            else:
                logging.info("Valid Token")
                return
        except Exception as e:
            logging.error(f"Token decryption error: {e}")
            raise HTTPException(status_code=403, detail="Invalid token")
    else:
        raise HTTPException(status_code=400, detail="Authorization token missing")
    

@app.get("/")
async def root():
    return {"msg": "OK"}


@app.post("/test/{evidence_id}")
async def upload_files(evidence_id: str, headers: dict = Depends(verify_request)):
    logging.info(f"valid toek {evidence_id}")
    return "yess"

@app.post("/upload_files/{evidence_id}")
async def upload_files(background_tasks: BackgroundTasks, evidence_id: str, files: List[UploadFile] = File(...), headers: dict = Depends(verify_request)):
    upload_folder = f"docs"
    os.makedirs(upload_folder, exist_ok=True)

    print("attachment id", evidence_id)
    filenames = []
    for _file in files:
        filename = _file.filename.replace(" ", "_")
        filename = f"{evidence_id}_{filename}" 
        file_path = os.path.join(upload_folder, filename)
        filenames.append(filename)
        with open(file_path, "wb") as buffer:
            buffer.write(await _file.read())
        print(f"Saved file: {filename} at {file_path}")
        
    background_tasks.add_task(load_data, filenames)

    return {"Message": "Files Added"}


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


    print("file name", file_name)
    print("evidence id", evidence_id)
    print("original filename", name)

    user_id = f"{uid}-{evidence_id}"
    response = await generate_response(user_id, file_name, rfe, markup)
    return response
    

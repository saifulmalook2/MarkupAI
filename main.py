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
from helpers import generate_response, load_data, check_documents_exist
import socketio
from cryptography.fernet import Fernet

logging.basicConfig(format="%(levelname)s     %(message)s", level=logging.INFO)
# hack to get rid of langchain logs
httpx_logger = logging.getLogger("httpx")
httpx_logger.setLevel(logging.WARNING)

key = os.getenv("SECRET_KEY")
KEY = key.encode()
cipher_suite = Fernet(KEY)


app = FastAPI()


async def verify_request(request: Request):
    headers = request.headers
    auth_token = headers.get('Authorization') 
    
    if auth_token:
        try:
            token = auth_token.split(' ')[1]            
            decrypted_token = cipher_suite.decrypt(token.encode()).decode()
            
            # Compare decrypted token with expected value
            if decrypted_token != os.getenv("SECRET_TOKEN"):
                logging.info("Invalid Token")

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


class ProjectManagmentExist(BaseModel):
    name : str


@app.post("/project-management/check-upload/{evidence_id}")
async def document_exist(evidence_id:str, data: ProjectManagmentExist, headers: dict = Depends(verify_request)):
    data_doc = jsonable_encoder(data)
    name = data_doc['name']
    name =  os.path.basename(name)
    file_name = f"{evidence_id}_{name}"

    print("file name", file_name)

    response, msg = await check_documents_exist(file_name)

    logging.info(f"response {response}")
    return {"status" : response, "msg" : msg}
    

class ProjectManagmentUpload(BaseModel):
    uid :str
    auditor_rfe: str
    name : str
    markup: bool


@app.post("/project-management/analyze-upload/{evidence_id}")
async def project_management_upload(evidence_id:str, data: ProjectManagmentUpload, headers: dict = Depends(verify_request)):
    data_doc = jsonable_encoder(data)
    rfe = data_doc['auditor_rfe']
    name = data_doc['name']
    name =  os.path.basename(name)
    uid = data_doc['uid']
    markup = data_doc['markup']
    file_name = f"{evidence_id}_{name}"


    print("file name", file_name)

    user_id = f"{uid}-{evidence_id}"
    response = await generate_response(user_id, file_name, rfe, markup)
    return response
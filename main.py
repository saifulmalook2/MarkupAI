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

logging.basicConfig(format="%(levelname)s     %(message)s", level=logging.INFO)
# hack to get rid of langchain logs
httpx_logger = logging.getLogger("httpx")
httpx_logger.setLevel(logging.WARNING)


app = FastAPI()

@app.get("/")
async def root():
    return {"msg": "OK"}


@app.post("/upload_files/{evidence_id}")
async def upload_files(background_tasks: BackgroundTasks, evidence_id: str, files: List[UploadFile] = File(...)):
    upload_folder = f"temp_docs"
    os.makedirs(upload_folder, exist_ok=True)

    print("eveidence id", evidence_id)
    filenames = []
    for _file in files:
        filename = _file.filename.replace(" ", "_")
        filename = f"{evidence_id}_{filename}" 
        print("filename", filename)
        file_path = os.path.join(upload_folder, filename)
        filenames.append(filename)
        with open(file_path, "wb") as buffer:
            buffer.write(await _file.read())

        print(f"Saved file: {filename} at {file_path}")

    background_tasks.add_task(load_data, upload_folder)

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
    response = await generate_response(uid, file_name, rfe, markup)
    return response
    

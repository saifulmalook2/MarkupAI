## Overview 
Service to abstract away the AI processing, etc for peer reviewer


## Tech Stack 
- fastapi web service 
- aure open ai llm 
- hugging face embeddings 
- langchain
- have to install tesseract on system for image recognition

## Run tests 
python3 -m pytest tests/


## RUn app locally 
docker build -t name . 
docker run -p8000:8000 name 

or

run bash script "start.sh"
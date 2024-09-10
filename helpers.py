import os
import json
import fitz
import pathlib
from langchain.schema import Document
from langchain_openai import AzureChatOpenAI
from langchain_openai import AzureOpenAIEmbeddings
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_community.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.document_loaders import TextLoader
from langchain_community.document_loaders import CSVLoader
from langchain_community.document_loaders.image import UnstructuredImageLoader
from langchain.chains import create_history_aware_retriever
from langchain_core.messages import AIMessage, HumanMessage
from langchain.chains import create_retrieval_chain
from langchain.chains.combine_documents import create_stuff_documents_chain
import uuid
import openpyxl
from openpyxl import Workbook
from openpyxl.styles import PatternFill
import csv
import pandas as pd
from docx import Document as DocxDocument
from docx.enum.text import WD_COLOR_INDEX
# from langchain_community.vectorstores.azuresearch import AzureSearch
from langchain.schema import Document
from typing import List
from langchain_community.retrievers import AzureAISearchRetriever
from vector_db import AzureSearch
import boto3
import shutil
from pathlib import Path

def upload_to_space(origin, output, region_name='nyc3'):

    client = boto3.client(
        's3',
        region_name=region_name,
        endpoint_url=f'https://annotated-files.nyc3.digitaloceanspaces.com',
        aws_access_key_id=os.getenv("SPACES_ACCESS"),
        aws_secret_access_key=os.getenv("SPACES_SECRET")
    )
    
    try:
        client.upload_file(origin, "annotated-files", f"{output}", ExtraArgs={'ACL': 'public-read'})
        public_url = f'https://annotated-files.nyc3.digitaloceanspaces.com/annotated-files/{output}'

        os.remove(origin)
        return public_url
    
    except Exception as e:
        print("error while placing file in bucket", e)
        return None


async def highlight_text_in_pdf(input_path, output_path, page_contents):
    doc = fitz.open(input_path)
    anotated_rects = []
    anotated_texts = []

    def get_line_rect(page, text_instance):
        block = page.get_text("dict")['blocks']
        for b in block:
            if b['type'] == 0:
                for line in b['lines']:
                    for span in line['spans']:
                        if (span['bbox'][0] <= text_instance.x0 <= span['bbox'][2] and
                            span['bbox'][1] <= text_instance.y0 <= span['bbox'][3]):
                            return fitz.Rect(span['bbox'][0], line['bbox'][1], span['bbox'][2], line['bbox'][3])
        return None

    try:
        for page_num, text_list in page_contents.items():
            if page_num >= len(doc) + 1 or page_num < 0:
                print(f"Page number {page_num} is out of range.")
                continue

            page = doc[page_num - 1]
            for l in text_list:
                if l.strip():
                    text_instances = page.search_for(l)
                    if text_instances:
                        for inst in text_instances:
                            if (
                                inst not in anotated_rects
                            ):
                                line_rect = get_line_rect(page, inst)
                                if line_rect:
                                    annot = page.add_highlight_annot(line_rect)
                                    annot.update()
                                    anotated_rects.append(line_rect)
                                    anotated_texts.append(l)

        doc.save(output_path)
        doc.close()
        
    except Exception as e:
        print("Error while marking PDF", e)


async def highlight_text_in_xlsx(input_path, output_path, page_contents):
    workbook = openpyxl.load_workbook(input_path)
    for page_num, details in page_contents.items():
        sheet_name = details['sheet']
        texts_to_highlight = details['text']
        print("sheet", sheet_name, texts_to_highlight)

        if sheet_name in workbook.sheetnames:
            sheet = workbook[sheet_name]
            row = sheet[page_num]
            for cell in row:
                for text in texts_to_highlight:
                    if text not in ["", "nan"] and text.strip() == (str(cell.value)).strip():
                        print("match")
                        cell.fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

    workbook.save(output_path)


async def highlight_text_in_csv(csv_file_path, xlsx_file_path, index_dict):
    wb = Workbook()
    ws = wb.active

    highlight_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

    highlight_indices = set(index_dict.keys())
    # highlight_indices = [1,2,3,4,5]

    with open(csv_file_path, 'r', newline='') as csvfile:
        csvreader = csv.reader(csvfile)
        
        for index_csv, row in enumerate(csvreader):
            is_highlighted = index_csv in highlight_indices
            if is_highlighted:

                values = index_dict[index_csv]
                for col_val in row:
                    if any(val in col_val or col_val in val for val in values):
                        break

            ws.append(row)
            
            if is_highlighted:
                last_row = ws.max_row
                for cell in ws[last_row]:
                    cell.fill = highlight_fill

    wb.save(xlsx_file_path)

    print(f"CSV file has been written to {xlsx_file_path}")


async def highlight_text_in_docx(docx_file, output_file, index_dict):
    doc = DocxDocument(docx_file)
    group_num = list(index_dict.keys())

    for para_index in group_num:
        ending_index = 3
        starting_index = 0
        if para_index != 0:
            starting_index = para_index * 3
            ending_index = starting_index + 3
        for paragraph_index in range(starting_index, ending_index):

            para_text = doc.paragraphs[paragraph_index].text
            paragraph = doc.paragraphs[paragraph_index]

            for q_text in index_dict[para_index]:
                if q_text != "" and q_text in para_text:
                    highlighted_text = f"{q_text}"
                    para_text = para_text.replace(q_text, highlighted_text)

            paragraph.clear()  
            paragraph.add_run(para_text).font.highlight_color = WD_COLOR_INDEX.YELLOW

               
    doc.save(output_file)
    print(f"Highlighted document saved as {output_file}")
    print(f"Highlighted document saved as {output_file}")


async def docx_loader(file):
    docx = DocxDocument(file)
    documents_with_paragraphs = []
    current_group_content = []
    paragraph_group_number = 0

    for i, paragraph in enumerate(docx.paragraphs):
        # if paragraph.text.strip():  # Skip empty paragraphs
        current_group_content.append(paragraph.text)
        
        # Create a document for every 3 paragraphs
        if len(current_group_content) == 3:
            doc_with_group = Document(
                                        metadata={"source" : file, "page" : paragraph_group_number},
                                        id=str(uuid.uuid4()),
                                        page_content="\n".join(current_group_content)
                                        
                                    )

            documents_with_paragraphs.append(doc_with_group)
            current_group_content = []
            paragraph_group_number += 1
    
    # Handle remaining paragraphs
    if current_group_content:
        doc_with_group = Document(
                                    metadata={"source" : file, "paragraph_group_number" : paragraph_group_number},
                                    id=str(uuid.uuid4()),
                                    page_content="\n".join(current_group_content)
                                )
        documents_with_paragraphs.append(doc_with_group)

    print("Loaded documents from all paragraph groups:", len(documents_with_paragraphs))
    return documents_with_paragraphs


async def excel_loader(file):
    sheets = pd.read_excel(file, sheet_name=None)
    
    documents_with_rows = []
    
    for sheet_name, df in sheets.items():
        for i, row in df.iterrows():
            row_text = "\n".join(str(cell) for cell in row)
            doc_with_row = Document(
                metadata={
                    "sheet": sheet_name,   
                    "row": i + 1,          
                    "source": file         
                },
                page_content=row_text      
            )
            documents_with_rows.append(doc_with_row)
    
    print("Loaded documents from all sheets with row and column numbers:", len(documents_with_rows))
    return documents_with_rows


def delete_all_in_dir(directory):
    if os.path.exists(directory):
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            try:
                if os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                else:
                    os.remove(file_path)
            except Exception as e:
                print(f"Error deleting {file_path}: {e}")
    else:
        print(f"The directory {directory} does not exist.")


async def load_data(folder_path: str):
    print("Background task initiated")
    try:
        all_documents = []

        files = os.path.join(os.getcwd(), folder_path)
        for filename in os.listdir(files):
            try:
                file = os.path.abspath(os.path.join(str(files), str(filename)))
                print(f"Processing {file}")
                file_extension = pathlib.Path(file).suffix

                if file_extension == ".pdf":
                    raw_documents = PyPDFLoader(file, extract_images=True).load()
                    all_documents.extend(raw_documents)


                elif file_extension == ".xlsx":
                    print("Loading")
                    raw_documents = await excel_loader(file)
                    all_documents.extend(raw_documents)

                elif file_extension == ".csv":
                    raw_documents = CSVLoader(file_path=file).load()
                    all_documents.extend(raw_documents)

                elif file_extension == ".docx":
                    raw_documents = await docx_loader(file)
                    all_documents.extend(raw_documents)

                elif file_extension == ".txt":
                    raw_documents = TextLoader(file).load()
                    all_documents.extend(raw_documents)

                elif file_extension in [".jpg", ".jpeg", ".png"]:
                    raw_documents = UnstructuredImageLoader(file).load()
                    all_documents.extend(raw_documents)

                os.makedirs("docs", exist_ok=True)
                source_file = os.path.join("temp_docs", filename)
                destination_file = os.path.join("docs", filename)
                shutil.copy(source_file, destination_file)
                delete_all_in_dir("temp_docs")
            except Exception as e:
                print(f"Failed to process {filename}: {e}")

        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=300, chunk_overlap=50
        )
        texts = text_splitter.split_documents(all_documents)

        print("split")
        embedding = AzureOpenAIEmbeddings(
            model="text-embedding-ada-002",
            azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT_EMBEDDINGS"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT_EMBEDDINGS"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY_EMBEDDINGS"),
        )  

        print("embeddings fetched")
        vectordb = AzureSearch(
                azure_search_endpoint=os.getenv("AZURE_SEARCH_ENDPOINT"),
                azure_search_key=os.getenv("AZURE_SEARCH_KEY"),
                index_name="soc-index",  # Replace with your index name
                embedding_function=embedding.embed_query,
            )
        
        print("db fetched")

        # vectordb.add_documents(documents=texts)

        # vectors = embedding.embed_documents([text.page_content for text in texts])
        print("embeddings created")
        for text in texts:

            if "id" not in text:
                text.id = str(uuid.uuid4())

            text.metadata["source"] = text.metadata["source"].split("/")[-1]


            if "row" in text.metadata:
                text.metadata["page"] = text.metadata['row']
                del text.metadata["row"]

            if "sheet" not in text.metadata:
                text.metadata["sheet"] = ""

        print(texts[0])
        await vectordb.aadd_documents(documents=texts)

        # index.upsert(vectors=zip(ids, vectors, metadatas), namespace="ai")

        print("Files Added")

    except Exception as e:
        print(f"Error in load_data: {e}")


chat_history = {}




def check_file_format(persist_directory: str):
    # Mapping of file extensions to output values
    file_format_output = {
        ".pdf": (6, 3),
        ".csv": (11, 7),
        ".docx": (5, 3),
        ".xlsx": (11, 7)
    }

    # Extract the file extension and return the corresponding value
    file_extension = Path(persist_directory).suffix.lower()
    return file_format_output.get(file_extension, (4,2))

async def create_chain(retriever, model):
    system_prompt = "You are an expert SOC2 Auditor. Your job is to decide if the provided evidence meets the auditor's standards, and remediate the issue based only on the company's knowledge base and documents provided.  Do not provide any information that is not explicitly contained in the documents retrieved.  Always give summarized answers within 100 words using only the content from the retrieved documents.  If there is not enough information in the documents, respond with Insufficient information provided in the documents.{context}"
    
    main_prompt = ChatPromptTemplate.from_messages(
        [
            ("system", system_prompt),
            MessagesPlaceholder(variable_name="chat_history"),
            ("human", "{input}"),
        ]
    )

    retriever_prompt = ChatPromptTemplate.from_messages(
        [
            MessagesPlaceholder(variable_name="chat_history"),
            ("human", "{input}"),
            (
                "human",
                "Fetch the documents provided and take the above conversation into consideration as well.",
            ),
        ]
    )

    chain = create_stuff_documents_chain(llm=model, prompt=main_prompt)

    # No need to create a separate retriever here; using AzureAISearchRetriever directly
    history_aware_retriever = create_history_aware_retriever(
        llm=model, retriever=retriever, prompt=retriever_prompt
    )


    print("system prompt", system_prompt)
    print("main prompt", main_prompt)

    return create_retrieval_chain(history_aware_retriever, chain)


async def process_chat(chain, question, chat_history, dir, threshold):
    # Invoke the chain with input question and chat history
    response = chain.invoke({"input": question, "chat_history": chat_history})
    

    # print(response)
    answer = response['answer']

    final_response ={
        "input" : question,
        "chat_history" : chat_history,
        "answer": answer,
        "context": []
    }
    for docs in response["context"]:
        score = docs.metadata['@search.score']
        metadata_dict = docs.metadata["metadata"]
        print("got", score)
        if score >= threshold and metadata_dict['source'] == dir:
            print("matched", score)
            custom_data = {"metadata" : metadata_dict, "page_content" : docs.page_content}
            final_response['context'].append(custom_data)

    return final_response


async def generate_response(uid, persist_directory, rfe, markup):

    persist_directory = persist_directory.replace(" ", "_")
    
    chat_history.setdefault(uid, [])

    threshold, k = check_file_format(persist_directory)


    retriever = AzureAISearchRetriever(
        api_key=os.getenv("AZURE_SEARCH_KEY"),
        service_name="azure-vector-db",
        index_name="soc-index",
        top_k=k,  # Number of documents to retrieve
        filter=f"metadata/source eq '{persist_directory}'"
    )
    # Initialize Azure Chat model
    model = AzureChatOpenAI(
        max_tokens=200,
        openai_api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
        azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT"),
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    )
    print("got chat")

    # Create chain with Azure Cognitive Search retriever and model
    chain = await create_chain(retriever, model)

    # Process chat with the created chain
    result = await process_chat(chain, rfe, chat_history[uid], persist_directory, threshold)
    
    print(result)
    chat_history[uid].extend(
        [HumanMessage(content=rfe), AIMessage(content=result["answer"])]
    )

    source = persist_directory
    pages, page_contents = set(), {}
    for doc_details in result["context"]:

        lines = doc_details["page_content"].splitlines()

       
        if "pdf" in source:
            page = int(doc_details['metadata'].get('page')) + 1
            page_contents[page] = lines

        elif "page_name" in source:
            page = doc_details['metadata'].get('page_name')
            page_contents[page] = lines

        elif source.endswith((".xlsx", ".csv")):
            page = int(doc_details['metadata'].get('page')) + 1
            page_contents[page] = {"sheet": doc_details['metadata'].get('sheet'), "text" : lines}

        elif "docx" in source:
            page = int(doc_details['metadata'].get('page'))
            page_contents[page] = doc_details["page_content"].split("\n")
        else:
            page = 0
        # page = doc_details['metadata'].get('page') or doc_details['metadata'].get('page_name')

        # page_contents.setdefault(page, lines)
        

        pages.add(page)
    
    space_url = ""

    if markup:
        if "pdf" in source:
            await highlight_text_in_pdf(
                                        f"./docs/{source}",
                                        "out.pdf",
                                        page_contents,
                                        )    

            space_file_path = f"{uuid.uuid4()}.pdf"
            space_url = upload_to_space("out.pdf", space_file_path)
            print(space_url)

        elif "xlsx" in source:
            await highlight_text_in_xlsx(
                                        f"./docs/{source}",
                                        "out.xlsx", 
                                        page_contents
                                        )
            space_file_path = f"{uuid.uuid4()}.xlsx"
            space_url = upload_to_space("out.xlsx", space_file_path)
            print(space_url)

        elif "csv" in source:
            await highlight_text_in_csv(
                                        f"./docs/{source}",
                                        "out.xlsx",
                                        page_contents
                                        )
            space_file_path = f"{uuid.uuid4()}.xlsx"
            space_url = upload_to_space("out.xlsx", space_file_path)
            print(space_url)

        elif "docx" in source:
            await highlight_text_in_docx(
                                        f"./docs/{source}",
                                        "out.docx",
                                        page_contents
                                        )
            space_file_path = f"{uuid.uuid4()}.docx"
            space_url = upload_to_space("out.docx", space_file_path)
            print(space_url)
        
    return {
        "AI_message": result["answer"].strip(),
        "Source": source,
        "Pages/Rows" : pages,
        "Annotated_file" : space_url
        # pdf file will be returned as well after deployment
        }



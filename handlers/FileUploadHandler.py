from dto.FileUploadDTO import FileUploadDTO, PaginatedFileUploadResponse, FileUploadNoBlobDTO
from repository.FileUploadRepository import new_file_uploads, get_file_uploads_count, get_file_uploads, get_upload_file
from util.ClamAV import VirusDetectedException, VirusScanException, scan_file_with_clamscan
import time
from fastapi.responses import StreamingResponse
import io
import uuid
import os
import tempfile
from pathlib import Path
from fastapi import HTTPException


async def save_upload_file_to_temp(file, temp_file_path: str) -> None:
    print ("save_upload_file_to_temp .. ", temp_file_path)
    temp_path = Path(temp_file_path)
    temp_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(temp_path, "wb") as temp_file:
        while True:
            chunk = await file.read(1024*1024)
            if not chunk:
                break
            temp_file.write(chunk)
            
    print ("save_upload_file_to_temp .. completed")

async def upload(**kwargs):
    
    user_session = kwargs.get("token_info")
    body = kwargs.get("body", {})
    files = kwargs.get("files")
    return_message = "success"
    virusScanEnabled = True
    MAX_FILE_SIZE = 50 * 1024 * 1024

    if(virusScanEnabled):
        for file in files:
            file.file.seek(0,2)
            file_size = file.file.tell()
            file.file.seek(0)
            
            if file_size > MAX_FILE_SIZE:
                raise Exception(f"Uploaded file '{file.filename}' exceeds the 50MB limit.")
                
            temp_file_path = None
            
            try:
                safe_filename = os.path.basename(file.filename)
                unique_filename = f"{uuid.uuid4()}_{safe_filename}"
                
                temp_file_path = str(Path("/tmp/upload_files") / unique_filename) #nosec B108
                
                print ("temp_file_path is ",temp_file_path)
                print ("calling save_upload_file_to_temp")
                await save_upload_file_to_temp(file, temp_file_path)
                
                # Run virus scan
                await scan_file_with_clamscan(temp_file_path)
                
                # Read only after scan is clean
                with open(temp_file_path, "rb") as clean_file:
                    file_bytes = clean_file.read()
                    
                upload_data = FileUploadDTO.model_validate({
                    **body,
                    "client_id": user_session["clientid"],
                    "user_id": user_session["userid"],
                    "upload_file_name": file.filename,
                    "upload_file_data": file_bytes,
                    "upload_file_type": file.content_type,
                    "upload_file_status": "processing",
                })
                x = new_file_uploads(upload_data)

            except VirusDetectedException as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"Virus detected in file {file.filename}. Upload rejected."
                )

            except Exception as e:
                raise HTTPException(
                    status_code=500,
                    detail=f"Upload failed for file {file.filename}: {str(e)}"
                )

            finally:
                if temp_file_path and os.path.exists(temp_file_path):
                    print ("removing the file ",temp_file_path)
                    os.remove(temp_file_path)

        else:
            for file in files:
                file_bytes = await file.read()
                upload_data = FileUploadDTO.model_validate({
                    **body,
                    "client_id" : user_session["clientid"],
                    "user_id" : user_session["userid"],
                    "upload_file_name": file.filename,
                    "upload_file_data": file_bytes,
                    "upload_file_type": file.content_type,
                    "upload_file_status" : "processing"
                })
                x = new_file_uploads(upload_data)
                print(x.id)
                
        return {"status": "success"}

def read_fileupload_data(page: int, size: int):
    skip = (page - 1) * size
    
    total = get_file_uploads_count()
    uploads = get_file_uploads(skip, size)
    
    response = PaginatedFileUploadResponse(
        total=total,
        page=page,
        size=size,
        data=[FileUploadNoBlobDTO.model_validate(u) for u in uploads]
    )
    
    return response.model_dump()


def download_file(file_id : int):
    
    file = get_upload_file(file_id)
    
    file_stream = io.BytesIO(file.upload_file_data)
    filename = file.upload_file_name
    content_type = file.upload_file_type or "application/octet-stream"
    
    return StreamingResponse(
        file_stream,
        media_type=content_type,
        headers={
            "Content-Disposition" : f'attachment; filename="{filename}"'
        }
    )
from sqlalchemy.orm import Session
import json
from db.session import SessionLocal
from sqlalchemy import select, func
from repository.UploadRawDataRepository import get_rawdata, get_raw_data_count, get_raw_all_data
from dto.GenericDataDTO import GenericDataDTO, PaginatedGenericDataResponse
from connexion import request

def read_all_data(file_type: str) -> PaginatedGenericDataResponse:
    clientid = "12345"
    rawdata = get_raw_all_data(clientid,file_type)
    data = [
        GenericDataDTO(
            id=obj.id,
            json_data=obj.json_data
        ).model_dump()
        for obj in rawdata
    ]
    return data

def read_generic_data(**kwargs) -> PaginatedGenericDataResponse:

    usersession = request.context["token_info"]
    clientid = usersession['clientid']
    bodyDict = kwargs.get("body", {})

    print('bodyDict=',bodyDict)
    filters = {}
    page: int = int(bodyDict['page'])
    size: int = int(bodyDict['size'])

    for k,v in bodyDict.items():
        if k not in ["page","size"]:
            filters[k] = v

    print(page,size,filters)
    skip = (page - 1) * size

    total = get_raw_data_count(clientid,filters)
    rawdata = get_rawdata(skip,size, clientid, filters)
    for obj in rawdata:
        print(type(obj.json_data))

    data = [
        GenericDataDTO(
            id=obj.id,
            json_data=obj.json_data
        ).model_dump()
        for obj in rawdata
    ]

    response = PaginatedGenericDataResponse(
        total=total,
        page=page,
        size=size,
        data=data
    )

    return response.model_dump()
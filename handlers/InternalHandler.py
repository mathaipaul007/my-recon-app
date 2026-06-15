from sqlalchemy.orm import Session
from db.session import SessionLocal
from dbmodels.BankData import BankData
from sqlalchemy import select, func
from repository.UploadRawDataRepository import get_rawdata, get_raw_data_count
from dto.RawDataDTO import RawDataDTO, PaginatedRawDataResponse
from connexion import request

def read_internal_data(page: int, size: int) -> PaginatedRawDataResponse:

    usersession = request.context["token_info"]
    clientid = usersession['clientid']

    skip = (page - 1) * size

    total = get_raw_data_count(clientid,'INTERNALBOOK')
    rawdata = get_rawdata(skip,size, clientid,'INTERNALBOOK')

    response = PaginatedRawDataResponse(
        total=total,
        page=page,
        size=size,
        data=[RawDataDTO(id=obj.id, **{ **(obj.json_data or {}), "account": str((obj.json_data or {}).get("account"))}) for obj in rawdata]
    )

    return response.model_dump()
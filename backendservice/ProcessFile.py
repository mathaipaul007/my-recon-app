import sys
from io import BytesIO
import pandas as pd
import json
from repository.FileColumnMappingRepository import get_filecolumnmapping
from repository.UploadRawDataRepository import save_raw_data
from repository.FileUploadRepository import (
    update_file_status,
    get_file_uploads_by_status,
    get_all_file_uploads_by_status
)
from datetime import date

engine_dict = {
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "openpyxl",
    "application/vnd.ms-excel": "xlrd",
    "text/csv": "c"
}

def validateFile(fileuploads, mapping) -> bool:
    print("Validating the columns")
    mappingJson = mapping.json_data

    mappingHeaders = []
    excel_binary = fileuploads.upload_file_data
    excel_file = BytesIO(excel_binary)

    if fileuploads.upload_file_type == "text/csv":
        df = pd.read_csv(excel_file)
    else:
        df = pd.read_excel(
            excel_file,
            sheet_name=mapping.sheet_idx,
            engine=engine_dict[fileuploads.upload_file_type],
            header=mapping.header_row,
            dtype=str
        )

    headers = df.columns.tolist()

    for columnmapping in mappingJson:
        col = columnmapping["columnname"]
        mappingHeaders.append(col)

    print("headers from excel ", headers)
    print("mappingHeaders ", mappingHeaders)

    return headers == mappingHeaders

def processFileObject(fileuploads, mapping):
    mappingJson = mapping.json_data
    mandatoryJson = mapping.mandatory_column

    excel_binary = fileuploads.upload_file_data
    excel_file = BytesIO(excel_binary)
    print('mapping.header_row=', mapping.header_row)

    if(fileuploads.upload_file_type == "text/csv"):
        df = pd.read_csv(excel_file)
    else:
        df = pd.read_excel(excel_file, sheet_name=mapping.sheet_idx, engine=engine_dict[fileuploads.upload_file_type],
                           header=mapping.header_row, dtype=str)
        df = df.astype("object").where(pd.notnull(df), None)

    merged_cols = []
    for columnmapping in mappingJson:
        if("merged" in columnmapping):
            mrg = columnmapping["merged"]
            if(mrg == "yes"):
                merged_cols.append(columnmapping["columnname"])

    if(len(merged_cols) > 0):
        df[merged_cols] = df[merged_cols].ffill()

    rows_as_json = []
    for _, row in df.iterrows():
        mapped_row = {}
        for columnmapping in mappingJson:
            col = columnmapping["columnname"]
            new_key = columnmapping["keyname"]
            col_type = ""
            if "type" in columnmapping:
                col_type = columnmapping["type"]

            if col in df.columns:
                if col_type == "date" and not pd.isna(row[col]):
                    ts = pd.to_datetime(row[col]).date()
                    mapped_row[new_key] = ts.strftime("%Y-%m-%d")
                else:
                    if pd.isna(row[col]):
                        mapped_row[new_key] = ''
                    else:
                        mapped_row[new_key] = str(row[col]).strip()

                insertData = True
                if(mandatoryJson is not None):
                    for columnmapping in mandatoryJson:
                        if("columnname" in columnmapping):
                            col=columnmapping["columnname"]
                            if(mapped_row[col] == 'None' or mapped_row[col] == ''):
                                insertData = False

                if(insertData):
                    rows_as_json.append(json.dumps(mapped_row))
            return rows_as_json


def mainall(clientid):
    fileuploadsList = get_all_file_uploads_by_status(clientid,'processing')
    if not fileuploadsList == None:
        for fileuploads in fileuploadsList:
            print('file type:', fileuploads.recon_file_type)
            mapping = get_filecolumnmapping(clientid, fileuploads.recon_file_type)
            print('mapping:', mapping)
            jsonData = processFileObject(fileuploads, mapping)
            print(fileuploads.id, clientid)
            save_raw_data(fileuploads.id, fileuploads.recon_file_type, clientid, jsonData)
            print('save_raw_data')
            update_file_status(fileuploads, 'processed')
            print('update_file_status ... done')

def ProcessFile(clientid):
    print("ProcessFile Triggered")
    fileuploads = get_file_uploads_by_status(clientid, 'processing')
    if not fileuploads == None:
        print('File type:', fileuploads.recon_file_type)
        mapping = get_filecolumnmapping(clientid, fileuploads.recon_file_type)
        valid = validatefile(fileuploads, mapping)
        if(valid):
            jsonData = processFileObject(fileuploads, mapping)
            save_raw_data(fileuploads.id, fileuploads.recon_file_type, clientid, jsonData)
            update_file_status(fileuploads, 'processed')
            print('update_file_status .. done')
        else:
            update_file_status(fileuploads, 'error')

if __name__ == '__main__':
    clientid = '12345'
    runtimeArgv = sys.argv[1:]

    if(len(runtimeArgv) > 0):
        print("Arguments:", runtimeArgv)
        mainAll(clientid)
    else:
        ProcessFile(clientid)



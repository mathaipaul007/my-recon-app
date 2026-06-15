def readfundingfiles(clientid, fileType, fileDir):
    if file_path.is_file():
        print("Reading file = ", file_path)
        fdto = FileUploadDTO.model_validate({
            "client_id" : clientid,
            "user_id" : 1,
            "recon_file_type" : fileType,
            "recon_file_desc" : "test",
            "upload_file_name" : file_path.name,
            "upload_file_data" : file_path.read_bytes(),
            "upload_file_type" : "xlsx",
            "upload_file_status" : "processing"
        })
        print("Saved file id ", new_file_uploads(fdto))
        backendservice.ProcessFile.main(clientid)

def main(clientid):
    bmofunding = r"D:\recon\Net Pay Bank Recon\Net Pay Bank Recon\Backup - "
    nbfunding = r"D:\recon\Net Pay Bank Recon\Net Pay Bank Recon\Backup - b"

    readfundingfiles(clientid, "BMOFUNDING", bmofunding)
    readfundingfiles(clientid, "NBFUNDING", nbfunding)

if __name__ == "__main__":
    clientid = "12345"
    main(clientid)

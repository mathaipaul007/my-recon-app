from repository.UserRepository import get_user_by_id
from repository.UploadRawDataRepository import reset_all_status
from repository.ReconReportRepository import clear_all_report
from repository.ReconReportBatchRepository import clear_all_report_batch

def main(clientid):
    user = get_user_by_id(1)
    print(user)
    reset_all_status()
    clear_all_report()
    clear_all_report_batch()

if __name__ == "__main__":
    clientid = '12345'
    main(clientid)

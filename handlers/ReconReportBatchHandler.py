from dto.ReconReportBatchDTO import ReconReportBatchDTO, ReportBatchDTO, PaginatedReportBatchResponse
from repository.ReconReportBatchRepository import save_reconreport_data, get_reportbatch_count, get_reconreportbatch

from connexion import request

def generate_report(**kwargs):

    usersession = kwargs.get("token_info")
    print(usersession)

    body = kwargs.get("body", {})


    from_date = body.get("from_date")
    to_date = body.get("to_date")
    count = get_reportbatch_count(usersession["clientid"]) + 1

    batch_dto = ReconReportBatchDTO(
        batchid=count,
        client_id=usersession["clientid"],
        report_query={
            "from_date": from_date,
            "to_date": to_date
        },
        report_status="not_started",
        generate_user_id=usersession["userid"],
    )

    x=save_reconreport_data(batch_dto)
    print('Saved Successfully - ', x.batchid)
    return {"status": "success"}

def read_reportbatch_data(page: int, size: int):
    usersession = request.context["token_info"]
    clientid = usersession['clientid']

    skip = (page - 1) * size

    total = get_reportbatch_count(clientid)
    rows = get_reconreportbatch(clientid, skip, size)

    result = []
    for batch_obj, username in rows:
        item = ReportBatchDTO(
            id=batch_obj.batchid,
            report_query=batch_obj.report_query,
            report_status=batch_obj.report_status,
            matched = batch_obj.matched,
            unmatched= batch_obj.unmatched,
            generated_by=username,
        )
        result.append(item)

    response = PaginatedReportBatchResponse(
        total=total,
        page=page,
        size=size,
        data=result
    )

    return response.model_dump()
from sqlalchemy.orm import Session
from db.session import SessionLocal
from sqlalchemy.orm import Session
from db.session import SessionLocal
from dbmodels.UploadRawData import UploadRawData
from dbmodels.FileUploads import FileUploads
from sqlalchemy import select, func, update, and_, String, literal, cast, Date
from sqlalchemy.orm import aliased
from backendservice.QueryDSL.QueryDSL import Query, json_text, json_number, json_date_time

def get_bmo_funding_advice_settlement(clientid : str, rec_name : str):
    db: Session = SessionLocal()
    try:

        A = aliased(UploadRawData)

        BMOINPUT = (
            Query(A)
            .where_all(
                A.data_type == "INPUTRECON",
                json_text(A.json_data, "description").ilike("%0055%")
            ).as_cte("BMOINPUT")
        )

        B = aliased(UploadRawData)

        settlement_no = json_text(
            B.json_data, "settlement_no"
        ).label("settlement_no")

        BMOFUNDINGSUM = (
            Query(B)
            .where_all(
                B.data_type == "BMOFUNDING",
            )
            .select(settlement_no)
            .sum("amounts", label="sum_amt")
            .group_by(settlement_no)
            .as_cte("BMOFUNDINGSUM")
        )
        BMOFUNDINGSRNO = (
            Query(BMOINPUT)
            .select(
                BMOINPUT.c.id.label("bmoinputid"),
                BMOFUNDINGSUM.c.settlement_no.label("sr_no")
            )
            .join(
                BMOFUNDINGSUM,
                func.round(func.abs(json_number(BMOINPUT.c.json_data, "net")), 2) == func.round(func.abs(BMOFUNDINGSUM.c.sum_amt), 2)
            ).as_cte("BMOFUNDINGSRNO")
        )

        SETTLEMENTUNCLEARED = aliased(UploadRawData)
        BMPFUNDINGUNCLEARED = aliased(UploadRawData)

        BMOFUNDINGDATA = (
            Query(SETTLEMENTUNCLEARED)
            .where_all(
                BMPFUNDINGUNCLEARED.data_type == "BMOFUNDING",
                SETTLEMENTUNCLEARED.data_type == "INPUTRECON",
                BMPFUNDINGUNCLEARED.cleared_status == "U",
                SETTLEMENTUNCLEARED.cleared_status == "U",
                json_text(SETTLEMENTUNCLEARED.json_data, "description").ilike("%SETTLE%"),
                json_text(BMPFUNDINGUNCLEARED.json_data, "details") == rec_name,
                BMOFUNDINGSRNO.c.sr_no == json_text(BMPFUNDINGUNCLEARED.json_data, "settlement_no"),
                func.round(func.abs(json_number(BMPFUNDINGUNCLEARED.json_data, "amounts")), 2)
                == func.round(func.abs(json_number(SETTLEMENTUNCLEARED.json_data, "net")), 2)
            )
            .select(BMPFUNDINGUNCLEARED.id.label("bmofundid"), SETTLEMENTUNCLEARED.id.label("settlementid"), BMOFUNDINGSRNO.c.bmoinputid)
        )

        stmt = BMOFUNDINGDATA._build_stmt()
        results = db.execute(stmt).all()

        return results
    finally:
        db.close()
    
def getReportBMOFundData(clientId, bmoDataId, bmoFundId):
    db: Session = SessionLocal()
    try:
        bmodata = aliased(UploadRawData, name="bmodata")
        bmofunding = aliased(UploadRawData, name="bmofunding")

        stmt = (
            select(
                bmodata.id,
                func.replace(func.concat(bmodata.json_data["description"], bmodata.json_data["net"]), ' ', '').label("DESCRIPTION"),
                literal("Funding").label("CATEGORY"),
                bmofunding.json_data["settlement_no"].label("RUNID"),
                bmofunding.json_data["details"].label("TYPE"),
                func.round(json_number(bmofunding.json_data, "amounts"), 2).label("AMOUNT"),
                bmodata.json_data["value_date"].label("Value_Date")
            )
            .join(bmofunding, bmofunding.id == bmoFundId)
            .where(bmodata.id == bmoDataId)
        )

        results = db.execute(stmt).mappings().all()
        return results
    finally:
            db.close()

def getReportSettlementData(clientId, settlementId):
        db: Session = SessionLocal()
        try:
            stmt = (
                select(
                    UploadRawData.id,
                    UploadRawData.json_data["description"].label("DESCRIPTION"),
                    UploadRawData.json_data["net"].label("AMOUNT"),
                    UploadRawData.json_data["value_date"].label("Value_Date"),
                    literal("Advice").label("CATEGORY"),
                )
                .where(UploadRawData.id == settlementId)
            )

            results = db.execute(stmt).mappings().all()
            return results
        finally:
            db.close()

def delete_get_bmo_funding_settlement_task_1(clientId : str):
    db: Session = SessionLocal()
    try:
        settlementdata = aliased(UploadRawData, name="settlementdata")
        uploads = aliased(FileUploads, name="uploads")

        results = (Query(bmodata) \
            .join(settlementdata, func.round(func.abs(json_number(bmodata.json_data, "net")), 2) \
                == func.round(func.abs(json_number(settlementdata.json_data, "net")), 2)) \
            .where_all(
                bmodata.data_type == "INPUTRECON",
                bmodata.cleared_status == "U",
                settlementdata.data_type == "INPUTRECON",
                settlementdata.cleared_status == "U",
                uploads.id == bmodata.file_id,
                uploads.id == settlementdata.file_id,
                uploads.client_id == clientId,
                json_text(bmodata.json_data, "description").ilike("%0055%"),
                json_text(settlementdata.json_data, "description").ilike("%SETTLE%")
            ) \
            .select(bmodata.id, settlementdata.id) \
            .execute(db)
        )

        return results
    finally:
        db.close()

# select all "0055 NETPAY" amount = Sum of amount in BMOFunding file
def get_get_bmo_funding_settlement_task_2(clientId : str):
    db: Session = SessionLocal()
    try:
        A = aliased(UploadRawData)
        AA = (
            Query(A)
            .where_all(
                A.data_type == "INPUTRECON",
                A.cleared_status == "U",
                json_text(A.json_data, "description").ilike("%0055%")
            ).as_cte("AA")
        )

        B = aliased(UploadRawData)
        settlement_no = json_text(
            B.json_data, "settlement_no"
        ).label("settlement_no")

        BB = (
            Query(B)
            .where_all(
                B.data_type == "BMOFUNDING",
                B.cleared_status == "U"
            )
            .select(settlement_no)
            .sum("amounts", label="sum_amt")
            .group_by(settlement_no)
            .as_cte("BB")
        )

        stmt = (
            select(
                AA.c.id,
                BB.c.settlement_no,
                BB.c.sum_amt
            )
            .join(
                BB,
                func.round(func.abs(json_number(AA.c.json_data, "net")), 2) == func.round(func.abs(BB.c.sum_amt), 2)
            )
        )

        results = db.execute(stmt).all()
        return results

    finally:
        db.close()

def delete_getReportBMOFundData(clientid, ids):
    db: Session = SessionLocal()
    try:
        bmodata = aliased(UploadRawData, name="bmodata")
        uploads = aliased(FileUploads, name="uploads")
        stmt = (
            select(
                UploadRawData.id,
                UploadRawData.json_data["details"].label("DESCRIPTION"),
                UploadRawData.json_data["amounts"].label("AMOUNT"),
                UploadRawData.json_data["pay_date"].label("Value_Date")
            )
            .join(FileUploads, UploadRawData.file_id == FileUploads.id)
            .where(
                and_(
                    UploadRawData.data_type == "BMOFUNDING",
                    FileUploads.client_id == clientId,
                    UploadRawData.id.in_(ids)
                )
            )
        )

        results = db.execute(stmt).mappings().all()

        resultDict = {}
        for data in results:
            resultDict[data.id] = data

        return resultDict
    finally:
        db.close()

def delete_get_bmo_amount_settlement_by_sr_bmoadvice(clientId, srNoList):
    db: Session = SessionLocal()
    try:
        settlementdata = aliased(UploadRawData, name="settlementdata")
        bmofunding = aliased(UploadRawData, name="bmofunding")
        uploads = aliased(FileUploads, name="uploads")

        results = (Query(bmofunding) \
            .join(settlementdata, settlementdata.data_type == "INPUTRECON" \
            ).where_all(
                bmofunding.data_type == "BMOFUNDING",
                bmofunding.cleared_status == "U",
                settlementdata.cleared_status == "U",
                json_text(settlementdata.json_data, "description").ilike("%SETTLE%"),
                json_text(bmofunding.json_data, "details") == 'BMO -Advices',
                json_text(bmofunding.json_data, "settlement_no").in_(srNoList),
                func.round(func.abs(json_number(bmofunding.json_data, "amounts")), 2) == func.round(func.abs(json_number(settlementdata.json_data, "net")), 2)
            ) \
            .select(bmofunding.id.label("bmofundid"), settlementdata.id.label("settlementid"), json_text(bmofunding.json_data, "settlement_no").label("sr_no")) \
            .execute(db)
        )
        return results
    finally:
        db.close()

def delete_get_bmo_amount_settlement_by_sr_add(clientId, srNoList):
    db: Session = SessionLocal()
    try:
        settlementdata = aliased(UploadRawData, name="settlementdata")
        bmofunding = aliased(UploadRawData, name="bmofunding")
        uploads = aliased(FileUploads, name="uploads")

        results = (Query(bmofunding) \
            .join(settlementdata, settlementdata.data_type == "INPUTRECON" \
            ).where_all(
                bmofunding.data_type == "BMOFUNDING",
                bmofunding.cleared_status == "U",
                settlementdata.cleared_status == "U",
                json_text(settlementdata.json_data, "description").ilike("%SETTLE%"),
                json_text(bmofunding.json_data, "details") == 'ADD',
                json_text(bmofunding.json_data, "settlement_no").in_(srNoList),
                func.round(func.abs(json_number(bmofunding.json_data, "amounts")), 2) \
                == func.round(func.abs(json_number(settlementdata.json_data, "net")), 2)
            ) \
            .select(bmofunding.id.label("bmofundid"), settlementdata.id.label("settlementid"),
                json_text(bmofunding.json_data, "settlement_no").label("sr_no")) \
            .execute(db)
        )
        return results
    finally:
        db.close()

def get_nextlevel_bmo_funding_advice_settlement(clientId : str, rec_name : str):
    db: Session = SessionLocal()
    try:

        A = aliased(UploadRawData)

        BMOINPUT = (
            Query(A)
            .where_all(
                A.data_type == "INPUTRECON",
                json_text(A.json_data, "description").ilike("%0055%")
            ).as_cte("BMOINPUT")
        )

        B = aliased(UploadRawData)

        settlement_no = json_text(
            B.json_data, "settlement_no"
        ).label("settlement_no")

        BMOFUNDINGSUM = (
            Query(B)
            .where_all(
                B.data_type == "BMOFUNDING",
            )
            .select(settlement_no)
            .sum("amounts", label="sum_amt")
            .group_by(settlement_no)
            .as_cte("BMOFUNDINGSUM")
        )
        
        BMOFUNDINGSRNO = (
            Query(BMOINPUT)
            .select(
                BMOINPUT.c.id.label("bmoinputid"),
                BMOFUNDINGSUM.c.settlement_no.label("sr_no")
            )
            .join(
                BMOFUNDINGSUM,
                func.round(func.abs(json_number(BMOINPUT.c.json_data, "net")), 2) == func.round(func.abs(BMOFUNDINGSUM.c.sum_amt), 2)
            ).as_cte("BMOFUNDINGSRNO")
        )

        C = aliased(UploadRawData)
        BMOFUNDINGNEXTLEVELSRNO = (
            Query(C)
            .select(
                BMOFUNDINGSRNO.c.bmoinputid,
                json_text(C.json_data, "details").label("sr_no")
            )
            .join(BMOFUNDINGSRNO, BMOFUNDINGSRNO.c.sr_no == json_text(C.json_data, "settlement_no"))
            .where_all(
                C.data_type == "BMOFUNDING"
            )
            .as_cte("BMOFUNDINGNEXTLEVELSRNO")
        )

        SETTLEMENTUNCLEARED = aliased(UploadRawData)
        BMPFUNDINGUNCLEARED = aliased(UploadRawData)

        BMOFUNDINGDATA = (
            Query(SETTLEMENTUNCLEARED)
            .where_all(
                BMPFUNDINGUNCLEARED.data_type == "BMOFUNDING",
                SETTLEMENTUNCLEARED.data_type == "INPUTRECON",
                BMPFUNDINGUNCLEARED.cleared_status == "U",
                SETTLEMENTUNCLEARED.cleared_status == "U",
                json_text(SETTLEMENTUNCLEARED.json_data, "description").ilike("%SETTLE%"),
                json_text(BMPFUNDINGUNCLEARED.json_data, "details") == rec_name,
                BMOFUNDINGNEXTLEVELSRNO.c.sr_no == json_text(BMPFUNDINGUNCLEARED.json_data, "settlement_no"),
                func.round(func.abs(json_number(BMPFUNDINGUNCLEARED.json_data, "amounts")), 2) \
                == func.round(func.abs(json_number(SETTLEMENTUNCLEARED.json_data, "net")), 2)
            )
            .select(BMPFUNDINGUNCLEARED.id.label("bmofundid"), SETTLEMENTUNCLEARED.id.label("settlementid"), BMOFUNDINGNEXTLEVELSRNO.c.bmoinputid)
        )

        stmt = BMOFUNDINGDATA._build_stmt()
        results = db.execute(stmt).all()

        return results
    finally:
        db.close()

def get_bmo_funding_lockbox_settlement(clientId : str, from_date, to_date):
    db: Session = SessionLocal()
    try:
        A = aliased(UploadRawData)
        
        posted_date = func.substr(A.json_data["posted"].astext, 1, 10)
        BMOINPUT = (
            Query(A)
            .where_all(
                A.data_type == "INPUTRECON",
                json_text(A.json_data, "description").ilike("%LBX%"),
                posted_date >= from_date.isoformat(),
                posted_date <= to_date.isoformat()
            )
        ).as_cte("BMOINPUT")
        
        stmt = select(BMOINPUT)
        results = db.execute(stmt).all()
        return results
    finally:
        db.close()

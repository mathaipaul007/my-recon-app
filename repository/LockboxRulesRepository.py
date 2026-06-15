from sqlalchemy.orm import Session
from db.session import SessionLocal
from sqlalchemy.orm import SessionLocal
from db.session import SessionLocal
from dbmodels.UploadRawData import UploadRawData
from dbmodels.FileUploads import FileUploads
from sqlalchemy import select, func, update, and_, String, literal
from sqlalchemy.orm import aliased
from backendservice.QueryDSL.QueryDSL import Query, json_text, json_number

def get_bmo_lockbox_funding_settlement_data(clientid : str, bmo_rec_name : str, nb_rec_name : str):
    db: Session = SessionLocal()
    try:
        A = aliased(UploadRawData)

        BMOINPUT = (
            Query(A)
            .where_all(
                A.data_type == "INPUTRECON",
                json_text(A.json_data, "description").ilike("%0055%")
            )
            .as_cte("BMOINPUT")
        )

        B = aliased(UploadRawData)

        BMOFUNDINGSUM = (
            Query(B)
            .where_all(
                B.data_type == "BMOFUNDING"
            )
            .select(json_text(B.json_data, "settlement_no").label("settlement_no"))
            .sum("amounts", label="sum_amt")
            .group_by(json_text(B.json_data, "settlement_no").label("settlement_no"))
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
                func.round(func.abs(json_number(BMOINPUT.c.json_data, "net")),2) == func.round(func.abs(BMOFUNDINGSUM.c.sum_amt),2)
            ).as_cte("BMOFUNDINGSRNO")
        )

        #stmt = BMOADVICE._build_stmt()

        stmt = select(BMOFUNDINGSRNO)
        results = db.execute(stmt).all()

        return results
    finally:
     db.close()

def test_get_bmo_nb_funding_settlement_data(clientid: str, bmo_rec_name: str, nb_rec_name: str):
    db: Session = SessionLocal()
    try:
        A = aliased(UploadRawData)

        BMOINPUT = (
            Query(A)
            .where_all(
                A.data_type == "INPUTRECON",
                json_text(A.json_data, "description").ilike("%0055%")
            )
            .as_cte("BMOINPUT")
        )

        B = aliased(UploadRawData)

        BMOFUNDINGSUM = (
            Query(B)
            .where_all(
                B.data_type == "BMOFUNDING"
            )
            .select(json_text(B.json_data, "settlement_no").label("settlement_no"))
            .sum("amounts", label="sum_amt")
            .group_by(json_text(B.json_data, "settlement_no").label("settlement_no"))
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
            )
            .as_cte("BMOFUNDINGSRNO")
        )

        C = aliased(UploadRawData)

        NBINPUT = (
            Query(C)
            .where_all(
                C.data_type == "INPUTRECON",
                json_text(C.json_data, "description").ilike("%3677%")
            ).as_cte("NBINPUT")
        )

        D = aliased(UploadRawData)

        NBFUNDINGSUM = (
            Query(D)
            .where_all(
                D.data_type == "NBFUNDING"
            )
            .select(json_text(D.json_data, "settlement_no").label("settlement_no"))
            .sum("amounts", label="sum_amt")
            .group_by(json_text(D.json_data, "settlement_no").label("settlement_no"))
            .as_cte("NBFUNDINGSUM")
        )

        NBFUNDINGSRNO = (
            Query(NBINPUT)
            .select(
                NBINPUT.c.id.label("nbinputid"),
                NBFUNDINGSUM.c.settlement_no.label("sr_no")
            )
            .join(
                NBFUNDINGSUM,
                func.round(func.abs(json_number(NBINPUT.c.json_data, "net")),2) == func.round(func.abs(NBFUNDINGSUM.c.sum_amt),2)
            ).as_cte("NBFUNDINGSRNO")
        )

        SETTLEMENTUNCLEARED = aliased(UploadRawData)
        BMFUNDINGUNCLEARED = aliased(UploadRawData)
        NBFUNDINGUNCLEARED = aliased(UploadRawData)

        BMOADVICE = (
            Query(SETTLEMENTUNCLEARED)
            .where_all(
                BMOFUNDINGUNCLEARED.data_type == "BMOFUNDING",
                NBFUNDINGUNCLEARED.data_type == "NBFUNDING",
                SETTLEMENTUNCLEARED.data_type == "INPUTRECON",
                BMOFUNDINGUNCLEARED.cleared_status == "U",
                NBFUNDINGUNCLEARED.cleared_status == "U",
                SETTLEMENTUNCLEARED.cleared_status == "U",
                json_text(SETTLEMENTUNCLEARED.json_data, "description").ilike("%SETTLEX%"),
                json_text(SETTLEMENTUNCLEARED.json_data, "details") == bmo_rec_name,
                json_text(NBFUNDINGUNCLEARED.json_data, "details") == nb_rec_name,
                json_text(BMOFUNDINGSRNO.c.sr_no == json_text(BMOFUNDINGUNCLEARED.json_data, "settlement_no")),
                BMOFUNDINGSRNO.c.sr_no == json_text(NBFUNDINGUNCLEARED.json_data, "settlement_no"),
                func.round(func.abs(json_number(SETTLEMENTUNCLEARED.json_data, "net")),2) ==
                func.round(func.abs(json_number(BMOFUNDINGUNCLEARED.json_data, "amounts")),2),
                func.round(func.abs(json_number(NBFUNDINGUNCLEARED.json_data, "amounts")),2)
            )
            .select(BMOFUNDINGUNCLEARED.id.label("bmofundid"), NBFUNDINGUNCLEARED.id.label("nbfundid"),
                SETTLEMENTUNCLEARED.id.label("settlementid"), BMOFUNDINGSRNO.c.bmoinputid, NBFUNDINGSRNO.c.nbinputid)
        )

        stmt = BMOADVICE._build_stmt()
        results = db.execute(stmt).all()

        return results
    finally:
        db.close()


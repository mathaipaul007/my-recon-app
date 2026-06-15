
from sqlalchemy.orm import Session
from db.session import SessionLocal
from sqlalchemy.orm import Session
from db.session import SessionLocal
from dbmodels.UploadRawData import UploadRawData
from dbmodels.FileUploads import FileUploads
from sqlalchemy import select, func, update, and_, String, literal
from sqlalchemy.orm import aliased
from backendservice.QueryDSL.QueryDSL import Query, json_text, json_number


def test_get_bmo_nb_funding_settlement_data(clientid : str, bmo_rec_name : str, nb_rec_name : str):
    db: Session = SessionLocal()
    try:

        A = aliased(UploadRawData)

        BMOINPUT = (
            Query(A)
            .where_all(
                A.data_type == "INPUTRECON",
                json_text(A.json_data, "description").ilike("%0555%")
            )
            .as_cte("BMOINPUT")
        )

        B = aliased(UploadRawData)

        BMOFUNDINGSUM = (
            Query(B)
            .where_all(
                B.data_type == "BMOFUNDING",
            )
            .select(json_text(B.json_data, "settlement_no").label("settlement_no"),
                    func.sum("amounts").label("sum_amt"))
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
            )
            .as_cte("BMOFUNDINGSRNO")
        )

        C = aliased(UploadRawData)

        BMOFUNDINGADD = (
            Query(C)
            .where_all(
                C.data_type == "BMOFUNDING",
                json_text(C.json_data, "settlement_no") == BMOFUNDINGSRNO.c.sr_no,
                json_text(C.json_data, "details") == "ADD",
            )
            .select(BMOFUNDINGSRNO.c.bmoinputid, C.id, json_text(C.json_data, "amounts").label("amount"),
                    json_text(C.json_data, "settlement_no").label("settlement_no"))
            .as_cte("BMOFUNDINGADD")
        )

        D = aliased(UploadRawData)

        NBFUNDINGADD = (
            Query(D)
            .where_all(
                D.data_type == "NBFUNDING",
                json_text(D.json_data, "settlement_no") == BMOFUNDINGSRNO.c.sr_no,
                json_text(D.json_data, "details") == "ADD",
            )
            .select(D.id, json_text(D.json_data, "amounts").label("amount"),
                    json_text(D.json_data, "settlement_no").label("settlement_no"))
            .as_cte("NBFUNDINGADD")
        )

        E = aliased(UploadRawData)

        NBINPUT = (
            Query(E)
            .where_all(
                E.data_type == "INPUTRECON",
                json_text(E.json_data, "description").ilike("%3677%")
            ).as_cte("NBINPUT")
        )

        F = aliased(UploadRawData)
        NBFUNDINGSUM = (
            Query(F)
            .where_all(
                F.data_type == "NBFUNDING",
            )
            .select(json_text(F.json_data, "settlement_no").label("settlement_no"))
            .sum("amounts", label="sum_amt")
            .group_by(json_text(F.json_data, "settlement_no").label("settlement_no"))
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
            )
            .as_cte("NBFUNDINGSRNO")
        )

        G = aliased(UploadRawData)

        SETTLE = (
            Query(G)
            .where_all(
                G.data_type == "INPUTRECON",
                json_text(G.json_data, "description").ilike("%SETTLE%"),
                BMFUNDINGADD.c.settlement_no == NBFUNDINGADD.c.settlement_no,
                func.round(func.abs(json_number(G.json_data, "net")),2) == func.round(func.abs(BMFUNDINGADD.c.amount),2),
                func.round(func.abs(json_number(G.json_data, "net")),2) == func.round(func.abs(NBFUNDINGADD.c.amount),2)
            )
            .select(G.id.label("settlementId"), BMFUNDINGADD.c.bmoinputid.label("bmoinputid"),
                    BMFUNDINGADD.c.id.label("bmoFundId"), NBFUNDINGADD.c.id.label("nbFundId"))
            .as_cte("SETTLE")
        )

        stmt = BMOADVICE._build_stmt()
        stmt = select(NBFUNDINGSRNO)
        results = db.execute(stmt).all()

        return results
    finally:
        db.close()


def get_bmo_nb_funding_settlement_data(clientid: str, bmo_rec_name: str, nb_rec_name: str):
    db: Session = SessionLocal()
    try:
        A = aliased(UploadRawData)
        BMOINPUT = (
            Query(A)
            .where_all(
                A.data_type == "INPUTRECON",
                json_text(A.json_data, "description").ilike("%00555%")
            )
            .as_cte("BMOINPUT")
        )

        B = aliased(UploadRawData)
        B_settle_expr = json_text(B.json_data, "settlement_no")
        BMOFUNDINGSUM = (
            Query(B)
            .where_all(
                B.data_type == "BMOFUNDING",
            )
            .select(B_settle_expr.label("settlement_no"))
            .sum("amounts", label="sum_amt")
            .group_by(B_settle_expr)
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
                func.round(func.abs(json_number(BMOINPUT.c.json_data, "net")), 2)
                == func.round(func.abs(BMOFUNDINGSUM.c.sum_amt), 2)
            )
            .as_cte("BMOFUNDINGSRNO")
        )

        C = aliased(UploadRawData)
        NBINPUT = (
        Query(C)
        .where_all(
            C.data_type == "INPUTRECON",
            json_text(C.json_data, "description").ilike("%677%")
        ).as_cte("NBINPUT")
        )

        D = aliased(UploadRawData)

        n_settle_expr = json_text(D.json_data, "settlement_no")
        NBFUNDINGSUM = (
            Query(D)
            .where_all(
                D.data_type == "NBFUNDING",
            )
            .select(n_settle_expr.label("settlement_no"),
                    "sum(amounts)", label="sum_amt")
            .group_by(n_settle_expr)
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
            )
            .as_cte("NBFUNDINGSRNO")
        )

        SETTLEMENTUNICLEARED = aliased(UploadRawData)
        NBFUNDINGUNICLEARED = aliased(UploadRawData)
        NBFUNDINGUNICLEARED = aliased(UploadRawData)
        BMOADVICE = (
        Query(SETTLEMENTUNCLEARED)
        .where_all(
            BMOFUNDINGUNCLEARED.data_type == "BMOFUNDING",
            NBFUNDINGUNCLEARED.data_type == "NBFUNDING",
            SETTLEMENTUNCLEARED.data_type == "INPUTRECON",
            SETTLEMENTUNCLEARED.cleared_status == "U",
            json_text(BMOFUNDINGUNCLEARED.json_data, "description").ilike("%SETTLEX%"),
            json_text(BMOFUNDINGUNCLEARED.json_data, "details") == bmo_rec_name,
            json_text(NBFUNDINGUNCLEARED.json_data, "details") == nb_rec_name,
            json_text(BMOFUNDINGUNCLEARED.json_data, "settlement_no") ==
            json_text(NBFUNDINGUNCLEARED.json_data, "settlement_no"),
            json_text(BMOFUNDINGSRNO.c.sr_no) == json_text(NBFUNDINGUNCLEARED.json_data, "settlement_no"),
            func.round(func.abs(json_number(SETTLEMENTUNCLEARED.json_data, "net")), 2)
            == func.round(func.abs(json_number(NBFUNDINGUNCLEARED.json_data, "amounts")), 2)
            == func.round(func.abs(json_number(BMOFUNDINGUNCLEARED.json_data, "amounts")), 2)
        )
        .select(BMOFUNDINGUNCLEARED.id.label("bmofundid"),
                NBFUNDINGUNCLEARED.id.label("nbfundid"),
                SETTLEMENTUNCLEARED.id.label("settlementid"),
                BMOFUNDINGSRNO.c.bmoinputid,
                NBFUNDINGSRNO.c.nbinputid)
        )

        stmt = BMOADVICE._build_stmt()
        results = db.execute(stmt).all()

        return results

    finally:
        db.close()

def getBMOBNFundData(clientId, srNo):
    db: Session = SessionLocal()
    try:
        bmofunding = aliased(UploadRawData, name="bmofunding")
        nbfunding = aliased(UploadRawData, name="nbfunding")
        settlement = aliased(UploadRawData, name="settlement")
        stmt = (
            select(
                settlement.id.label("setId"),
                nbfunding.id.label("nbfId"),
                bmofunding.id.label("bmoFid"),
            )
            .select_from(settlement)
            .join(bmofunding, bmofunding.data_type == "BMOFUNDING")
            .join(nbfunding, nbfunding.data_type == "NBFUNDING")
            .where(and_(
                settlement.data_type == "INPUTRECON",
                settlement.cleared_status == "U",
                json_text(settlement.json_data, "description").ilike("%SETTLE%"),
                json_text(settlement.json_data, "settlement_no") == srNo,
                json_text(settlement.json_data, "details") == "BMO - Advices",
                bmofunding.cleared_status == "U",
                json_text(bmofunding.json_data, "settlement_no") == srNo,
                json_text(bmofunding.json_data, "details") == "NBS - Advices",
                nbfunding.cleared_status == "U",
                func.round(func.abs(json_number(settlement.json_data, "net")), 2)
                == func.round(func.abs(json_number(bmofunding.json_data, "amounts")), 2)
                == func.round(func.abs(json_number(nbfunding.json_data, "amounts")), 2)
            ))
        )
        results = db.execute(stmt).all()
        return results
    finally:
        db.close()

def getBMONBAddData(clientid, srNo):
    db: Session = SessionLocal()
    try:
        bmofunding = aliased(UploadRawData, name="bmofunding")
        nbfunding = aliased(UploadRawData, name="nbfunding")
        settlement = aliased(UploadRawData, name="settlement")
        stmt = (
            select(
                settlement.id.label("setid"),
                bmofunding.id.label("bmofid"),
                nbfunding.id.label("nbfid"),
            )
            .select_from(settlement)
            .join(bmofunding, bmofunding.data_type == "BMOFUNDING")
            .join(nbfunding, nbfunding.data_type == "NBFUNDING")
            .where(and(
                settlement.data_type == "INPUTRECON",
                settlement.cleared_status == "U",
                json_text(settlement.json_data, "description").ilike("%SETTLEX%"),
                json_text(bmofunding.json_data, "settlement_no") == srNo,
                json_text(bmofunding.json_data, "details") == "Add",
                json_text(nbfunding.json_data, "settlement_no") == srNo,
                json_text(nbfunding.json_data, "details") == "Add",
                bmofunding.cleared_status == "U",
                nbfunding.cleared_status == "U",
                func.round(func.abs(json_number(settlement.json_data, "net")),2)
                == func.round(func.abs(json_number(bmofunding.json_data, "amounts")),2)
                == func.round(func.abs(json_number(nbfunding.json_data, "amounts")),2)
            ))
        )
        results = db.execute(stmt).all()
        return results
    finally:
        db.close()
def getBMONBAddData(clientid, srNo):
    db: Session = SessionLocal()
    try:
        bmofunding = aliased(UploadRawData, name="bmofunding")
        nbfunding = aliased(UploadRawData, name="nbfunding")
        settlement = aliased(UploadRawData, name="settlement")
        stmt = (
            select(
                settlement.id.label("setid"),
                bmofunding.id.label("bmofid"),
                nbfunding.id.label("nbfid"),
            )
            .select_from(settlement)
            .join(bmofunding, bmofunding.data_type == "BMOFUNDING")
            .join(nbfunding, nbfunding.data_type == "NBFUNDING")
            .where(and_(
                settlement.data_type == "INPUTRECON",
                settlement.cleared_status == "U",
                json_text(settlement.json_data, "description").ilike("%SETTLEX%"),
                json_text(bmofunding.json_data, "settlement_no") == srNo,
                json_text(bmofunding.json_data, "details") == "Add",
                json_text(nbfunding.json_data, "settlement_no") == srNo,
                json_text(nbfunding.json_data, "details") == "Add",
                bmofunding.cleared_status == "U",
                nbfunding.cleared_status == "U",
                func.round(func.abs(json_number(settlement.json_data, "net")),2)
                == func.round(func.abs(json_number(bmofunding.json_data, "amounts")),2)
                == func.round(func.abs(json_number(nbfunding.json_data, "amounts")),2)
            ))
        )
        results = db.execute(stmt).all()
        return results
    finally:
        db.close()

def getReportNBFundData(clientId,nbInputId, nbfundId):
    db: Session = SessionLocal()
    try:
        nbdata = aliased(UploadRawData, name="nbdata")
        nbfunding = aliased(UploadRawData, name="nbfunding")

        stmt = (
            select(
                nbdata.id,
                func.replace(func.concat(nbdata.json_data["description"], nbdata.json_data["net"]), "" , "").label("DESCRIPTION"),
                literal("Funding").label("CATEGORY"),
                nbfunding.json_data["settlement_no"].label("RUNID"),
                nbfunding.json_data["details"].label("TYPE"),
                func.round(json_number(nbfunding.json_data, "amounts"),2).label("AMOUNT"),
                nbdata.json_data["value_date"].label("Value_Date")
            )
            .join(nbfunding,nbfunding.id == nbfundId)
            .where(nbdata.id == nbInputId)
        )
        results = db.execute(stmt).mappings().all()
        return results
    finally:
        db.close()


def delete_getReportNBFundData(clientId,nbAdveId):
    db: Session = SessionLocal()
    try:
        nbdata = aliased(UploadRawData, name="nbdata")
        nbfunding = aliased(UploadRawData, name="nbfunding")

        stmt = (
            select(
                literal("Credit Memo , BR.3677 NET PAY-0").label("DESCRIPTION"),
                literal("Funding").label("CATEGORY"),
                nbfunding.json_data["settlement_no"].label("RUNID"),
                nbfunding.json_data["details"].label("TYPE"),
                func.round(json_number(nbfunding.json_data, "amounts"),2).label("AMOUNT")
            )

            .where(and_(
                nbfunding.data_type == "NBFUNDING",
                nbfunding.id == nbAdveId
            ))
        )

        results = db.execute(stmt).mappings().all()
        return results
    finally:
        db.close()


def TestgetReportNBFundData(clientId, nbAdveId):
    db: Session = SessionLocal()
    try:
        nbdata = aliased(UploadRawData, name="nbdata")
        nbfunding = aliased(UploadRawData, name="nbfunding")

        stmt = (
            select(
                literal("- -").label("DESCRIPTION"),
                literal("Funding").label("CATEGORY"),
                nbfunding.json_data["settlement_no"].label("RUNID"),
                nbfunding.json_data["details"].label("TYPE"),
                nbfunding.json_data["amounts"].label("AMOUNT")
            )
            .join(nbdata, nbdata.data_type == "INPUTRECON")
            .where(and_(
                nbfunding.data_type == "NBFUNDING",
                nbfunding.id == nbAdveId,
                json_text(nbdata.json_data, "description").ilike("%3677%"),
                func.round(func.abs(json_number(nbdata.json_data, "net")), 2)
                == func.round(func.abs(json_number(nbfunding.json_data, "amounts")), 2)
            ))
        )
        results = db.execute(stmt).mappings().all()
        return results
    finally:
        db.close()

def get_nextlevel_bmo_nb_funding_settlement_data(clientId : str, bmo_rec_name : str, nb_rec_name : str):
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

        BMOFUNDINGSUM = (
            Query(B)
            .where_all(
                B.data_type == "BMOFUNDING",
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
            ).as_cte("BMOFUNDINGSRNO")
        )

        CC = aliased(UploadRawData)
        BMOFUNDINGNEXTLEVELSRNO = (
            Query(CC)
            .select(
                BMOFUNDINGSRNO.c.bmoinputid,
                json_text(CC.json_data, "details").label("sr_no")
            )
            .join(BMOFUNDINGSRNO, BMOFUNDINGSRNO.c.sr_no == json_text(CC.json_data, "settlement_no"))
            .where_all(
                CC.data_type == "BMOFUNDING"
            )
            .as_cte("BMOFUNDINGNEXTLEVELSRNO")
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
                D.data_type == "NBFUNDING",
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
                func.round(func.abs(json_number(NBINPUT.c.json_data, "net")), 2) == func.round(func.abs(NBFUNDINGSUM.c.sum_amt), 2)
            ).as_cte("NBFUNDINGSRNO")
        )

        DD = aliased(UploadRawData)
        NBFUNDINGNEXTLEVELSRNO = (
            Query(DD)
            .select(
                NBFUNDINGSRNO.c.nbinputid,
                json_text(DD.json_data, "details").label("sr_no")
            )
            .join(NBFUNDINGSRNO, NBFUNDINGSRNO.c.sr_no == json_text(DD.json_data, "settlement_no"))
            .where_all(
                DD.data_type == "NBFUNDING"
            ).as_cte("NBFUNDINGNEXTLEVELSRNO")
        )
        SETTLEMENTUNCLEARED = aliased(UploadRawData)
        BMOFUNDINGUNCLEARED = aliased(UploadRawData)
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
                json_text(SETTLEMENTUNCLEARED.json_data, "description").ilike("%SETTLE%"),
                json_text(BMOFUNDINGUNCLEARED.json_data, "details") == bmo_rec_name,
                json_text(NBFUNDINGUNCLEARED.json_data, "details") == nb_rec_name,
                BMOFUNDINGNEXTLEVELSRNO.c.sr_no == json_text(BMOFUNDINGUNCLEARED.json_data, "settlement_no"),
                BMOFUNDINGNEXTLEVELSRNO.c.sr_no == json_text(BMOFUNDINGUNCLEARED.json_data, "settlement_no"),
                NBFUNDINGNEXTLEVELSRNO.c.sr_no == json_text(NBFUNDINGUNCLEARED.json_data, "settlement_no"),
                func.round(func.abs(json_number(SETTLEMENTUNCLEARED.json_data, "net")),2)
                == func.round(func.abs(json_number(BMOFUNDINGUNCLEARED.json_data, "amounts")),2)
                + func.round(func.abs(json_number(NBFUNDINGUNCLEARED.json_data, "amounts")),2)
            )
            .select(BMOFUNDINGUNCLEARED.id.label("bmofundid"),NBFUNDINGUNCLEARED.id.label("nbfundid"),
            SETTLEMENTUNCLEARED.id.label("settlementid"),BMOFUNDINGNEXTLEVELSRNO.c.bmoinputid,
            NBFUNDINGNEXTLEVELSRNO.c.nbinputid,NBFUNDINGNEXTLEVELSRNO.c.sr_no)

        )

        stmt = BMOADVICE._build_stmt()
        #stmt = select(BMOFUNDINGNEXTLEVELSRNO)
        results = db.execute(stmt).all()

        return results
    finally:
        db.close()





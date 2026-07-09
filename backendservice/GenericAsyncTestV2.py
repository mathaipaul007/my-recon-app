import asyncio
import datetime
import inspect
import json
from decimal import Decimal
from typing import AsyncIterator, Any, Callable, List, TypeVar
from sqlalchemy.dialects import postgresql
from sqlalchemy import select, cast, Numeric, String
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from dbmodels.UploadRawData import UploadRawData
from dbmodels.FileUploads import FileUploads
from sqlalchemy import select, text, func, literal
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from backendservice.Query import Q, FromBMOFundingFile, _sum_, FromInputStatement, jcol, jnum, col ,with_aliases, pg_safe_numeric, FromNBFundingFile, add_, pg_json_text, FromLockBoxUploadFiles, fuzzy_match,FromLockBoxFile, FromNetPayRegistry, FromSTPV, case_numeric, FromReversalNetPayRegistry, FromAchRejectTracking,FromHanaBreakUp, const, FromDeductionGL
from dbmodels.ReconReport import ReconReport
from py_mini_racer import py_mini_racer

# ---------------------------------------------------------
# Settings
# ---------------------------------------------------------

DATABASE_URL = ""
engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# ---------------------------------------------------------
# Type helpers
# ---------------------------------------------------------

T = TypeVar("T")
U = TypeVar("U")

# Stage = function that takes AsyncIterator[In] -> AsyncIterator[Out]
StageFunc = Callable[[AsyncSession, AsyncIterator[Any]], AsyncIterator[Any]]


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)

def getQueryObject(query,batch,offset,item):
    allowed_globals = {
        "__builtins__": {},
        "FromBMOFundingFile": FromBMOFundingFile,
        "FromInputStatement": FromInputStatement,
        "FromLockBoxUploadFiles": FromLockBoxUploadFiles,
        "FromLockBoxFile": FromLockBoxFile,
        "FromNetPayRegistry": FromNetPayRegistry,
        "FromReversalNetPayRegistry": FromReversalNetPayRegistry,
        "FromAchRejectTracking": FromAchRejectTracking,
        "FromHanaBreakUp": FromHanaBreakUp,
        "FromDeductionGL": FromDeductionGL,
        "FromSTPV": FromSTPV,
        "sum_": sum_,
        "batch_size": batch,
        "offset": offset,
        "UploadRawData": UploadRawData,
        "FileUploads": FileUploads,
        "literal": literal,
        "item": item,
        "jcol": jcol,
        "jnum": jnum,
        "col": col,
        "Q": Q,
        "with_aliases": with_aliases,
        "func":func,
        "pg_safe_numeric": pg_safe_numeric,
        "pg_json_text": pg_json_text,
        "FromNBFundingFile" : FromNBFundingFile,
        "add_" : add_,
        "fuzzy_match":fuzzy_match,
        "cast" : cast,
        "Numeric" : Numeric,
        "String" : String,
        "case_numeric" : case_numeric,
        "const" : const
    }
    #print(query)
    return eval(query , allowed_globals, {})  # nosec B307


async def StageFuncNoBatchNoSplit(
    session: AsyncSession, batches: AsyncIterator[List[Dict]],**args
) -> AsyncIterator[List[dict]]:

    
    if(args and args['query']):
            query = str(args['query'])
            query = query.replace("\n", "").replace("\t", "")
    taskName = args['taskName']
    print(taskName)
    async for batch in batches:
        resultdata = []
        for item in batch:
            #print("Stage 2 input : ",item)
            #print("StageFuncNoBatch query ",query)

            qobj = getQueryObject(query,0,0, item)
            stmt = qobj.stmt()
            result = await session.execute(stmt)
            inputbatch = result.all()
            for x in inputbatch:
                d = dict(x._mapping)
                resultdata.append(d)
        if(taskName == "Query#11"):
            print(taskName)
            print(resultdata)
        yield resultdata

async def StageFuncJSExecutor(
    session: AsyncSession, batches: AsyncIterator[List[Dict]],**args
) -> AsyncIterator[List[dict]]:

    if(args and args['jsfunc']):
        jsfunc = str(args['jsfunc'])
    taskName = args['taskName']
    ctx = py_mini_racer.MiniRacer()
    ctx.eval(jsfunc)

    async for batch in batches:
        inputbatch = []
        for item in batch:
            inputbatch.append(item)
            print("Calling js function with param")

            #json_str = json.dumps(item, cls=DecimalEncoder)

            normalized_data = {
                k : str(v) if isinstance(v, Decimal) else v
                for k, v in item.items()
            }
            result = await asyncio.to_thread(ctx.call,"doProcess",normalized_data)
            #print(result)

            yield [result]

async def ReportStageFunc(
    session: AsyncSession, batches: AsyncIterator[List[Dict]],**args
) -> AsyncIterator[List[dict]]:

    if(args and args['query']):
        query = str(args['query'])
        query = query.replace("\n", "").replace("\t", "")
    taskName = args['taskName']

    async for batch in batches:
        inputbatch = []
        for item in batch:
            inputbatch.append(item)

            resultdata = []
            qobj = getQueryObject(query,0,0, item)
            stmt = qobj.stmt()
            result = await session.execute(stmt)
            nextbatch = result.all()
            for x in nextbatch:
                d = dict(x._mapping)
                resultdata.append(d)
            #await bulk_insert_report
            #if(taskName == "4" or taskName == "Report" ):
            print(taskName)
            print(resultdata)

        yield inputbatch






async def InitStageFuncBatch(
    session: AsyncSession, _: AsyncIterator[Any], **args
) -> AsyncIterator[List[dict]]:
    """Stage 1: Stream / paginate new orders"""
    offset = 0
    batch_size = 10

    if(args and args['batchsize']):
        batch_size = int(args['batchsize'])

    if(args and args['query']):
        query = str(args['query'])
        query = query.replace("\n", "").replace("\t", "")

    while True:
        #print("StageFuncBatch query ",query)
        qobj = getQueryObject(query,batch_size,offset,None)
        #stmt = qobj.stmt()
        #print(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        #result = await session.execute(stmt)

        batch = await qobj.execute(session)
        if not batch:
            break

        batchx = [dict(x) for x in batch]
        #print(f"Stage -> fetched {len(batch)} (offset={offset})")
        yield batchx

        offset += batch_size

async def StageFuncBatch(
    session: AsyncSession, batches: AsyncIterator[List[Dict]],**args
) -> AsyncIterator[List[dict]]:
    """Stage 1: Stream / paginate new orders"""
    offset = 0
    batch_size = 10

    if(args and args['batchsize']):
        batch_size = int(args['batchsize'])

    if(args and args['query']):
        query = str(args['query'])
        query = query.replace("\n", "").replace("\t", "")

    while True:
        async for batch in batches:
            print("Running the query with batch_size ",batch)
            for item in batch:

                qobj = getQueryObject(query,batch_size,offset,item)
                stmt = qobj.stmt()

                result = await session.execute(stmt)
                batch = result.all()

                if not batch:

                    break

                batchx = [dict(x._mapping) for x in batch]
                taskName = args['taskName']
                #print(taskName)
                #print(batchx)
                yield batchx

        offset += batch_size


async def run_pipeline(
    session:AsyncSession,
    stages: List[tuple[StageFunc,dict]]
):

    if not stages:
        print("No stages provided")
        return

    current_stream: AsyncIterator[Any] = iter([]).__iter__() # empty async iterator

    for i, (stage_func,config) in enumerate(stages, 1):
        print(f"\n— Starting stage {i}/{len(stages)}: {stage_func.__name__} — (config: {config})")

        current_stream = stage_func(session, current_stream, **config)

    async for _ in current_stream:
        pass

    print("\nPipeline completed")

async def InitStageFuncNoBatch(
    session: AsyncSession, _: AsyncSession[Any], **args
) -> AsyncIterator[List[dict]]:
    if(args and args['query']):
        query = str(args['query'])
        query = query.replace("\n", "").replace("\t", "")



    qobj = getQueryObject(query,0,0,None)
    stmt = qobj.stmt()
    #print(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    result = await session.execute(stmt)
    batch = result.all()

    batchx = [dict(x._mapping) for x in batch]

    #print(f"Stage -> fetched {len(batch)} (offset={offset})")
    yield batchx


async def StageFuncNoBatch(
    session: AsyncSession, batches: AsyncIterator[List[Dict]],**args
) -> AsyncIterator[List[dict]]:


    if(args and args['query']):
        query = str(args['query'])
        query = query.replace("\n", "").replace("\t", "")
    taskName = args['taskName']
    print(taskName)
    async for batch in batches:
        for item in batch:
            #print("Stage 2 input : ",item)
            #print("StageFuncNoBatch query ",query)
            resultdata = []
            qobj = getQueryObject(query,0,0, item)
            #stmt = qobj.stmt()
            #result = await session.execute(stmt)
            inputbatch = await qobj.execute(session)

            for x in inputbatch:
                d = dict(x)
                resultdata.append(d)
            if(taskName == "Query#461" or taskName == "Query#191"):
                print(taskName)
                print(resultdata)
            yield resultdata



async def main():
    #BMO-SUM-AMOUNT
    #Get the sum of amount group by settlement_no from BMO Funding file
    query_1 = """FromBMOFundingFile().
    find("settlement_no", sum_("amounts").as_("sum_amt")).
    grouped_by("settlement_no")"""

    # COMPARE-SUM-INPUT-BMO
    # Match the sum with net value of 00555 record in the input file
    query_2 = """FromInputStatement()
    .find(UploadRawData.id.label("bmoinputid"),literal(item["settlement_no"]).label("sr_no"),
    literal(item["sum_amt"]).label("bmo_sum_amt"))
    .where(jcol("description").contains("%00555%"))
    .and_(jnum("net").rounded_abs_eq(item["sum_amt"],2))"""

    # GET-BMO-AMOUNT-BY-SRNO
    # Get the amount value from the bmo funding file for the filtered settlement_no
    query_3 = """FromBMOFundingFile()
    .find(UploadRawData.id.label("bmofundid"), pg_safe_numeric(UploadRawData.json_data,"amounts").label("bmoamount") ,
    literal(item["bmoinputid"]).label("bmoinputid") ,literal(item["sr_no"]).label("sr_no"))
    .where(jcol("settlement_no").is_(item["sr_no"]))
    .and_(jcol("details").is_("BMO -Advices"))
    .and_(col("cleared_status").is_("U"))"""

    #COMPARE-BMO-AMOUT-SETTLEMENT
    query_4 = """FromInputStatement()
    .find(UploadRawData.id.label("settlementid"),literal(item["bmofundid"]).label("bmofundid"),
    literal(item["bmoinputid"]).label("bmoinputid"))
    .where(jcol("description").contains("%SETTLE%"))
    .and_(col("cleared_status").is_("U"))
    .and_(jnum("net").rounded_abs_eq(item["bmoamount"],2))"""

    #NB-SUM-AMOUNT
    #Get the sum of amount group by settlement_no from NB Funding file
    query_5 = """FromNBFundingFile().
    find("settlement_no", sum_("amounts").as_("sum_amt"),literal(item["bmofundid"]).label("bmofundid"),
    literal(item["bmoamount"]).label("bmoamount"),literal(item["bmoinputid"]).label("bmoinputid")).
    where(jcol("settlement_no").is_(item["sr_no"])).
    grouped_by("settlement_no")"""

    # COMPARE-SUM-INPUT-NB
    # Match the sum with net value of 3677 record in the input file
    query_6 = """FromInputStatement()
    .find(UploadRawData.id.label("nbinputid"),literal(item["settlement_no"]).label("sr_no"),
    literal(item["bmofundid"]).label("bmofundid"),literal(item["bmoamount"]).label("bmoamount"),
    literal(item["bmoinputid"]).label("bmoinputid"))
    .where(jcol("description").contains("%3677%"))
    .and_(jnum("net").rounded_abs_eq(item["sum_amt"],2))"""

    # GET-NB-AMOUNT-BY-SRNO
    # Get the amount value from the NB funding file for the filtered settlement_no
    query_7 = """FromNBFundingFile()
    .find(UploadRawData.id.label("nbfundid"), add_(UploadRawData.json_data,"amounts",
    literal(item["bmoamount"])).label("bmo-nbamounts") ,literal(item["bmoinputid"]).label("bmoinputid"),
    literal(item["bmofundid"]).label("bmofundid"),literal(item["nbinputid"]).label("nbinputid"))
    .where(jcol("settlement_no").is_(item["sr_no"]))
    .and_(jcol("details").is_("NBS - Advices"))
    .and_(col("cleared_status").is_("U"))"""

    # COMPARE-NB-BMO-AMOUT-SETTLEMENT
    # Match the BMO and NB amount with settlement
    query_8 = """FromInputStatement()
    .find(UploadRawData.id.label("settlementid"),literal(item["bmofundid"]).label("bmofundid"),
    literal(item["bmoinputid"]).label("bmoinputid"),literal(item["nbfundid"]).label("nbfundid"),
    literal(item["nbinputid"]).label("nbinputid"))
    .where(jcol("description").contains("%SETTLE%"))
    .and_(col("cleared_status").is_("U"))
    .and_(jnum("net").rounded_abs_eq(item["bmo-nbamounts"],2))"""

    # GET-BMO-NEXTLEVEL-SRNO
    # Get the next level SRNO
    query_9 = """FromBMOFundingFile()
    .find(UploadRawData.id.label("bmofundid"), pg_json_text(UploadRawData.json_data,"details").label("sr_no") ,
    literal(item["bmoinputid"]).label("bmoinputid") ,
    pg_safe_numeric(UploadRawData.json_data,"amounts").label("bmoamount"))
    .where(jcol("settlement_no").is_(item["sr_no"]))
    .and_(jcol("details").contains("SR-"))"""

    # GET-BMO-ADD-BY-SRNO
    # Get the ADD value from the bmo funding file for the filtered settlement_no
    query_10 = """FromBMOFundingFile()
    .find(UploadRawData.id.label("bmofundid"), pg_safe_numeric(UploadRawData.json_data,"amounts").label("bmoamount") ,
    literal(item["bmoinputid"]).label("bmoinputid") ,literal(item["sr_no"]).label("sr_no"))
    .where(jcol("settlement_no").is_(item["sr_no"]))
    .and_(jcol("details").is_("ADD"))
    .and_(col("cleared_status").is_("U"))"""

    # GET-BMO-LOCKBOX-BY-SRNO
    # Get the LOCKBOX value from the bmo funding file for the filtered settlement_no
    query_11 = """FromBMOFundingFile()
    .find(UploadRawData.id.label("bmofundid"), pg_safe_numeric(UploadRawData.json_data,"amounts").label("bmoamount") ,
    literal(item["bmoinputid"]).label("bmoinputid") ,literal(item["sr_no"]).label("sr_no"),
    pg_json_text(UploadRawData.json_data,"details").label("details"))
    .where(jcol("settlement_no").is_(item["sr_no"]))
    .and_(jcol("details").contains("Lockbox adj"))
    .and_(col("cleared_status").is_("U"))"""

    # GET-LOCKBOX-UPLOAD-BY-FILENAME
    # Locate the lockbox upload bybased on upload file name
    query_12 = """FromLockBoxUploadFiles()
    .find(literal(item["bmofundid"]).label("bmofundid"),literal(item["bmoamount"]).label("bmoamount") ,
    literal(item["bmoinputid"]).label("bmoinputid") ,literal(item["sr_no"]).label("sr_no"),
    FileUploads.upload_file_name.label("upload_file_name"),FileUploads.id.label("fileid"))
    .post_filter(fuzzy_match("upload_file_name",item["details"],0.70))"""

    # MATCH-LOCKBOX-SUM-OF-CHECKAMT-BMOAMT
    # Compare the Sum of check amount with input bmo amount
    query_13 = """FromLockBoxFile()
    .find(func.sum(pg_safe_numeric(UploadRawData.json_data,"cheque_amount")),
    literal(item["fileid"]).label("fileid"),literal(item["bmofundid"]).label("bmofundid"),
    literal(item["bmoinputid"]).label("bmoinputid"),literal(item["sr_no"]).label("sr_no"))
    .where(col("file_id").is_(item["fileid"]))
    .and_(jcol("company").contains("BMO"))
    .grouped_by("funding_adj")
    .having(func.sum(pg_safe_numeric(UploadRawData.json_data,"cheque_amount")) == func.abs(cast(item["bmoamount"], Numeric)))
    """
    # GET-LOCKBOX-CHECKAMT-SUM-GROUP-BY-FUNDINGADJ-REPORTDATE
    # Get the sum of check amount based on Funding adj and Report Date
    query_14 = """FromLockBoxFile()
    .find("funding_adj","report_date",func.sum(pg_safe_numeric(UploadRawData.json_data,"cheque_amount")).label("sum_amt"),
    literal(item["bmofundid"]).label("bmofundid"),literal(item["bmoinputid"]).label("bmoinputid"),
    literal(item["sr_no"]).label("sr_no"))
    .where(col("file_id").is_(item["fileid"]))
    .and_(jcol("company").contains("BMO"))
    .grouped_by("funding_adj","report_date")"""


    # MATCH-SUM-AMOUNT-INPUT-LBX-VALUE
    # Compare the Sum amount with inputsheets net value of LBX rows
    query_15 = """FromInputStatement()
    .find(UploadRawData.id.label("lbxinputid"),literal(item["bmofundid"]).label("bmofundid"),
    literal(item["bmoinputid"]).label("bmoinputid"),literal(item["funding_adj"]).label("funding_adj"),
    literal(item["report_date"]).label("report_date"),literal(item["sr_no"]).label("sr_no"))
    .where(jcol("description").contains("%LBX%"))
    .and_(jnum("net").rounded_abs_eq(item["sum_amt"],2))"""

    # GET-CHECK-NOS
    # Get all check numbers from input file
    query_16 = """FromInputStatement()
    .find(UploadRawData.id.label("cheque_id"),
    func.substring(UploadRawData.json_data.op("->>")("description"),r"Cheque\s,\s*NO\.(\d+)").label("cheque"))
    .where(col("cleared_status").is_("U"))"""

    # MATCH-RETURNED-CHECK-NOS
    # Compare the check numbers with returned check numbers
    query_17 = """FromInputStatement()
    .find(literal(item["cheque_id"]).label("cheque_id"),UploadRawData.id.label("return_cheque_id"),
    literal(item["cheque"]).label("chequeno"))
    .where(col("cleared_status").is_("U"))
    .and_(cast(literal(item["cheque"]),String)== func.substring(UploadRawData.json_data.op("->>")("description"),r"RETURNED ITEM NO\.\s*(\d+)"))
    """

    # GET-PREMIER-CHECK-BY-CHECK
    # Get the amount value from the premier funding file for the filtered premier Check
    query_18 = """FromBMOFundingFile()
    .find(UploadRawData.id.label("bmofundid"), pg_safe_numeric(UploadRawData.json_data,"amounts").label("bmoamount") ,
    literal(item["bmoinputid"]).label("bmoinputid") ,literal(item["sr_no"]).label("sr_no"),
    literal(item["bmo_sum_amt"]).label("bmo_sum_amt"))
    .where(jcol("settlement_no").is_(item["sr_no"]))
    .and_(jcol("details").is_("BMO -Checks"))
    .and_(col("cleared_status").is_("U"))
    """

    # MATCH-NETPAY-PREMIER-CHECK-BY-SRNO
    # Get the amount value from the premier funding file for the filtered premier Check
    query_19 = """FromNetPayRegistry()
    .find("settlement_run", func.sum(pg_safe_numeric(UploadRawData.json_data,"net_pay")) ,
    literal(item["bmoinputid"]).label("bmoinputid"),literal(item["bmofundid"]).label("bmofundid"))
    .where(jcol("settlement_run").is_(item["sr_no"]))
    .and_(jcol("Company").is_("Bank of Montreal"))
    .and_(jcol("Check_Num") != 0)
    .grouped_by("settlement_run")
    .having(func.sum(pg_safe_numeric(UploadRawData.json_data,"net_pay"))== func.round(cast(item["bmoamount"], Numeric), 2))
    """

    #.and_(col("cleared_status").is_("U"))
    #Example for cte usage
    #query_CTE = """Q.with_cte("netpay_rows",
    #FromNetPayRegistry()
    #.find("settlement_run","Check_Num", pg_safe_numeric(UploadRawData.json_data,"net_pay").label("net_pay"))
    #.where(jcol("settlement_run").is_(item["settlement_run"]))
    #.and_(jcol("Company").is_("Bank of Montreal"))
    #.and_(jcol("Check_Num") != 0)
    #.and_(col("cleared_status").is_("U")),
    #lambda a:(
    #  FromSTPV().find("serial_number",a.c.Check_Num, literal(item["bmoinputid"]).label("bmoinputid"),
    #  a.c.netpayid, literal(item["bmofundid"]).label("bmofundid"),a.c.net_pay)
    #  .where(pg_safe_numeric(UploadRawData.json_data,"serial_number") != case_numeric(a.c.Check_Num))
    #))
    #"""

    # FILTER-NETPAY-PREMIER-CHECKS-NOT-IN-STPV
    # Filter out all the netpay premier checks which are not in STPV file

    query_20 = """FromNetPayRegistry()
    .find("settlement_run","Check_Num", UploadRawData.id.label("netpayid"),
    literal(item["bmoinputid"]).label("bmoinputid") ,literal(item["bmofundid"]).label("bmofundid"),
    pg_safe_numeric(UploadRawData.json_data,"net_pay").label("net_pay"))
    .where(jcol("settlement_run").is_(item["settlement_run"]))
    .and_(jcol("Company").is_("Bank of Montreal"))
    .and_(jcol("Check_Num") != 0)
    .and_(col("cleared_status").is_("U"))
    .and_(pg_safe_numeric(UploadRawData.json_data, "Check_Num").not_in(FromSTPV()
    .find(pg_safe_numeric(UploadRawData.json_data,"serial_number")).as_scalar_subquery()))
    """

    # MATCH-INPUT-RECON-CHECK-BY-CHECK-NETPAY
    # Filter out all the netpay premier checks which are not in STPV file

    query_21 = """FromInputStatement()
    .find(UploadRawData.id.label("cheque_id"), literal(item["Check_Num"]).label("cheque"),
    literal(item["settlement_run"]).label("settlement_run"),literal(item["bmoinputid"]).label("bmoinputid"),
    literal(item["bmofundid"]).label("bmofundid"), literal(item["netpayid"]).label("netpayid"))
    .where(col("cleared_status").is_("U"))
    .and_(func.substring(UploadRawData.json_data.op("->>")("description"),r"Cheque\s,\s*NO\.(\d+)") == item["Check_Num"])
    .and_(cast(item["net_pay"], Numeric) == pg_safe_numeric(UploadRawData.json_data,"net"))
    """

    # FILTER-NETPAY-PREMIER-CHECKS-IN-STPV
    # Filter out all the netpay premier checks which are in STPV file

    query_22 = """FromNetPayRegistry()
    .find("settlement_run","Check_Num", UploadRawData.id.label("netpayid"),
    literal(item["bmoinputid"]).label("bmoinputid") ,literal(item["bmofundid"]).label("bmofundid"),
    pg_safe_numeric(UploadRawData.json_data,"net_pay").label("net_pay"),
    pg_json_text(UploadRawData.json_data,"employee_id").label("employee_id") )
    .where(jcol("settlement_run").is_(item["settlement_run"]))
    .and_(jcol("Company").is_("Bank of Montreal"))
    .and_(jcol("Check_Num") != 0)
    .and_(col("cleared_status").is_("U"))
    .and_(pg_safe_numeric(UploadRawData.json_data, "Check_Num").in_(FromSTPV().find(pg_safe_numeric(UploadRawData.json_data,
    "serial_number")).as_scalar_subquery()))
    """

    query_23 = """FromReversalNetPayRegistry()
    .find(UploadRawData.id.label("rnetpayid"))
    .where(jcol("settlement_run").is_(item["settlement_run"]))
    .and_(jcol("result_type_descr").is_("Reversal"))
    .and_(jcol("employee_id").is_(item["employee_id"]))
    .and_(cast(item["net_pay"], Numeric) == pg_safe_numeric(UploadRawData.json_data,"net_pay"))
    """

    # GET-BMO-REVERSAL
    # Get the amount value from the premier funding file for the filtered settlement_no

    query_24 = """FromBMOFundingFile()
    .find(UploadRawData.id.label("bmofundid"), pg_safe_numeric(UploadRawData.json_data,"amounts").label("bmoamount") ,
    literal(item["bmoinputid"]).label("bmoinputid") ,literal(item["sr_no"]).label("sr_no"),
    pg_json_text(UploadRawData.json_data,"pay_date").label("pay_date"),
    literal(item["bmoinputid"]).label("bmoinputid"))
    .where(jcol("settlement_no").is_(item["sr_no"]))
    .and_(jcol("details").is_("reversal"))
    .and_(col("cleared_status").is_("U"))"""

    # GET-NETPAY-REGISTRY-BY-PREMIER-REVERSAL
    # Get the EE DETAILS from netpay registry file for the filtered paydate and Reversal
    query_25 = """FromReversalNetPayRegistry()
    .find(sum_("net_pay").as_("sum_amt"),literal(item["pay_date"]).label("pay_date"),
    literal(item["bmoamount"]).label("bmoamount"),literal(item["bmofundid"]).label("bmofundid"),
    literal(item["bmoinputid"]).label("bmoinputid"))
    .where(jcol("lookup_company_id").is_("BMO"))
    .and_(jcol("result_type_descr").is_("Reversal"))
    .and_(jcol("payment_date_rev_date").is_(item["pay_date"]))
    """

    # GET-NETPAY-REGISTRY-BY-PREMIER-REVERSAL-MATCHING-SUM
    # Get the EE DETAILS from netpay registry file for the filtered paydate and Reversal and matching the sum value

    query_26 = """FromReversalNetPayRegistry()
    .find(literal(item["pay_date"]).label("pay_date"),
    pg_json_text(UploadRawData.json_data,"employee_id").label("rev_eeid"),
    UploadRawData.id.label("rev_netpayid"),pg_safe_numeric(UploadRawData.json_data,"net_pay").label("rev_netpay"),
    literal(item["bmofundid"]).label("bmofundid"),literal(item["bmoinputid"]).label("bmoinputid"))
    .where(jcol("lookup_company_id").is_("BMO"))
    .and_(jcol("result_type_descr").is_("Reversal"))
    .and_(jcol("payment_date_rev_date").is_(item["pay_date"]))
    .and_(item["sum_amt"] == item["bmoamount"])
    """

    # GET-INPUT-STATEMENT-BY-RECALL-VALUE
    # Get Input statement details by RECALL values

    query_27 = """FromInputStatement()
    .find(UploadRawData.id.label("recallinputid"),pg_json_text(UploadRawData.json_data,"value_date").label("value_date"),
    literal(item["rev_eeid"]).label("rev_eeid"),literal(item["rev_netpayid"]).label("rev_netpayid"),
    literal(item["rev_netpay"]).label("rev_netpay"),literal(item["bmofundid"]).label("bmofundid"),
    literal(item["bmoinputid"]).label("bmoinputid"))
    .where(jcol("description").contains("%RECALLS%"))
    """

    # GET-ACH-REJECT-SUM-AMOUNT-BY-VALUE-DATE
    # Get ACH reject sum amount value by value date
    query_28 = """FromAchRejectTracking()
    .find(sum_("amount"),"deb_cred","value_date",literal(item["rev_eeid"]).label("rev_eeid"),
    literal(item["rev_netpayid"]).label("rev_netpayid"),literal(item["rev_netpay"]).label("rev_netpay"),
    literal(item["bmofundid"]).label("bmofundid"),literal(item["bmoinputid"]).label("bmoinputid"))
    .where(jcol("value_date").is_(item["value_date"]))
    .and_(jcol("type_of_reject").is_("240"))
    .grouped_by("value_date","deb_cred")
    """

    # MATCH-ACH-REJECT-BY-VALUE-DATE-TYPE-OF-REJECT
    # Filter out Ach Reject rows by value date and type of reject
    query_29 = """FromAchRejectTracking()
    .find(literal(item["sum_amt"]).label("sum_amt"), literal(item["deb_cred"]).label("deb_cred"),
    literal(item["value_date"]).label("value_date"), literal(item["rev_eeid"]).label("rev_eeid"),
    pg_json_text(UploadRawData.json_data,"ein").label("ach_eeid"),
    pg_safe_numeric(UploadRawData.json_data,"amount").label("ach_amount"),
    literal(item["rev_netpayid"]).label("rev_netpayid"),
    literal(item["rev_netpay"]).label("rev_netpay"),
    literal(item["bmofundid"]).label("bmofundid"),literal(item["bmoinputid"]).label("bmoinputid"))
    .where(jcol("value_date").is_(item["value_date"]))
    .and_(jcol("type_of_reject").is_("240"))
    .and_(jcol("deb_cred").is_(item["deb_cred"]))
    """

    # GET-INPUT-STATEMENT-BY-CREDIT-VALUE
    # Get the details of input statement for the Ach Credit values
    # Yet to implement the Clear part
    query_30 = """FromInputStatement()
    .find(UploadRawData.id.label("input_id"),literal(item["bmofundid"]).label("bmofundid"),
    literal(item["bmoinputid"]).label("bmoinputid"), literal(item["ach_eeid"]).label("eeid") )
    .where(jcol("value_date").is_(item["value_date"]))
    .and_(pg_safe_numeric(UploadRawData.json_data, "credit") == item["sum_amt"])
    .and_(item["deb_cred"] == "Credit")
    .and_(cast(item["rev_eeid"],Numeric) == cast(item["ach_eeid"],Numeric))
    .and_(func.abs(item["rev_netpay"]) == func.abs(item["ach_amount"]))
    """

    # GET-PREMIER-AMOUNT-BY-HANA
    # Get the Hana amount value from the premier funding file for the filtered settlement_no

    query_31 = """FromBMOFundingFile()
    .find(col("id").label("bmofundid"), pg_safe_numeric(UploadRawData.json_data,"amounts").label("bmoamount") ,
    const(item["bmoinputid"]).label("bmoinputid") ,const(item["sr_no"]).label("sr_no"),
    jcol("pay_date").label("pay_date"),
    const(item["bmoinputid"]).label("bmoinputid"),jcol("details").label("details"))
    .where(jcol("settlement_no").is_(item["sr_no"]))
    .and_(jcol("details").contains("%Hana_Returned%"))
    .and_(col("cleared_status").is_("U"))"""

    # Javascript logic to extract PayReport Date and CrossCharge Date from Hanna Details value
    query_32 = """
    function doProcess(param){
        const mVal = String(param.details).replace(/[\u00A0\u200B\u200C\u200D\uFEFF]/g," ")
            .replace(/[\u2010\u2011\u2012\u2013\u2014\u2212]/g,"-")
            .replace(/\s+/g," ").trim()
        const m1 = mVal.match(/^\s*(\d{1,2})-(\d{1,2})-(\d{4})/);
        const mm = Number(m1[1]);
        const mmStr = (mm < 10) ? '0'+mm : ''+mm
        const dd = Number(m1[2]);
        const ddStr = (dd < 10) ? '0'+dd : ''+dd
        const yyyy = Number(m1[3]);
        //const startDate = new Date(yyyy, mm - 1, dd);
        
        param.crossChargeDate = yyyy + '-' + mmStr + '-' + ddStr
        
        const monthRegex = /(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})\s+(\d{4})/g;
        
        const last = mVal.match(monthRegex)[0].split(" ")
        
        if (last) {
            
            const monthName = last[0];
            const day2 = Number(last[1]);
            const year2 = Number(last[2]);
            
            // Let JS parse month name reliably by using a canonical string.
            const endDate = new Date(`${monthName} ${day2}, ${year2}`);
            const yyyy = endDate.getFullYear();
            const mm = endDate.getMonth() + 1;
            const mmStr = (mm < 10) ? '0'+mm : ''+mm
            const dd = endDate.getDate()
            const ddStr = (dd < 10) ? '0'+dd : ''+dd
            
            param.payReportDate = yyyy + '-' + mmStr + '-' + ddStr
        }else{
            param.payReportDate = mVal
        }
        return param
    }"""

    # GET-SUM-AMOUNT-GROUP-BY-COROSSCHARGE-PAYDATE
    # Filter the data by the company BMO and sum the amount group by Cross charge date and pay date
    query_33 = """FromHanaBreakUp()
    .find(sum_("dr").as_("sum_amt"),const(item["crossChargeDate"]).label("crossChargeDate"),
    const(item["payReportDate"]).label("payReportDate"),const(item["bmoamount"]).label("bmoamount"),
    const(item["bmofundid"]).label("bmofundid"),const(item["bmoinputid"]).label("bmoinputid"))
    .where(jcol("company").is_("BMO"))
    .grouped_by("cross_charge","pay_date")
    """

    # GET-HANA-DATA-BY-COMPANY-CROSSCHARGE-PAYDATE
    # Get the Hana data based on company, Cross charge Date and pay report date
    query_34 = """FromHanaBreakUp()
    .find("ein","dr",col("id").label("hana_id"),const(item["payReportDate"]).label("payReportDate"),
    const(item["bmofundid"]).label("bmofundid"),const(item["bmoinputid"]).label("bmoinputid"))
    .where(jcol("company").is_("BMO"))
    .and_(jcol("cross_charge").is_(item["crossChargeDate"]))
    .and_(jcol("pay_date").is_(item["payReportDate"]))
    .and_(item["sum_amt"] == cast(item["bmoamount"],Numeric))
    """

    # GET-DEFT-RETURNED-TXNS-INPUT-DATA
    # Get "DEFT RETURNED TXNS" Input data
    query_35 = """FromInputStatement()
    .find(jcol("value_date").label("inp_value_date"),jcol("posted").label("inp_posted"),const(item["ein"]).label("ein"),
    const(item["dr"]).label("hana_amount"),const(item["payReportDate"]).label("payReportDate"),
    const(item["bmofundid"]).label("bmofundid"),const(item["bmoinputid"]).label("bmoinputid"),
    const(item["hana_id"]).label("hana_id"))
    .where(jcol("description").contains("%Not Service Chargeable , DEFT RETURNED TXNS%"))
    """

    # GET-ACHREJECT-DATA-SUM-AMOUNT-GROUPBY-VALUEDATE-RUN-DATE
    # Get Sum amount value from AchRejectTracking Data group by value date and run date
    query_36 = """FromAchRejectTracking()
    .find(sum_("amount"),"run_date","value_date",const(item["ein"]).label("ein"),
    const(item["hana_amount"]).label("hana_amount"),const(item["payReportDate"]).label("payReportDate"),
    const(item["inp_posted"]).label("inp_posted"),const(item["inp_value_date"]).label("inp_value_date"),
    const(item["bmofundid"]).label("bmofundid"),const(item["bmoinputid"]).label("bmoinputid"),
    const(item["hana_id"]).label("hana_id"))
    .where(jcol("value_date").is_(item["payReportDate"]))
    .and_(jcol("run_date").is_(item["inp_posted"]))
    .and_(jcol("type_of_reject").is_("260"))
    .and_(jcol("deb_cred").is_("Debit"))
    .grouped_by("value_date","run_date")
    """

    # GET-ACHREJECT-DATA-BY-VALUEDATE-RUNDATE-TYPEOFREJECT-DEBIT
    # Get the AchRejectTracking based on value date, run date, type of reject, debit and EE
    query_37 = """FromAchRejectTracking()
    .find(col("id"),jcol("ein").label("ach_eeid"),const(item["sum"]).label("sum_amt"),
    const(item["ein"]).label("ein"),const(item["inp_posted"]).label("inp_posted"),
    const(item["inp_value_date"]).label("inp_value_date"),const(item["bmofundid"]).label("bmofundid"),
    const(item["bmoinputid"]).label("bmoinputid"),const(item["hana_id"]).label("hana_id"))
    .where(jcol("value_date").is_(item["payReportDate"]))
    .and_(jcol("run_date").is_(item["inp_posted"]))
    .and_(jcol("type_of_reject").is_("260"))
    .and_(jcol("deb_cred").is_("Debit"))
    .and_(jcol("ein").is_(item["ein"]))
    .and_(jnum("amount").eq(cast(item["hana_amount"],Numeric)))
    """

    # GET-INPUTDATA-BY-VALUEDATE-POSTED-SUMAMOUNT
    # Get the inputdata based on value date, posted date and sum amount
    query_38 = """FromInputStatement()
    .find(col("id").label("input_id"),const(item["bmofundid"]).label("bmofundid"),
    const(item["bmoinputid"]).label("bmoinputid"),const(item["ein"]).label("ein"),
    const(item["hana_id"]).label("hana_id"),const(item["achid"]).label("achid"))
    .where(jcol("value_date").is_(item["inp_value_date"]))
    .and_(jcol("posted").is_(item["inp_posted"]))
    .and_(jnum("net").eq(item["sum_amt"]))
    """

    # GET-PREMIER-AMOUNT-BY-NPREIS
    # Get the NPREIS amount value from the premier funding file for the filtered settlement_no
    query_39 = """FromBMOFundingFile()
    .find(col("id").label("bmofundid"),jnum("amounts").label("bmoamount"),const(item["bmoinputid"]).label("bmoinputid"),
    const(item["sr_no"]).label("sr_no"),jcol("pay_date").label("pay_date"),const(item["bmoinputid"]).label("bmoinputid"))
    .where(jcol("settlement_no").is_(item["sr_no"]))
    .and_(jcol("details").is_("NPREIS"))
    .and_(col("cleared_status").is_("U"))"""

    # GET-DEDUCTIONGL-SUM-AMOUNT-BY-NPREIS
    # Get the NPREIS amount value from the premier funding file for the filtered settlement_no
    query_40 = """FromDeductionGL()
    .find(sum_("sum_amount").as_("ded_sum_amt"),const(item["sr_no"]).label("sr_no"),
    const(item["bmoamount"]).label("bmoamount"),const(item["bmofundid"]).label("bmofundid"),
    const(item["bmoinputid"]).label("bmoinputid"))
    .where(jcol("settlement_no").is_(item["sr_no"]))
    .and_(jcol("company").is_("Bank of Montreal"))
    .and_(jcol("deduction").contains("NPREIS"))
    .and_(col("cleared_status").is_("U"))"""

    # GET-DEDUCTIONGL-FILTER-BY-NPREIS-AMOUNT
    query_41 = """FromDeductionGL()
    .find(const(item["sr_no"]).label("sr_no"),col("id").label("dgl_id"),jcol("sum_amount").label("ded_amt"),
    jcol("employee_id").label("ded_ein"),const(item["bmofundid"]).label("bmofundid"),
    const(item["bmoinputid"]).label("bmoinputid"))
    .where(jcol("settlement_no").is_(item["sr_no"]))
    .and_(jcol("company").is_("Bank of Montreal"))
    .and_(jcol("deduction").contains("NPREIS"))
    .and_(item["ded_sum_amt"] == cast(item["bmoamount"],Numeric))
    .and_(col("cleared_status").is_("U"))"""

    # GET-DEGL-DEFT-RETURNED-TXNS-INPUT-DATA
    # Get "DEFT RETURNED TXNS" Input data
    query_42 = """FromInputStatement()
    .find(col("id").label("input_id"),jcol("value_date").label("inp_value_date"),jcol("posted").label("inp_posted"),
    jcol("net").label("net"),const(item["ded_amt"]).label("ded_amt"),const(item["ded_ein"]).label("ded_ein"),
    const(item["bmofundid"]).label("bmofundid"),const(item["bmoinputid"]).label("bmoinputid"),
    const(item["dgl_id"]).label("dgl_id"))
    .where(jcol("description").contains("%Not Service Chargeable , DEFT RETURNED TXNS%"))
    """

    # GET-DEGL-ACHREJECT-DATA-SUM-AMOUNT-GROUPBY-VALUEDATE-RUN-DATE
    query_43 = """FromAchRejectTracking()
    .find(sum_("amount"),"run_date","value_date",const(item["net"]).label("net"),const(item["ded_amt"]).label("ded_amt"),
    const(item["bmofundid"]).label("bmofundid"),
    const(item["bmoinputid"]).label("bmoinputid"),const(item["input_id"]).label("input_id"),
    const(item["dgl_id"]).label("dgl_id"))
    .where(jcol("value_date").is_(item["inp_value_date"]))
    .and_(jcol("run_date").is_(item["inp_posted"]))
    .and_(jcol("type_of_reject").is_("260"))
    .and_(jcol("deb_cred").is_("Credit"))
    .grouped_by("value_date","run_date")
    """

    # GET-DEGL-ACHREJECT-DATA-SUM-AMOUNT-MATCHING-AMOUNT
    query_44 = """FromAchRejectTracking()
    .find(col("id").label("achid"),const(item["bmofundid"]).label("bmofundid"),const(item["bmoinputid"]).label("bmoinputid"),
    const(item["input_id"]).label("input_id"),const(item["dgl_id"]).label("dgl_id"),
    const(item["ded_ein"]).label("ein"),item["sum"],item["net"])
    .where(jcol("value_date").is_(item["inp_value_date"]))
    .and_(jcol("run_date").is_(item["inp_posted"]))
    .and_(jcol("type_of_reject").is_("260"))
    .and_(jcol("deb_cred").is_("Credit"))
    .and_(jcol("ein").is_(item["ded_ein"]))
    .and_(cast(item["sum"],Numeric) == func.abs(cast(item["net"],Numeric)))
    .and_(jnum("amount").eq(func.abs(cast(item["ded_amt"],Numeric))))
    """

    # GET-DEGL-DEFT-REJECTS-TXNS-INPUT-DATA
    # Get "DEFT REJECTS TXNS" Input data
    query_45 = """FromInputStatement()
    .find(col("id").label("input_id"),jcol("value_date").label("inp_value_date"),jcol("posted").label("inp_posted"),
    jcol("net").label("net"),const(item["ded_amt"]).label("ded_amt"),const(item["ded_ein"]).label("ded_ein"),
    const(item["bmofundid"]).label("bmofundid"),const(item["bmoinputid"]).label("bmoinputid"),
    const(item["dgl_id"]).label("dgl_id"))
    .where(jcol("description").contains("%Not Service Chargeable , DEFT REJECTS BOM%"))
    """

    # GET-DEGL-ACHREJECT-DATA-SUM-AMOUNT-GROUPBY-VALUEDATE-RUN-DATE-210
    query_46 = """FromAchRejectTracking()
    .find(sum_("amount").as_("ach_sum_amt"),"run_date","value_date",const(item["net"]).label("net"),
    const(item["ded_amt"]).label("ded_amt"),const(item["ded_ein"]).label("ded_ein"),
    const(item["bmofundid"]).label("bmofundid"),const(item["bmoinputid"]).label("bmoinputid"),
    const(item["input_id"]).label("input_id"),const(item["dgl_id"]).label("dgl_id"))
    .where(jcol("value_date").is_(item["inp_value_date"]))
    .and_(jcol("run_date").is_(item["inp_posted"]))
    .and_(jcol("type_of_reject").is_("210"))
    .and_(jcol("deb_cred").is_("Credit"))
    .grouped_by("value_date","run_date")
    """

    # GET-DEGL-ACHREJECT-DATA-SUM-AMOUNT-MATCHING-AMOUNT-210
    query_47 = """FromAchRejectTracking()
    .find(col("id").label("achid"),const(item["bmofundid"]).label("bmofundid"),const(item["bmoinputid"]).label("bmoinputid"),
    const(item["input_id"]).label("input_id"),const(item["dgl_id"]).label("dgl_id"),
    const(item["ded_ein"]).label("ein"),item["ach_sum_amt"],item["net"])
    .where(jcol("value_date").is_(item["inp_value_date"]))
    .and_(jcol("type_of_reject").is_("210"))
    .and_(jcol("deb_cred").is_("Credit"))
    .and_(jcol("ein").is_(item["ded_ein"]))
    .and_(cast(item["ach_sum_amt"],Numeric) == func.abs(cast(item["net"],Numeric)))
    .and_(jnum("amount").eq(func.abs(cast(item["ded_amt"],Numeric))))
    .union(FromAchRejectTracking()
    .find(col("id").label("achid"),const(item["bmofundid"]).label("bmofundid"),const(item["bmoinputid"]).label("bmoinputid"),
    const(item["input_id"]).label("input_id"),const(item["dgl_id"]).label("dgl_id"),
    const(item["ded_ein"]).label("ein"),item["ach_sum_amt"],item["net"])
    .where(jcol("value_date").is_(item["inp_value_date"]))
    .and_(jcol("run_date").is_(item["inp_posted"]))
    .and_(jcol("type_of_reject").is_("210"))
    .and_(jcol("deb_cred").is_("Credit"))
    .and_(jcol("ein").is_(item["ded_ein"]))
    .and_(cast(item["ach_sum_amt"],Numeric) == func.abs(cast(item["net"],Numeric)))
    .and_(jnum("amount").eq(func.abs(cast(item["ded_amt"],Numeric)))))
    """

    # GET-UNCLEARED-NETPAYREGISTRY
    query_48 = """FromNetPayRegistry()
    .find(col("id").as_("netpayid"),jcol("employee_id").as_("employee_id"),literal(item["bmoinputid"]).label("bmoinputid"),
    literal(item["bmo_sum_amt"]).label("bmo_sum_amt"))
    .where(jcol("settlement_run").is_(item["sr_no"]))
    .and_(jcol("Company").is_("Bank of Montreal"))
    .and_(jcol("Check_Num") != 0)
    .and_(col("cleared_status").is_("U"))
    """

    # Report Query
    # BMO-Report-to pull BMO-Advice row in the report
    rep_query_1 = """with_aliases(
    UploadRawData,"bmodata","bmofunding",
    lambda bmodata, bmofunding:
    Q(bmodata)
    .join(bmofunding, on=bmofunding.id == item["bmofundid"])
    .find(func.replace(func.concat(bmodata.json_data["description"].astext,
    bmodata.json_data["net"].astext),'', '').label("DESCRIPTION"),
    literal("Funding").label("CATEGORY"),
    bmofunding.json_data["settlement_no"].astext.label("RUNID"),
    bmofunding.json_data["details"].astext.label("TYPE"),
    func.round(pg_safe_numeric(bmofunding.json_data["amounts"],2).label("AMOUNT"),
    bmodata.json_data["value_date"].astext.label("Value_Date"))
    .where(bmodata.id == item["bmoinputid"])
    )"""

    rep_query_2 = """FromInputStatement().
    find(UploadRawData.json_data["description"].astext.label("DESCRIPTION"),
    func.round(pg_safe_numeric(UploadRawData.json_data["net"],2).label("AMOUNT"),
    UploadRawData.json_data["value_date"].astext.label("Value_Date"),literal("Advice").label("CATEGORY")).
    where(UploadRawData.id == item["settlementid"])"""

    # NB-Report-to pull NB-Advice row in the report
    rep_query_3 = """with_aliases(
    UploadRawData,"nbdata","nbfunding",
    lambda nbdata, nbfunding:
    Q(nbdata)
    .join(nbfunding, on=nbfunding.id == item["nbfundid"])
    .find(func.replace(func.concat(nbdata.json_data["description"].astext, nbdata.json_data["net"].astext),'','')
    literal("Funding").label("CATEGORY"),
    nbfunding.json_data["settlement_no"].astext.label("RUNID"),
    nbfunding.json_data["details"].astext.label("TYPE"),
    func.round(pg_safe_numeric(nbfunding.json_data["amounts"],2).label("AMOUNT"),
    nbdata.json_data["value_date"].astext.label("Value_Date"))
    .where(nbdata.id == item["nbinputid"])
    )"""

    # LOCKBOX-INPUT-Report
    # Generate report based on Lockbox and input recon data
    rep_query_4 = """with_aliases(
    UploadRawData,"lbxinputdata","lockboxdata",
    lambda lbxinputdata, lockboxdata:
    Q(lbxinputdata)
    .join(lockboxdata, on=lockboxdata.json_data["report_date"].astext == literal(item["report_date"]))
    .find(lbxinputdata.json_data["description"].astext.label("DESCRIPTION"),
    literal("LBX").label("CATEGORY"),
    lockboxdata.json_data["ein"].astext.label("EIN"),
    func.round(pg_safe_numeric(lockboxdata.json_data["cheque_amount"],2).label("AMOUNT"),
    lbxinputdata.json_data["value_date"].astext.label("Value_Date"))
    .where(lbxinputdata.id == item["lbxinputid"])
    .and_(lockboxdata.json_data["funding_adj"].astext == literal(item["funding_adj"]))
    )"""

    # LOCKBOX-BMOFUND-Report
    # Generate report based on Lockbox and Bmo Fund data
    rep_query_5 = """with_aliases(
    UploadRawData,"bmoinputdata","lockboxdata","bmofundingdata",
    lambda bmoinputdata, lockboxdata, bmofundingdata:
    Q(bmoinputdata)
    .join(lockboxdata, on=lockboxdata.json_data["report_date"].astext == literal(item["report_date"]))
    .join(bmofundingdata, on=bmofundingdata.id == item["bmofundid"])
    .find(func.replace(func.concat(bmoinputdata.json_data["description"].astext,
    bmoinputdata.json_data["net"].astext),'','').label("DESCRIPTION"),
    literal("Funding").label("CATEGORY"),
    lockboxdata.json_data["ein"].astext.label("EIN"),
    bmofundingdata.json_data["details"].astext.label("TYPE"),
    literal(item["sr_no"]).label("RUNID"),
    func.round(pg_safe_numeric(lockboxdata.json_data["cheque_amount"],2).label("AMOUNT"),
    bmoinputdata.json_data["value_date"].astext.label("Value_Date"))
    .where(bmoinputdata.id == item["bmoinputid"])
    .and_(lockboxdata.json_data["funding_adj"].astext == literal(item["funding_adj"]))
    )"""

    # GET-CHECK-NO-Report
    # Generate report for check number
    rep_query_6 = """FromInputStatement().
    find(UploadRawData.json_data["description"].astext.label("DESCRIPTION"),
    func.round(pg_safe_numeric(UploadRawData.json_data["net"],2).label("AMOUNT"),
    UploadRawData.json_data["value_date"].astext.label("Value_Date"),literal(item["chequeno"]).label("CHECK #")).
    where(UploadRawData.id == item["cheque_id"])"""

    # GET-RETURNED-CHECK-NO-Report
    # Generate report for Returned Check number
    rep_query_7 = """FromInputStatement().
    find(UploadRawData.json_data["description"].astext.label("DESCRIPTION"),
    func.round(pg_safe_numeric(UploadRawData.json_data["net"],2).label("AMOUNT"),
    UploadRawData.json_data["value_date"].astext.label("Value_Date"),literal(item["chequeno"]).label("CHECK #")).
    where(UploadRawData.id == item["return_cheque_id"])"""

    # GET-PREMIER-CHECK-NO-Report
    # Generate report for premier check number
    rep_query_8 = """with_aliases(
    UploadRawData,"bmodata","bmofunding", "netpay",
    lambda bmodata, bmofunding, netpay:
    Q(bmodata)
    .join(bmofunding, on=bmofunding.id == item["bmofundid"])
    .join(netpay, on=netpay.id == item["netpayid"])
    .find(func.replace(func.concat(bmodata.json_data["description"].astext,
    bmodata.json_data["net"].astext),'','').label("DESCRIPTION"),
    literal("Funding").label("CATEGORY"),
    bmofunding.json_data["settlement_no"].astext.label("RUNID"),
    bmofunding.json_data["details"].astext.label("TYPE"),
    func.round(pg_safe_numeric(netpay.json_data["net_pay"],2).label("AMOUNT"),
    netpay.json_data["employee_id"].astext.label("EIN"),
    netpay.json_data["Check_Num"].astext.label("CHECK #"),
    bmodata.json_data["value_date"].astext.label("Value_Date"))
    .where(bmodata.id == item["bmoinputid"])
    )"""

    # GET-PREMIER-INPUT-CHECK-NO-Report
    # Generate report for premier Check number
    rep_query_9 = """FromInputStatement().
    find(UploadRawData.json_data["description"].astext.label("DESCRIPTION"),
    func.round(pg_safe_numeric(UploadRawData.json_data["net"],2).label("AMOUNT"),
    UploadRawData.json_data["value_date"].astext.label("Value_Date")).
    where(UploadRawData.id == item["cheque_id"])"""

    rep_query_10 = """with_aliases(
    UploadRawData,"bmodata","bmofunding",
    lambda bmodata, bmofunding:
    Q(bmodata)
    .join(bmofunding, on=bmofunding.id == item["bmofundid"])
    .find(func.replace(func.concat(bmodata.json_data["description"].astext,
    bmodata.json_data["net"].astext),'','').label("DESCRIPTION"),
    literal("Funding").label("CATEGORY"),
    bmofunding.json_data["settlement_no"].astext.label("RUNID"),
    bmofunding.json_data["details"].astext.label("TYPE"),
    literal(item["eeid"]).label("EIN"),
    func.round(pg_safe_numeric(bmofunding.json_data["amounts"],2).label("AMOUNT"),
    bmodata.json_data["value_date"].astext.label("Value_Date"))
    .where(bmodata.id == item["bmoinputid"])
    )"""

    # GET-PREMIER-INPUT-STAT-ACH-REJECT-Report
    # Generate report for premier, input Ach Reject Report
    rep_query_11 = """with_aliases(
    UploadRawData,"inputdata","bmofunding",
    lambda inputdata, bmofunding:
    Q(inputdata)
    .join(bmofunding, on=bmofunding.id == item["bmofundid"])
    .find(inputdata.json_data["description"].astext.label("DESCRIPTION"),
    inputdata.json_data["value_date"].astext.label("Value_Date"),
    literal("DEFR-240").label("CATEGORY"),
    literal(item["eeid"]).label("EIN"),
    func.round(pg_safe_numeric(bmofunding.json_data["amounts"],2).label("AMOUNT"))
    .where(inputdata.id == item["input_id"])
    )"""

    # Generate report for premier, Hana Report
    rep_query_12 = """
    with_aliases(
        UploadRawData,"bmodata","bmofunding","hanadata",
        lambda bmodata, bmofunding, hanadata:
        Q(bmodata)
            .join(bmofunding, on=bmofunding.id == item["bmofundid"])
            .join(hanadata, on=hanadata.id == item["hana_id"])
            .find(func.replace(func.concat(bmodata.json_data["description"].astext,
            bmodata.json_data["net"].astext),'', '').label("DESCRIPTION"),
            literal("Funding").label("CATEGORY"),
            bmofunding.json_data["settlement_no"].astext.label("RUNID"),
            bmofunding.json_data["details"].astext.label("TYPE"),
            func.round(pg_safe_numeric(hanadata.json_data,"dr"),2).label("AMOUNT"),
            literal(item["ein"]).label("EIN"),
            bmodata.json_data["value_date"].astext.label("Value_Date"))
            .where(bmodata.id == item["bmoinputid"])
    )
    """


    # GET-PREMIER-INPUT-STAT-HANA-ACH-REJECT-Report
    # Generate report for premier, Hana Report
    rep_query_13 = """
    with_aliases(
        UploadRawData,"inputdata","achdata",
        lambda inputdata, achdata:
        Q(inputdata)
            .join(achdata, on=achdata.id == item["achid"])
            .find(literal("DEFR-260 DR").label("CATEGORY"),
            func.round(pg_safe_numeric(achdata.json_data,"amount"),2).label("AMOUNT"),
            literal(item["ein"]).label("EIN"),
            inputdata.json_data["description"].astext.label("DESCRIPTION"),
            inputdata.json_data["value_date"].astext.label("VALUE_DATE"))
            .where(inputdata.id == item["input_id"])
    )
    """
    # GET-PREMIER-DEDUCTION-GL-Report
    # Generate report for premier, Deduction GL Report
    rep_query_14 = """
    with_aliases(
        UploadRawData,"bmodata","bmofunding","dgldata",
        lambda bmodata, bmofunding, dgldata:
        Q(bmodata)
            .join(bmofunding, on=bmofunding.id == item["bmofundid"])
            .join(dgldata, on=dgldata.id == item["dgl_id"])
            .find(func.replace(func.concat(bmodata.json_data["description"].astext,
            bmodata.json_data["net"].astext),'', '').label("DESCRIPTION"),
            literal("Funding").label("CATEGORY"),
            bmofunding.json_data["settlement_no"].astext.label("RUNID"),
            bmofunding.json_data["details"].astext.label("TYPE"),
            func.round(pg_safe_numeric(dgldata.json_data,"sum_amount"),2).label("AMOUNT"),
            literal(item["ein"]).label("EIN"),
            bmodata.json_data["value_date"].astext.label("Value_Date"))
            .where(bmodata.id == item["bmoinputid"])
    )
    """

    # GET-PREMIER-INPUT-STAT-DEDUCTION-GL-ACH-REJECT-Report
    # Generate report for premier, DEDUCTION-GL-ACH Report
    rep_query_15 = """
    with_aliases(
        UploadRawData,"inputdata","achdata",
        lambda inputdata, achdata:
        Q(inputdata)
            .join(achdata, on=achdata.id == item["achid"])
            .find(literal("DEFR-260").label("CATEGORY"),
            func.round(pg_safe_numeric(achdata.json_data,"amount"),2).label("AMOUNT"),
            literal(item["ein"]).label("EIN"),
            inputdata.json_data["description"].astext.label("DESCRIPTION"),
            inputdata.json_data["value_date"].astext.label("VALUE_DATE"))
            .where(inputdata.id == item["input_id"])
    )
    """

    # GET-PREMIER-INPUT-STAT-DEDUCTION-GL-ACH-REJECT-210-Report
    # Generate report for premier, DEDUCTION-GL-ACH Report
    rep_query_16 = """
    with_aliases(
        UploadRawData,"inputdata","achdata",
        lambda inputdata, achdata:
        Q(inputdata)
            .join(achdata, on=achdata.id == item["achid"])
            .find(literal("DEFR-210").label("CATEGORY"),
            func.round(pg_safe_numeric(achdata.json_data,"amount"),2).label("AMOUNT"),
            literal(item["ein"]).label("EIN"),
            inputdata.json_data["description"].astext.label("DESCRIPTION"),
            inputdata.json_data["value_date"].astext.label("VALUE_DATE"))
            .where(inputdata.id == item["input_id"])
    )
    """

    # GET-PREMIER-CHECK-1-OUTSTANDING-Report
    rep_query_17 = """
    with_aliases(
        UploadRawData,"inputdata","netpaydata",
        lambda inputdata, netpaydata:
        Q(inputdata)
            .join(netpaydata, on=netpaydata.id == item["netpayid"])
            .find(const("Funding").label("CATEGORY"),
            func.replace(func.concat(inputdata.json_data["description"].astext,
            func.round(item["bmo_sum_amt"],2)*-1),'', '').label("DESCRIPTION"),
            func.round(pg_safe_numeric(netpaydata.json_data,"net_pay"),2).label("AMOUNT"),
            const(item["sr_no"]).label("RUNID"),
            const(item["employee_id"]).label("EIN"),
            const("BMO -Checks").as_("TYPE"),
            netpaydata.json_data["check_num"].astext.label("CHECK #"),
            inputdata.json_data["description"].astext.label("DESCRIPTION"),
            inputdata.json_data["value_date"].astext.label("VALUE_DATE"))
            .where(inputdata.id == item["bmoinputid"])
    )
    """

    # QuerySet Execution Logic
    async def main():
        async with async_session() as session:
            async with session.begin():
                stages = [
                    (InitStageFuncNoBatch, dict(taskName="Query#1",query=query_1)),
                    (StageFuncNoBatch, dict(taskName="Query#2",query=query_2)),
                    (StageFuncNoBatch, dict(taskName="Query#18",query=query_18)),
                    (StageFuncNoBatch, dict(taskName="Query#48",query=query_48)),
                    (ReportStageFunc, dict(taskName="rep_query_17",query=rep_query_17)),
                ]
                
                await run_pipeline(
                    session=session,
                    stages=stages
                )

if __name__ == "__main__":
    asyncio.run(main())
import asyncio
import datetime
import inspect
from typing import AsyncIterator, Any, Callable, List, TypeVar
from sqlalchemy.dialects import postgresql
from sqlalchemy import select, cast, Numeric, String
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from dbmodels.UploadRawData import UploadRawData
from dbmodels.ReconRuleScriptCfg import ReconRuleScriptCfg
from dbmodels.ReconRuleWorkflow import ReconRuleWorkflow, ReconWorkflow
from dbmodels.Client import Client
from sqlalchemy import select, text, func, literal, bindparam
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from backendservice.Query import Q, sum_, jcol, jnum, col, with_aliases, pg_safe_numeric, pg_json_text, add, fuzzy_match, const
from backendservice.Query import FromHanaBreakUp, FromNBFundingFile, FromInputStatement, FromLockBoxUploadfiles, FromLockBoxFile, FromNetPayRegistry, FromSTPV, FromReversalNetPayRegistry, FromAchRejectTracking, FromDeductionGL
from dbmodels.ReconReportBatch import ReconReportBatch
from dbmodels.ReconReport import ReconReport
from backendservice.ReportFormat import ReportFormat
from backendservice.UpdateDSL import Update
from dbmodels.FileUploads import FileUploads
from py_mini_racer import py_mini_racer
from decimal import Decimal

DATABASE_URL = ""
engine = create_async_engine(DATABASE_URL, echo=False, pool_size=5, max_overflow=10)
async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

T = TypeVar("T")
U = TypeVar("U")

StageFunc = Callable[[AsyncSession, AsyncIterator[Any]], AsyncIterator[Any]]

def param(name, default=None):
    return bp.value(default)

def getQueryObject(query, batch, offset, item):
    allowed_globals = {
        "FromBMOFundingFile": FromBMOFundingFile,
        "FromInputStatement": FromInputStatement,
        "FromLockBoxFile": FromLockBoxFile,
        "FromNetPayRegistry": FromNetPayRegistry,
        "FromReversalNetPayRegistry": FromReversalNetPayRegistry,
        "FromArchRejectTracking": FromArchRejectTracking,
        "FromHanaBreakUp": FromHanaBreakUp,
        "FromDeductionGL": FromDeductionGL,
        "FromSTPV": FromSTPV,
        "sum": sum,
        "batch size": batch,
        "offset": offset,
        "UploadRawData": UploadRawData,
        "literal": literal,
        "item": item,
        "input": item,
        "jcol": jcol,
        "jnum": jnum,
        "col": col,
        "q": q,
        "with aliases": with_aliases,
        "func": func,
        "pg_safe_numeric": pg_safe_numeric,
        "pg_json_text": pg_json_text,
        "Update": Update,
        "FromNBFundingFile": FromNBFundingFile,
        "add": add,
        "fuzzy_match": fuzzy_match,
        "cast": cast,
        "Numeric": Numeric,
        "String": String,
        "FileUploads": FileUploads,
        "const": const
    }
    #print(query)
    return eval(query, allowed_globals, {}) # nosec B307

async def StageFuncBatch(
    session: AsyncSession, *args
) -> AsyncIterator[List[dict]]:
    """Stage 1: Stream / paginate new orders"""
    offset = 0
    batch_size = 10

    if(args and args['batchsize']):
        batch_size = int(args['batchsize'])

    if(args and args['query']):
        query = str(args['query'])
        query = query.replace("\r\n", "").replace("\r", "").replace("\n", "").replace("\t", "")

    while True:
        #print("StageFuncBatch query ", query)
        qobj = getQueryObject(query, batch_size, offset, None)
        stmt = qobj.stmt
        #print(stmt.compile(dialect=postgresql.dialect, compile_kwargs={"literal_binds": True}))
        result = await session.execute(stmt)
        batch = result.all()

        if not batch:
            break

        batchx = [dict(x._mapping) for x in batch]
        #print(f"Stage - fetched {len(batch)} (offset={offset})")
        yield batchx
        offset += batch_size

async def InitStageFuncNoBatch(
    session: AsyncSession, _: AsyncSession[Any], **args
) -> AsyncIterator[List[dict]]:
    if(args and args['script']):
        query = str(args['script'])
        query = query.replace("\n", "").replace("\t", "").replace("\r", "")

    qobj = getQueryObject(query,0,0,None)
    stmt = qobj.stmt()
    #print(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    result = await session.execute(stmt)
    batch = result.all()

    batchx = [dict(x._mapping) for x in batch]
    #print(f"Stage -> fetched {len(batch)} (offset={offset})")
    yield batchx

async def run_pipeline(
    session: AsyncSession,
    stages: List[tuple[StageFunc, dict]]
):
    if not stages:
        print("No stages provided")
        return
    current_stream: AsyncIterator[Any] = iter([]).__iter__()  # empty async iterator

    for i, (stage_func, config) in enumerate(stages, 1):
        print(f"\n--- Starting stage {i}/{len(stages)}: {stage_func.__name__} --- (config: {config})")
        current_stream = stage_func(session, current_stream, **config)
        async for _ in current_stream:
            pass
    print("\nPipeline completed")

async def StageFuncNoBatch(
    session: AsyncSession, batches: AsyncIterator[List[Dict]], **args
) -> AsyncIterator[List[dict]]:
    if(args and args['script']):
        query = str(args['script'])
        query = query.replace("\n", "").replace("\t", "").replace("\r", "")
        taskName = args['taskName']
        async for batch in batches:
            for item in batch:
                resultdata = []
                qobj = getQueryObject(query, 0, 0, item)
                stmt = qobj.stmt()
                result = await session.execute(stmt)
                inputbatch = result.all()
                for x in inputbatch:
                    d = dict(x._mapping)
                    resultdata.append(d)
                print(taskName)
                print(resultdata)
                yield resultdata

async def ReportStageFunc(
    session: AsyncSession, batches: AsyncIterator[List[dict]], **args
) -> AsyncIterator[List[dict]]:
    if(args and args['script']):
        query = str(args['script'])
        query = query.replace("\r\n", "").replace("\r.", "").replace("\n", "").replace("\t", "")

    taskName = args['taskName']
    batchid = args['batchid']
    reporttype = args['reporttype']
    async for batch in batches:
        inputbatch = []
        for item in batch:
            inputbatch.append(item)
            resultdata = []
            qobj = getQueryObject(query,0,0,item)
            stmt = qobj.stmt
            result = await session.execute(stmt)
            nextbatch = result.all()
            for x in nextbatch:
                d = dict(x._mapping)
                print(d)
                resultdata.append(ReconReport(batchid=batchid , report_type=reporttype,
                    details=ReportFormat().getReportData(d)))
        #print(resultdata)
        await bulk_insert_report(resultdata)
        yield inputbatch

async def bulk_insert_report(
    items: List[ReconReport]) -> int:

    total = 0
    batch_size = 100
    async with async_session() as session:
        async with session.begin():
            for i in range(0, len(items), batch_size):
                batch = items[i : i + batch_size ]
                session.add_all(batch)
                await session.flush()
                total += len(batch)

        await session.commit()
    return total


async def update_report(item: List[int]) -> int:
    print("update_report ids ", item)
    async with async_session() as session:
        async with session.begin():
            u = ( Update(UploadRawData).
                set(cleared_status="C").
                where(UploadRawData.id.in_(item))
            )

        result = await session.execute(u.stmt())
        await session.commit()

    return 0

async def update_batch(batchId: int) -> int:

    async with async_session() as session:
        async with session.begin():
            u = (
                Update(ReconReportBatch)
                .set(report_status="Completed")
                .where(ReconReportBatch.batchid == batchId)
            )

            result = await session.execute(u.stmt())
            await session.commit()

    return 0


async def get_batch_details() -> int:

    async with async_session() as session:
        async with session.begin():
            reconBatch = await session.execute(select(ReconReportBatch)
                .where(ReconReportBatch.client_id == client_id,
                       ReconReportBatch.report_status == 'not_started').limit(1))
            reconBatches = reconBatch.scalars().all()

            return reconBatches


async def clear_recon(session: AsyncSession, batches: AsyncIterator[List[Dict]]) -> AsyncIterator[List[dict]]:
    async for batch in batches:
        inputbatch = []
        id_arr = []
        for item in batch:
            for k, v in item.items():
                print(k, v)
                if isinstance(v, int):
                    id_arr.append(int(v))
        await update_report(id_arr)
        yield inputbatch

async def start_pipeline(client_id,batchid,wkflid,clearflag):
    async with session.begin():
        workflowRules = await session.execute(
            select(ReconRuleScriptCfg)
            .join(ReconRuleWorkflow, ReconRuleWorkflow.ruleid==ReconRuleScriptCfg.id)
            .join(ReconRuleWorkflow, ReconRuleWorkflow.workflowid==ReconRuleWorkflow.workflowid)
            .join(Client, ReconRuleWorkflow.client_id == Client.id)
            .where(ReconRuleWorkflow.workflowid == wkflid)
            .order_by(ReconRuleWorkflow.run_seq)
        )
        ruleinput = workflowRules.all()
        tasks = []
        outstanding_tasks = []
        taskIdx = 0
        for rule in ruleinput:
            taskParam = {}
            taskParam['taskName'] = rule[0].rulename
            taskParam['script'] = f'{rule[0].script}'

            taskParam['batchid'] = batchid
            taskParam['scriptType'] = rule[0].script_type
            if(taskIdx == 0):
                tasks.append((InitStageFuncNoBatch, taskParam))
                taskIdx += 1
            elif("OUTSTANDING" in taskParam['taskName']):
                taskParam['reporttype'] = 'Outstanding'
                outstanding_tasks.append((ReportStageFunc, taskParam))
            elif("Report" in taskParam['taskName']):
                taskParam['reporttype'] = 'Cleared'
                tasks.append((ReportStageFunc, taskParam))
            elif("J" == taskParam['scriptType']):
                tasks.append((StageFuncJSExecutor, taskParam))
            elif("Q" == taskParam['scriptType']):
                taskParam['script'] = ''.join(taskParam['script'].splitlines())
                tasks.append((StageFuncNoBatch, taskParam))

        if(clearflag):
            tasks.append((clear_recon()))
        else:
            tasks.extend(outstanding_tasks)
        await run_pipeline(
            session=session,
            stages=tasks
        )

async def init_pipeline(client_id, workflowids, outstandingworkflowids):

    async with async_session() as session:
        async with session.begin():
            reconBatches = await get_batch_details()
            batchid = -1
            if len(reconBatches) != 0:
                batchid = reconBatches[0].batchid
            for id in workflowids:
                await start_pipeline(client_id, batchid, id, True)

            for id in outstandingworkflowids:
                await start_pipeline(client_id, batchid, id, False)

            print("update batch status for workflowid", workflowids)
            await update_batch(batchid)

async def StageFuncJSExecutor(
    session: AsyncSession, batches: AsyncIterator[List[Dict]], **args
) -> AsyncIterator[List[dict]]:
    
    if(args and args['script']):
        jsfunc = str(args['script'])
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
            result = await asyncio.to_thread(ctx.call, "doProcess", normalized_data)
            #print(result)
            yield [result]

if __name__ == "__main__":
    client_id = '12345'
    workflowids = [1,3,4,5,6,7,8,9,10,11,13]
    #workflowids = [1]
    outstandingworkflowids = [14]
    asyncio.run(init_pipeline(client_id, workflowids, outstandingworkflowids))






    


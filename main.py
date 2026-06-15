from security.jwt import jwt_bearer
import db.session
from db.init_db import init_db, seed_data, force_seed_data

import traceback
import connexion
from contextlib import asynccontextmanager
from starlette.middleware.cors import CORSMiddleware
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from starlette.applications import Starlette
from backendservice.scheduler import SchedulerManager, JobConfig ,schedule_function
from repository.SchedulerJobRepository import get_scheduled_jobs

scheduler_manager = SchedulerManager(max_worker_threads=5)

def initSchedulerManager():
    
    jobs = get_scheduled_jobs()
    job_definitions = []
    for j in jobs:
        job_definitions.append(
            JobConfig(
                id=str(j.id),
                func=schedule_function[j.handler],
                trigger_args={"minute": "*/"+str(j.min)} if j.hour == 0 else {"hour": str(j.hour), "minute": str(j.min)},
                max_instances=1,
                coalesce=False,
                misfire_grace_time=60,
                kwargs = {"clientId": j.client_id},
                use_lock=True
            ),
        )
    
    for job in job_definitions:
        scheduler_manager.register_job(job)
        
    scheduler_manager.start()


@asynccontextmanager
async def lifespan(app):
    init_db()
    seed_data()
    #force_seed_data()
    #initSchedulerManager()
    #app.state.scheduler_manager = scheduler_manager
    
    yield


def create_connexion_app():
    conn_app = connexion.AsyncApp(__name__)
    
    conn_app.add_api(
        "openapi.yaml",
        security_map={"JWTBearer": jwt_bearer},
        options={"allow_options": True, "swagger_ui": "PROD"},
        validate_responses=False,
    )
    
    return conn_app


# ---- Connexion app ----
conn_app = create_connexion_app()

# ---- OUTER Starlette app (CORS ALWAYS APPLIES) ----
app = Starlette(lifespan=lifespan)

app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4200",
        "http://127.0.0.1:4200",
        "https://happy-water-0e5473f00.6.azurestaticapps.net"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)

# ---- Mount Connexion ----
app.mount("/", conn_app)
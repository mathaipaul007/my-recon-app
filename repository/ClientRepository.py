from sqlalchemy.orm import Session
from db.session import SessionLocal
from dbmodels.User import User
from dbmodels.Client import Client
from dbmodels.ClientUser import ClientUser
from dto.ClientDTO import ClientDTO, PaginatedClientResponse
from sqlalchemy import select, func

def get_client_count():
    db: Session = SessionLocal()
    try:
        stmt = select(func.count()).select_from(Client)
        total = db.execute(stmt).scalar_one()

        return total
    finally:
        db.close()

def get_client(offset: int, limit: int):
    db: Session = SessionLocal()

    try:
        stmt = (
            select(Client)
            .order_by(Client.id.desc())
            .offset(offset)
            .limit(limit)
        )

        users = db.execute(stmt).scalars().all()
        return users

    finally:
        db.close()

def new_dbclient(client: Client):
    db: Session = SessionLocal()

    try:
        db.add(client)
        db.commit()
        db.refresh(client)
        return client

    finally:
        db.close()
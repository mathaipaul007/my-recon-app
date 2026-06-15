from sqlalchemy.orm import Session
from db.session import SessionLocal
from dbmodels.User import User
from dbmodels.Client import Client
from dbmodels.ClientUser import ClientUser
from sqlalchemy import select, func, and_

def get_clientuser_count(clientid: str):
    db: Session = SessionLocal()
    try:
        stmt = (
            select(func.count(User.id))
            .join(ClientUser, ClientUser.user_id == User.id)
            .join(Client, ClientUser.client_id == Client.id)
            .where(ClientUser.client_id == clientid)
        )
        total = db.execute(stmt).scalar_one()
        return total
    finally:
        db.close()

def get_clientuser(clientid: str, offset: int, limit: int):
    db: Session = SessionLocal()
    try:
        stmt = (
            select(User)
            .join(ClientUser, ClientUser.user_id == User.id)
            .join(Client, ClientUser.client_id == Client.id)
            .where(
                and_(
                    ClientUser.client_id == clientid
                )
            )
            .order_by(ClientUser.id.desc())
            .offset(offset)
            .limit(limit)
        )
        clientusers = db.execute(stmt).scalars().all()
        return clientusers
    finally:
        db.close()
def new_clientuser(clientUser: ClientUser):
    db: Session = SessionLocal()

    try:
        db.add(clientUser)
        db.commit()
        db.refresh(clientUser)
        return clientUser

    finally:
        db.close()


def remove_clientuser(client_user_id: int):
    db: Session = SessionLocal()

    try:
        user = db.query(ClientUser).filter(
            ClientUser.id == client_user_id
        ).first()

        if not user:
            raise Exception("Client user not found")

        db.delete(user)
        db.commit()

        return "success"

    finally:
        db.close()

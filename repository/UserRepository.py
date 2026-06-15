from sqlalchemy.orm import Session
from db.session import SessionLocal
from dbmodels.User import User
from dbmodels.Client import Client
from dbmodels.ClientUser import ClientUser
from dto.UserDTO import UserLoginDTO
from sqlalchemy import select, func

def get_users_count():
    db: Session = SessionLocal()
    try:
        stmt = select(func.count()).select_from(User)
        total = db.execute(stmt).scalar_one()
        
        return total
    finally:
        db.close()

def get_users(offset: int, limit: int):
    db: Session = SessionLocal()
    
    try:
        stmt = (
            select(User)
            .order_by(User.id.desc())
            .offset(offset)
            .limit(limit)
        )
        
        users = db.execute(stmt).scalars().all()
        return users
        
    finally:
        db.close()

def get_user_by_id(userId: int):
    db: Session = SessionLocal()
    
    try:
        stmt = (
            select(User)
            .where(User.id == userId)
        )
        
        user = db.execute(stmt).scalars().first()
        return user
        
    finally:
        db.close()

def update_dbuser(user: dict):
    db: Session = SessionLocal()
    
    try:
        dbuser = db.query(User).filter(User.id == user['id']).first()
        if not dbuser:
            raise Exception("User not found")
        
        dbuser.username = user['username']
        dbuser.eff_status = user['eff_status']
        dbuser.admin_user = user['admin_user']
        
        db.commit()
        db.refresh(dbuser)
        
        return dbuser
        
    finally:
        db.close()

def new_dbuser(user: UserLoginDTO):
    db: Session = SessionLocal()
    
    try:
        db.add(user)
        db.commit()
        db.refresh(user)
        return user
        
    finally:
        db.close()

def authenticate_user(user: UserLoginDTO):
    db: Session = SessionLocal()
    
    try:
        stmt = (
            select(User)
            .join(ClientUser, ClientUser.user_id == User.id)
            .join(Client, Client.id == ClientUser.client_id)
            .where(
                User.usermail == user.username,
                Client.id == user.client_id
            )
        )
        
        users = db.execute(stmt).scalars().all()
        return users
        
    finally:
        db.close()

def get_user_by_email(email: str):
    db: Session = SessionLocal()
    
    try:
        stmt = (
            select(User)
            .where(
                User.usermail == email
            )
        )
        
        users = db.execute(stmt).scalars().all()
        return users[0]
        
    finally:
        db.close()
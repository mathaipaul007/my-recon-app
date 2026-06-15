from fastapi import HTTPException, status
from connexion import request
from jose import JWTError, jwt
from datetime import datetime, timedelta, timezone
import bcrypt
from repository.UserRepository import get_user_by_email

SECRET_KEY = "YOUR_SECRET_KEY" # Keep this secret & secure
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

def jwt_bearer(token_info):
    try:
        token = token_info # already extracted
        #payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return decode_access_token(token)
        
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid token")

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES or 15)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def hash_password(password: str) -> str:
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')

def decode_access_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=ALGORITHM)
        usermail = payload.get("usermail")
        clientid = payload.get("clientid")

        db_user = get_user_by_email(usermail)

        if db_user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token: missing subject",
                headers={"WWW-Authenticate": "Bearer"},
            )

        usersession = {"username": usermail, "clientid": clientid, "userid": db_user.id}
        return usersession
        
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
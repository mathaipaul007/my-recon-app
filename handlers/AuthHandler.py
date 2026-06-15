from dto.UserDTO import UserLoginDTO
from repository.UserRepository import authenticate_user
from connexion.exceptions import ProblemException
from passlib.context import CryptContext
from dbmodels.User import User
from security.jwt import create_access_token

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def login(body):
    userDTO = UserLoginDTO(**body)
    user = authenticate_user(userDTO)

    if not user or not verify_password(userDTO.password, user[0].password):
        raise ProblemException(
            status=400,
            title="Authentication failed",
            detail="Invalid username or password"
        )

    access_token = create_access_token(data={"usermail": user[0].usermail, "clientid": userDTO.client_id})

    return {"access_token": access_token}

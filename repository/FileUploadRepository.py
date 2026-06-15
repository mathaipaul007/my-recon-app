from sqlalchemy.orm import Session
from db.session import SessionLocal
from dbmodels.FileUploads import FileUploads
from sqlalchemy import select, func, update
from dto.FileUploadDTO import FileUploadDTO
from sqlalchemy.orm import load_only

def get_file_uploads_count():
    db: Session = SessionLocal()
    try:
        stmt = select(func.count()).select_from(FileUploads)
        total = db.execute(stmt).scalar_one()
        return total
    finally:
        db.close()

def get_file_uploads_by_status(clientid: str, status: str):
    db: Session = SessionLocal()
    try:
        stmt = select(FileUploads).where(FileUploads.client_id == clientid,
                                         FileUploads.upload_file_status == status)
        fileuploads = db.scalars(stmt).first()
        return fileuploads
    finally:
        db.close()

def get_all_file_uploads_by_status(clientid: str, status: str):
    db: Session = SessionLocal()
    try:
        stmt = select(FileUploads).where(FileUploads.client_id == clientid,
                                         FileUploads.upload_file_status == status)
        fileuploads = db.execute(stmt).scalars().all()
        return fileuploads
    finally:
        db.close()

def get_file_uploads(offset: int, limit: int):
    db: Session = SessionLocal()

    try:
        stmt = (
            select(FileUploads)
            .options(
                load_only(
                    FileUploads.id,
                    FileUploads.client_id,
                    FileUploads.user_id,
                    FileUploads.recon_file_type,
                    FileUploads.recon_file_desc,
                    FileUploads.upload_file_name,
                    FileUploads.upload_file_type,
                    FileUploads.upload_file_status
                )
            )
            .order_by(FileUploads.id.desc())
            .offset(offset)
            .limit(limit)
        )
        fu = db.execute(stmt).scalars().all()
        return fu
    finally:
        db.close()

def new_file_uploads(fileUploadDTO: FileUploadDTO):
    db: Session = SessionLocal()
    db_file_uploads = FileUploads(**fileUploadDTO.model_dump())
    try:
        db.add(db_file_uploads)
        db.commit()
        db.refresh(db_file_uploads)
        return db_file_uploads
    finally:
        db.close()

def update_file_status(fileuploads: FileUploads, status: str):
    db: Session = SessionLocal()

    try:
        stmt = (
            update(FileUploads)
            .where(FileUploads.id == fileuploads.id)
            .values(upload_file_status=status)
            .execution_options(synchronize_session="fetch")
        )

        db.execute(stmt)
        db.commit()

    finally:
        db.close()


def get_upload_file(file_id: int):
    db: Session = SessionLocal()

    try:
        stmt = (
            select(FileUploads)
            .where(FileUploads.id == file_id)
        )
        fu = db.execute(stmt).scalar_one_or_none()

        return fu

    finally:
        db.close()

from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List
from datetime import datetime
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, DateTime
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import logging
from logging.handlers import RotatingFileHandler
import os

# =========================================================
# LOGGING CONFIGURATION
# =========================================================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "admin_app.log")

file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5)
console_handler = logging.StreamHandler()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[file_handler, console_handler],
)

logger = logging.getLogger(__name__)
logger.info(" Logging initialized for Admin API")

# =========================================================
# DATABASE CONFIGURATION
# =========================================================
DATABASE_URL_1 = "postgresql://postgres:Root%40123@localhost:5432/STATE_DB_Test"
DATABASE_URL_2 = "postgresql://postgres:Root%40123@localhost:5432/RVSK_DB_Test"

engine1 = create_engine(DATABASE_URL_1)
engine2 = create_engine(DATABASE_URL_2)

SessionLocal1 = sessionmaker(autocommit=False, autoflush=False, bind=engine1)
SessionLocal2 = sessionmaker(autocommit=False, autoflush=False, bind=engine2)

Base = declarative_base()

# =========================================================
# SQLALCHEMY MODEL
# =========================================================
class AdminDB(Base):
    __tablename__ = "admin"

    Udise_code = Column(Integer, primary_key=True, index=True)
    State_ID = Column(Integer)
    State_Name = Column(String(200))
    District_ID = Column(Integer)
    District_name = Column(String(200))
    Block_ID = Column(Integer)
    Block_name = Column(String(200))
    Cluster_ID = Column(Integer)
    Cluster_name = Column(String(200))
    School_location_type = Column(Integer)
    School_Name = Column(String(300))
    Latitude = Column(Float)
    Longitude = Column(Float)
    School_management_type = Column(String(150))
    Type_of_school = Column(Integer)
    School_category_code = Column(Integer)
    Is_Active = Column(String(10))
    School_classification = Column(Integer)
    Minority_managed = Column(Integer)
    lowest_class_in_school = Column(Integer)
    Highest_class_in_school = Column(Integer)
    Total_Students = Column(Integer)
    Total_Students_Boys = Column(Integer)
    Total_Students_girls = Column(Integer)
    Total_Teachers = Column(Integer)
    Total_Teachers_Male = Column(Integer)
    Total_Teachers_Female = Column(Integer)
    Total_Teachers_Transgender = Column(Integer)

    # Infrastructure / Facilities
    Internet_Availability = Column(Integer)
    Electricity_Availability = Column(Integer)
    Smart_Classrooms_Availability = Column(Integer)
    Toilet_Availability = Column(Integer)
    Total_Boys_Toilet = Column(Integer)
    Total_Girls_Toilet = Column(Integer)
    Drinking_Water_Availability = Column(Integer)
    Boundary_wall_type = Column(Integer)
    Fire_extinguisher_available = Column(Integer)
    year_of_establishment = Column(DateTime)
    Free_uniform = Column(Integer)
    Free_Textbook_primary = Column(Integer)
    Free_Textbook_upper_primary = Column(Integer)
    Actual_Teaching_Days = Column(Integer)
    SMC_Formation_date = Column(DateTime)
    SMC_Status = Column(String(50))
    Total_SMC_Members = Column(Integer)
    Is_ICT_lab = Column(Integer)
    Total_Laptops = Column(Integer)
    Total_Functional_desktops = Column(Integer)
    Total_Functional_laptops = Column(Integer)
    Total_Functional_Tablets = Column(Integer)
    Total_Functional_digital_boards = Column(Integer)
    Total_Functional_projectors = Column(Integer)

    # Audit / Tracking
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    ingested_at = Column(DateTime, default=datetime.utcnow)
    Timestamp = Column(DateTime)


# =========================================================
# PYDANTIC MODEL
# =========================================================
class Admin(BaseModel):
    Udise_code: int
    State_ID: int
    State_Name: str
    District_ID: int
    District_name: str
    Block_ID: int
    Block_name: str
    Cluster_ID: int
    Cluster_name: str
    School_location_type: int
    School_Name: str
    Latitude: float
    Longitude: float
    School_management_type: str
    Type_of_school: int
    School_category_code: int
    Is_Active: str
    School_classification: int
    Minority_managed: int
    lowest_class_in_school: int
    Highest_class_in_school: int
    Total_Students: int
    Total_Students_Boys: int
    Total_Students_girls: int
    Total_Teachers: int
    Total_Teachers_Male: int
    Total_Teachers_Female: int
    Total_Teachers_Transgender: int

    Internet_Availability: int
    Electricity_Availability: int
    Smart_Classrooms_Availability: int
    Toilet_Availability: int
    Total_Boys_Toilet: int
    Total_Girls_Toilet: int
    Drinking_Water_Availability: int
    Boundary_wall_type: int
    Fire_extinguisher_available: int
    year_of_establishment: datetime
    Free_uniform: int
    Free_Textbook_primary: int
    Free_Textbook_upper_primary: int
    Actual_Teaching_Days: int
    SMC_Formation_date: datetime
    SMC_Status: str
    Total_SMC_Members: int
    Is_ICT_lab: int
    Total_Laptops: int
    Total_Functional_desktops: int
    Total_Functional_laptops: int
    Total_Functional_Tablets: int
    Total_Functional_digital_boards: int
    Total_Functional_projectors: int

    created_at: datetime
    updated_at: datetime
    ingested_at: datetime
    Timestamp: datetime

    class Config:
        from_attributes = True


# =========================================================
# DATABASE SESSION DEPENDENCIES
# =========================================================
def get_db1():
    db = SessionLocal1()
    try:
        yield db
    finally:
        db.close()


def get_db2():
    db = SessionLocal2()
    try:
        yield db
    finally:
        db.close()


# =========================================================
# FASTAPI INITIALIZATION
# =========================================================
app = FastAPI(title="Admin API", version="1.0")

@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine1)
    Base.metadata.create_all(bind=engine2)
    logger.info("Admin tables ensured in both databases")


# =========================================================
# BULK INSERT ENDPOINT
# =========================================================
@app.post("/admin/bulk", response_model=List[Admin])
def create_admin_records(
    admins: List[Admin],
    db1: Session = Depends(get_db1),
    db2: Session = Depends(get_db2)
):
    BATCH_SIZE = 1000
    total = len(admins)
    created = []

    logger.info(f"Starting Admin bulk insert: {total} records (batch size {BATCH_SIZE})")

    try:
        for i in range(0, total, BATCH_SIZE):
            batch = admins[i:i + BATCH_SIZE]
            dict_batch = [a.dict() for a in batch]

            db1.bulk_insert_mappings(AdminDB, dict_batch)
            db2.bulk_insert_mappings(AdminDB, dict_batch)

            db1.commit()
            db2.commit()

            created.extend(batch)
            logger.info(f"Inserted batch {i // BATCH_SIZE + 1} ({len(batch)} records)")

    except Exception as e:
        db1.rollback()
        db2.rollback()
        logger.exception(f" Error during Admin batch insert: {e}")
        raise HTTPException(status_code=500, detail=f"Error inserting admins: {str(e)}")

    logger.info("Admin bulk insert completed successfully")
    return created


# =========================================================
# READ ENDPOINTS
# =========================================================
@app.get("/admin/", response_model=List[Admin])
def get_all_admins(db1: Session = Depends(get_db1)):
    logger.info("Fetching all Admin records from DB1")
    return db1.query(AdminDB).all()


@app.get("/admin/{udise_code}", response_model=Admin)
def get_admin(udise_code: int, db1: Session = Depends(get_db1)):
    logger.info(f"Fetching Admin record with UDISE {udise_code}")
    admin = db1.query(AdminDB).filter(AdminDB.Udise_code == udise_code).first()
    if not admin:
        logger.warning(f"Admin record not found for UDISE {udise_code}")
        raise HTTPException(status_code=404, detail="Admin record not found")
    return admin

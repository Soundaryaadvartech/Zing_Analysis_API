from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import os
import urllib.parse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_USER = urllib.parse.quote_plus(os.getenv("DB_USER"))
DB_PASSWORD = urllib.parse.quote_plus(os.getenv("DB_PASSWORD"))
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")

DB_URL =  f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"

engine = create_engine(DB_URL)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()

# Dependency to get a session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

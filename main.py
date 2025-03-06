from fastapi import FastAPI
from routers.router import router
from database.database import Base, engine

app = FastAPI(title="Inventory Summary")

app.include_router(router, prefix="/api")
Base.metadata.create_all(bind = engine)
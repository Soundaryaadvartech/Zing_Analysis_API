import traceback
import json
from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session
from sqlalchemy import distinct
from fastapi.responses import JSONResponse
from database.database import get_db
from utilities.utils import generate_inventory_summary
from database.models import Item

router = APIRouter()

@router.get("/inventory_summary")
def inventory_summary(days: int, days_to_predict: int, db:Session = Depends(get_db)):
    try:
        summary_df = generate_inventory_summary(db, days, days_to_predict)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content = json.loads(summary_df.to_json(orient="records"))
        )
    
    except Exception:
        traceback.print_exc()
        return  JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content = {"message":"Something went wrong"}
        )

@router.get("/unique_values")
def unique_values(db: Session = Depends(get_db)):
    try:
        # Fetch distinct values for each column
        unique_values = {
            "Item_Name": [row[0] for row in db.query(distinct(Item.Item_Name)).all() if row[0] is not None],
            "Item_Type": [row[0] for row in db.query(distinct(Item.Item_Type)).all() if row[0] is not None],
            "Category": [row[0] for row in db.query(distinct(Item.Category)).all() if row[0] is not None],
            "Colour" : [row[0] for row in db.query(distinct(Item.Colour)).all() if row[0] is not None],
            "__Batch": [row[0] for row in db.query(distinct(Item.batch)).all() if row[0] is not None],
            "Fabric": [row[0] for row in db.query(distinct(Item.Fabric)).all() if row[0] is not None],
            "Fit" : [row[0] for row in db.query(distinct(Item.Fit)).all() if row[0] is not None],
            "Neck" : [row[0] for row in db.query(distinct(Item.Neck)).all() if row[0] is not None],
            "Occasion": [row[0] for row in db.query(distinct(Item.Occasion)).all() if row[0] is not None],
            "Print": [row[0] for row in db.query(distinct(Item.Print)).all() if row[0] is not None],
            "Size" : [row[0] for row in db.query(distinct(Item.Size)).all() if row[0] is not None],
            "Sleeve": [row[0] for row in db.query(distinct(Item.Sleeve)).all() if row[0] is not None],
            "Mood" : [row[0] for row in db.query(distinct(Item.mood)).all() if row[0] is not None]
        }
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content = unique_values
        )
    except Exception:
        traceback.print_exc()
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"message":"Something Went Wrong"}
        )

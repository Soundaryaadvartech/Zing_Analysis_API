import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import func
from database.models import Item, Sale, ViewsAtc

def generate_inventory_summary(db: Session, days: int, days_to_predict: int):
    """
    Generates an inventory summary using SQLAlchemy ORM.
    """

    # Query 1: Fetch all items
    items = db.query(Item.Item_Id, Item.Item_Name, Item.Item_Type, Item.Category, 
                     Item.Current_Stock, Item.launch_date, Item.Sale_Price, 
                     Item.Sale_Discount, Item.batch).all()
    t1 = pd.DataFrame(items, columns=["Item_Id", "Item_Name", "Item_Type", "Category", 
                                      "Current_Stock", "__Launch_Date", "Sale_Price", 
                                      "Sale_Discount", "__Batch"])

    # Query 2: Fetch sales data
    sales = db.query(Sale.Item_Id, Sale.Date, Sale.Quantity, Sale.Total_Value).all()
    t2 = pd.DataFrame(sales, columns=["Item_Id", "Date", "Quantity", "Total_Value"])

    # Query 3: Fetch views and add-to-cart data
    viewsatc = db.query(ViewsAtc.Item_Id, ViewsAtc.Date, ViewsAtc.Items_Viewed, ViewsAtc.Items_Addedtocart).all()
    t3 = pd.DataFrame(viewsatc, columns=["Item_Id", "Date", "Items_Viewed", "Items_Addedtocart"])

    # Query 4: Fetch first sold date
    first_sold_dates = (
        db.query(Sale.Item_Id, func.min(Sale.Date).label("First_Sold_Date"))
        .group_by(Sale.Item_Id)
        .all())    
    t4 = pd.DataFrame(first_sold_dates, columns=["Item_Id", "First_Sold_Date"])

    last_sold_dates = (
        db.query(Sale.Item_Id, func.max(Sale.Date).label("Last_Sold_Date"))
        .group_by(Sale.Item_Id)
        .all()
    )
    t5 = pd.DataFrame(last_sold_dates, columns=["Item_Id", "Last_Sold_Date"])

    # Convert date columns to datetime format
    t1["__Launch_Date"] = pd.to_datetime(t1["__Launch_Date"])
    t1["Item_Id"] = t1["Item_Id"].astype("int")
    t1["Sale_Price"] = t1["Sale_Price"].astype("int")
    t1["Current_Stock"] = t1["Current_Stock"].astype("int")
    t1["Sale_Discount"] = t1["Sale_Discount"].astype("int")
    t5["Item_Id"] = t5["Item_Id"].astype("int")
    t5["Last_Sold_Date"] = pd.to_datetime(t5["Last_Sold_Date"])

    # Merge First Sold Date
    t1 = pd.merge(t1, t4, how="left", on="Item_Id")
    t1["__Launch_Date"].fillna(t1["First_Sold_Date"], inplace=True)

    # Comprehensive Item Summary Function
    def get_item_summary(t1, t2, t3, days):
        # Convert dates to datetime
        t1['__Launch_Date'] = pd.to_datetime(t1['__Launch_Date'])
        t2['Date'] = pd.to_datetime(t2['Date'])
        t3['Date'] = pd.to_datetime(t3['Date'])
        t3["Item_Id"] = t3["Item_Id"].astype("int")

        # Get minimum launch date for each Item_Name
        item_min_launch_date = t1.groupby(['Item_Name',"Item_Type","Category"])['__Launch_Date'].min().reset_index()
        t1 = t1.merge(item_min_launch_date, on='Item_Name', suffixes=('', '_Min'))

        # Calculate date range
        t1['Start_Date'] = t1['__Launch_Date_Min']
        t1['End_Date'] = t1['Start_Date'] + pd.to_timedelta(days, unit='D')

        # Filter data based on date range
        t2 = t2.merge(t1[['Item_Id', 'Item_Name','Item_Type','Category', 'Start_Date', 'End_Date']], on='Item_Id', how='inner')
        t3 = t3.merge(t1[['Item_Id', 'Item_Name','Item_Type','Category', 'Start_Date', 'End_Date']], on='Item_Id', how='inner')

        t2_filtered = t2[(t2['Date'] >= t2['Start_Date']) & (t2['Date'] <= t2['End_Date'])]
        t3_filtered = t3[(t3['Date'] >= t3['Start_Date']) & (t3['Date'] <= t3['End_Date'])]

        # Aggregate data
        t1_agg = t1.groupby(['Item_Name','Item_Type','Category'], as_index=False)['Current_Stock'].sum()
        t2_agg = t2_filtered.groupby(['Item_Name','Item_Type','Category'], as_index=False)[['Quantity', 'Total_Value']].sum()
        t3_agg = t3_filtered.groupby(['Item_Name','Item_Type','Category'], as_index=False)[['Items_Viewed', 'Items_Addedtocart']].sum()

        # Merge aggregated values
        final_df = t1_agg.merge(t2_agg, on=['Item_Name','Item_Type','Category'], how='left')
        final_df = final_df.merge(t3_agg, on=['Item_Name','Item_Type','Category'], how='left')
        final_df = final_df.fillna(0)

        return final_df

    # Generate initial summary
    df = get_item_summary(t1, t2, t3, days)

    # Calculate total quantity sold per item
    temp_t2 = t2.groupby("Item_Id").agg({"Quantity": "sum"}).rename(columns={"Quantity": "Alltime_Total_Quantity"}).reset_index()
    temp_merged = pd.merge(t1, temp_t2, how="left", on="Item_Id")
    temp_quan = temp_merged.groupby(['Item_Name','Item_Type','Category']).agg({"Alltime_Total_Quantity":"sum"}).reset_index()
    temp_curr = t1.groupby(['Item_Name','Item_Type','Category']).agg({"Current_Stock":"sum","Sale_Discount":"mean"}).reset_index()

    # Calculate total stock
    temp_total = pd.merge(temp_curr, temp_quan, how="inner", on=['Item_Name','Item_Type','Category'])
    temp_total["Total_Stock"] = temp_total["Alltime_Total_Quantity"] + temp_total["Current_Stock"]
    
    
    # Merge and calculate metrics
    df_final = df.merge(temp_total[["Item_Name","Item_Type","Category","Total_Stock","Alltime_Total_Quantity","Sale_Discount"]], how="left", on=['Item_Name','Item_Type','Category'])
    df_final["Stock_Sold_Percentage"] = round((df_final["Quantity"]/df_final["Total_Stock"] * 100),2).fillna(0)
    

    # Add additional details
    t1_unique = t1[["Item_Name", "Item_Type","Category"]].drop_duplicates()
    
    df_final = pd.merge(df_final, t1_unique, how="inner", on=['Item_Name','Item_Type','Category'])
    t1_Launch = t1.groupby(['Item_Name','Item_Type','Category']).agg({"__Launch_Date":"min"}).reset_index()
    t1_Launch['days_since_launch'] = (pd.to_datetime('today') - t1_Launch['__Launch_Date']).dt.days
    df_final = pd.merge(df_final, t1_Launch, how="inner", on=['Item_Name','Item_Type',"Category"])
    t3_total = t3.groupby(["Item_Id"]).agg({"Items_Viewed":"sum","Items_Addedtocart":"sum"}).rename(columns={"Items_Addedtocart": "Alltime_Items_Addedtocart","Items_Viewed":"Alltime_Items_Viewed"}).reset_index()
    t3_toatl = pd.merge(t1, t3_total, how="left", on="Item_Id")
    
    temp_t3 = t3_toatl.groupby(['Item_Name','Item_Type','Category']).agg({"Alltime_Items_Addedtocart":"sum","Alltime_Items_Viewed":"sum"}).reset_index()
    
    df_final = df_final.merge(temp_t3[["Item_Name","Item_Type","Category","Alltime_Items_Addedtocart","Alltime_Items_Viewed"]], how="left", on=['Item_Name','Item_Type','Category'])
    t1_sale_price = t1.groupby(['Item_Name','Item_Type','Category']).agg({"Sale_Price":"mean"}).reset_index()
    
    df_final = pd.merge(df_final, t1_sale_price, how="inner", on=['Item_Name','Item_Type','Category'])
    

    

    # Get minimum Item_Id for each Item_Name
    t1_min_id = t1.groupby(['Item_Name','Item_Type','Category']).agg({"Item_Id":"min"}).reset_index()
    df_final = pd.merge(df_final, t1_min_id, how="inner", on=['Item_Name','Item_Type','Category'])
        
    df_final = df_final.merge(t5,how="left",on = "Item_Id")
    
    df_final['Days_Sold_Out_Past'] = df_final.apply(
                lambda row: (row['Last_Sold_Date'] - row['__Launch_Date']).days if row['Current_Stock'] == 0 else 0,
                    axis=1
                            ).fillna(0)
    df_final["Alltime_perday_Quantity"] = (np.where(
                                                    df_final["Alltime_Total_Quantity"] == 0, 
                                                    round(df_final["Alltime_Total_Quantity"] / df_final["Days_Sold_Out_Past"], 2), 
                                                    round(df_final["Alltime_Total_Quantity"] / df_final["days_since_launch"], 2)
                                                ))
    df_final.Alltime_perday_Quantity.fillna(0,inplace=True)
    
    
    # Calculate stock values
    df_final["Alltime_Total_Quantity_Value"] =df_final["Alltime_Total_Quantity"]* (df_final["Sale_Price"] *((100-df_final["Sale_Discount"])/100)).fillna(0)
    df_final["Current_Stock_Value"] = df_final["Current_Stock"] * (df_final["Sale_Price"] *((100-df_final["Sale_Discount"])/100)).fillna(0)
    df_final.rename(columns={"Quantity":"Quantity_sold", "Total_Value":"Sold_Quantity_Value"}, inplace=True)
    df_final["Total_Stock_Value"] = ((df_final["Sale_Price"] *((100-df_final["Sale_Discount"])/100))  * df_final["Total_Stock"]).fillna(0)
    
    df_final["Alltime_perday_View"] = round((df_final["Alltime_Items_Viewed"]/df_final["days_since_launch"]),2).fillna(0)
    df_final["Alltime_perday_atc"] = round((df_final["Alltime_Items_Addedtocart"]/df_final["days_since_launch"]),2).fillna(0)
    df_final["Total_Stock_Sold_Percentage"] = round((df_final["Alltime_Total_Quantity"]/df_final["Total_Stock"] *100),2).fillna(0)
    df_final["perday_view"] = np.where(df_final['days_since_launch'] > days, df_final['Items_Viewed'] / days, df_final['Items_Viewed'] / df_final["days_since_launch"])
    df_final.perday_view.fillna(0,inplace=True)
    df_final["perday_atc"] =np.where(df_final['days_since_launch'] > days, df_final['Items_Viewed'] / days, df_final['Items_Addedtocart'] / df_final["days_since_launch"])
    df_final.perday_atc.fillna(0,inplace=True)
    df_final["Projected_Days_to_Sellout"] = df_final["Current_Stock"]/df_final["Alltime_perday_Quantity"]
    column_name = f"Predicted_Quantity_Next{days_to_predict}Days"
    df_final[column_name] = np.where(
                    df_final["Current_Stock"] != 0, 
                    df_final["Alltime_perday_Quantity"] * days_to_predict, 
                    0  # If Current_Stock is 0, prediction will be set to 0
                )
    
    
    t1_final = t1.groupby(["Item_Name", "Item_Type","Category"]).apply(
            lambda group: pd.Series({
                'Sale_Price_After_Discount': (group['Sale_Price'] * ((100 - group['Sale_Discount']) / 100)).mean(),
                'Sale_Discounts': ','.join(group['Sale_Discount'].unique().astype(str))  # Use unique() to avoid duplicates
            })
        ).reset_index()
    
    df_final = df_final.merge(t1_final,how="left",on=["Item_Name","Item_Type","Category"])
    
    


    # Final selection of columns
    df_done = df_final[[
                        "Item_Id", 
                        "Item_Name", 
                        "Item_Type", 
                        "Category", 
                        "__Launch_Date", 
                        "days_since_launch", 
                        "Projected_Days_to_Sellout", 
                        "Days_Sold_Out_Past",
                        "Current_Stock", 
                        "Total_Stock", 
                        "Current_Stock_Value", 
                        "Total_Stock_Value",
                        "Sale_Price", 
                        "Sale_Discounts", 
                        "Sale_Price_After_Discount", 
                        "Quantity_sold", 
                        "Sold_Quantity_Value", 
                        "Alltime_Total_Quantity", 
                        "Alltime_Total_Quantity_Value",
                        "Alltime_perday_Quantity",  
                        "Items_Viewed", 
                        "perday_view", 
                        "Alltime_Items_Viewed", 
                        "Alltime_perday_View", 
                        "Items_Addedtocart", 
                        "perday_atc", 
                        "Alltime_Items_Addedtocart", 
                        "Alltime_perday_atc",
                        "Stock_Sold_Percentage", 
                        "Total_Stock_Sold_Percentage",column_name]]
    
    
                    


    numeric_cols = df_done.select_dtypes(include=['number']).columns
    df_done[numeric_cols] = df_done[numeric_cols].round(2)
    df_done["__Launch_Date"] = df_done["__Launch_Date"].dt.strftime('%Y-%m-%d')   


    return df_done.sort_values(by="Item_Id").reset_index(drop=True)



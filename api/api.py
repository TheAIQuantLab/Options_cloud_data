from fastapi import FastAPI, Query
from typing import List, Optional
import boto3
from boto3.dynamodb.conditions import Key, Attr
import os

app = FastAPI()

ACCESS_KEY = os.environ['ACCESS_KEY']
SECRET_KEY = os.environ['SECRET_KEY']

# DynamoDB config
dynamodb = boto3.resource(
    'dynamodb',
    region_name='eu-north-1',
    aws_access_key_id=ACCESS_KEY
    aws_secret_access_key=SECRET_KEY
)

TABLE_NAME = 'options_data'
table = dynamodb.Table(TABLE_NAME)

@app.get("/execution-days")
def get_execution_days():
    """
    Return all unique execution dates in the table.
    """
    response = table.scan(ProjectionExpression="execution_date")
    dates = {item['execution_date'] for item in response.get("Items", [])}
    return sorted(dates)

@app.get("/ivs")
def get_ivs(
    execution_date: str = Query(...),
    type_cp: Optional[str] = Query(None),
    T: Optional[float] = Query(None)
):
    """
    Get IVs filtered by execution_date, type_CP, and T (optional).
    """
    filter_expr = Attr("execution_date").eq(execution_date)
    if type_cp:
        filter_expr &= Attr("type_CP").eq(type_cp)
    if T is not None:
        filter_expr &= Attr("T").eq(T)

    response = table.scan(FilterExpression=filter_expr)
    items = response.get("Items", [])

    return [
        {
            "id": item["id"],
            "execution_date": item["execution_date"],
            "type_CP": item["type_CP"],
            "T": item["T"],
            "IV": item["IV"]
        }
        for item in items
    ]

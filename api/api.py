from fastapi import FastAPI, Query
from typing import Optional, List
import boto3
from boto3.dynamodb.conditions import Attr
import os
from mangum import Mangum

# Initialize FastAPI
app = FastAPI(title="Options Data API")
handler = Mangum(app)

# AWS credentials (use environment variables for security in production)
AWS_REGION = 'eu-north-1'
AWS_ACCESS_KEY = os.environ['ACCESS_KEY']
AWS_SECRET_KEY = os.environ['SECRET_KEY']
TABLE_NAME = 'meff_options'

# Initialize DynamoDB client
dynamodb = boto3.resource(
    'dynamodb',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

table = dynamodb.Table(TABLE_NAME)

print(dynamodb.tables.all())


@app.get("/execution-days",
        response_model=List[str])
def get_execution_days():
    """
    Returns a sorted list of unique execution dates from the table.
    """
    response = table.scan(ProjectionExpression="execution_date")
    items = response.get("Items", [])
    unique_dates = sorted({item["execution_date"] for item in items})
    return unique_dates


@app.get("/expiration_dates", response_model=List[str])
def get_expiration_dates(execution_date: str = Query(...,
                         description="Execution date to filter")):
    """
    Returns a sorted list of unique expiration dates from the table.
    """
    filter_expr = Attr("execution_date").eq(execution_date)
    response = table.scan(  FilterExpression=filter_expr,
                            ProjectionExpression="expiration_date")
    items = response.get("Items", [])
    unique_dates = sorted({item["expiration_date"] for item in items})
    return unique_dates


@app.get("/ivs")
def get_ivs(
    execution_date: str = Query(...,
                                description="Execution date to filter"),
    type_cp: Optional[str] = Query(None,
                                    description="Type CP to filter"),
    expiration_date: Optional[str] = Query(None,
                                            description="expiration_date to filter")
):
    """
    Returns filtered IVs based on execution_date,
    and optionally by type_CP and T.
    """
    filter_expr = Attr("execution_date").eq(execution_date)
    if type_cp:
        filter_expr = filter_expr & Attr("type_CP").eq(type_cp)
    if expiration_date:
        aux = Attr("expiration_date").eq(expiration_date)
        filter_expr = filter_expr & aux

    response = table.scan(FilterExpression=filter_expr)
    items = response.get("Items", [])

    return [
        {
            "id": item["id"],
            "execution_date": item["execution_date"],
            "expiration_date": item["expiration_date"],
            "type_CP": item["type_CP"],
            "strike_price": item["strike_price"],
            "T": item["T"],
            "IV": item["IV"]
        }
        for item in items
    ]

"""
Extract transactions from a file
"""

from typing import List, Dict
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from pydantic import BaseModel
from fastapi.requests import Request

# connection string to database
CONNECTION_STRINGS = "postgresql://postgres:postgres@jetson.local:5432/saokedb"
engine = create_engine(CONNECTION_STRINGS, echo=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def validate(q):
    """
    validate a query
    """

    if not q:
        raise HTTPException(
            status_code=400,
            detail={
                "status_code": 400,
                "message": "Query is empty. Please provide a query to search for transactions. Such as /?q=abc",
                "status": "Bad Request",
                "total": 0,
                "result": [],
            },
        )

    if len(q) < 3:
        raise HTTPException(
            status_code=400,
            detail={
                "status_code": 400,
                "message": "Query is too short, should be at least 3 characters",
                "status": "Bad Request",
                "total": 0,
                "result": [],
            },
        )

    if len(q) > 50:
        raise HTTPException(
            status_code=400,
            detail={
                "status_code": 400,
                "message": "Query is too long, should be at most 50 characters",
                "status": "Bad Request",
                "total": 0,
                "result": [],
            },
        )

    return True


# create a FastAPI app
app = FastAPI()

# origins that are allowed to make requests
# replace this with the actual origin
origins = ['*']
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# @app.middleware("http")
# async def custom_middleware(request: Request, call_next):
#     try:
#         response = await call_next(request)
#         return response
#     except HTTPException as e:
#         return {
#             "status_code": e.status_code,
#             "detail": {
#                 "status_code": e.status_code,
#                 "message": e.detail,
#                 "status": "Bad Request",
#                 "total": 0,
#                 "result": [],
#             },
#         }
#     except Exception as e:
#         # Handle the exception properly here
#         return {
#             "status_code": 500,
#             "detail": {
#                 "status_code": 500,
#                 "message": f"Internal Server Error: {str(e)}",
#                 "status": "Internal Server Error",
#                 "total": 0,
#                 "result": [],
#             },
#         }


class Transaction(BaseModel):
    date: str
    stt: str
    amount: str
    subject: str
    provider: str


class TransactionResponse(BaseModel):
    result: List[Transaction]
    total: int
    status: str
    message: str
    status_code: int


@app.get("/", response_model=TransactionResponse)
async def read_root(q: str = ''):
    """
    search transactions by term,
    return Not Found if no transaction is found
    """
    validate(q)

    with SessionLocal() as session:
        # replace this by a query to the database
        query = text(
            """
        SELECT date, stt, amount, subject, provider
        FROM transactions t
        WHERE to_tsvector('english', subject) @@ plainto_tsquery('english', :q);
        """
        )
        params = {"q": q}

        # execute the query
        # and fetch all the results
        result = session.execute(query, params)

        rows = result.fetchall()

        if len(rows) == 0:
            raise HTTPException(
                status_code=404,
                detail={
                    "status_code": 404,
                    "message": "Not Found, no transaction found",
                    "status": "Not Found",
                    "total": 0,
                    "result": [],
                },
            )

        print("Rows: ", rows)
        # Convert rows to a list of dictionaries
        transactions = [
            Transaction(
                date=row[0],
                stt=row[1],
                amount=row[2],
                subject=row[3],
                provider=row[4],
            )
            for row in rows
        ]

        print("Transaction: ", transactions)

        response = TransactionResponse(
            result=transactions,
            total=len(transactions),
            status="success",
            message="Transactions found",
            status_code=200,
        )

        return response


@app.get("/total")
async def total():
    """
    return the total number of transactions
    """
    with SessionLocal() as session:
        query = text(
            """
                SELECT SUM(CAST(regexp_replace(amount, '\.', '', 'g') AS BIGINT)) AS total
                FROM transactions t
                where amount != ''
                """
        )
        result = session.execute(query)
        rows = result.fetchone()
        if rows is None:
            total = 0
        else:
            total = rows[0]

        return {"total": total}

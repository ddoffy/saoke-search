"""
Extract transactions from a file
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.requests import Request
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from typing import List
from pydantic import BaseModel

# connection string to database
connectionString = "postgresql://postgres:postgres@jetson.local:5432/saokedb"
engine = create_engine(connectionString, echo=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def validate(q):
    """
    validate a query
    """
    if len(q) < 3:
        raise HTTPException(
            status_code=400,
            detail={
                "status_code": 400,
                "message": "Query is too short, should be at least 3 characters",
                "status": "Bad Request",
                "total": 0,
                "result": [],
            }
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
            }
        )

    return True


# create a FastAPI app
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
async def read_root(q: str = None):
    """
    search transactions by term,
    return Not Found if no transaction is found
    """
    if q is None:
        raise HTTPException(
            status_code=404,
            detail={
                "status_code": 404,
                "message": "Not Found, should try with a query. e.g. /?q=abc",
                "status": "Not Found",
                "total": 0,
                "result": [],
            }
        )
    else:
        validate(q)

        with SessionLocal() as session:
            # result = df[df["subject"].str.contains(q, case=False, na=False)].to_dict(orient="records")
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
                    }
                )
            # Convert rows to a list of dictionaries
            transactions = [
                {
                    "date": row[0],
                    "stt": row[1],
                    "amount": row[2],
                    "subject": row[3],
                    "provider": row[4],
                }
                for row in rows
            ]

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
        total = result.fetchone()[0]

        return {"total": total}

@app.middleware("http")
async def custom_middleware(request: Request, call_next):
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        # Handle the exception properly here
        return {
            "status_code": 500,
            "detail": {
                "status_code": 500,
                "message": "Internal Server Error",
                "status": "Internal Server Error",
                "total": 0,
                "result": [],
            }
        }

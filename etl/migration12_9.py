"""
Extract transactions from a file
"""

from datetime import datetime
import re
import pandas as pd
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.requests import Request
from sqlalchemy import create_engine


# connection string to database
connectionString = "postgresql://postgres:postgres@localhost:5432/saokedb"

# date time format in file
DATE_FORMAT = "%d/%m/%Y"

# id length
ID_LENGTH = 10

# list of lines that we want to ignore
IGNORE_LIST = [
    "SAO KÊ TÀI KHOẢN ACCOUNT STATEMENT",
    "Tên tài khoản/Account name:",
    "MAT TRAN TO QUOC VN - BAN CUU TRO TW",
    "Ngày thực hiện/ Date: 11/09/2024",
    "SỞ GIAO DỊCH",
    "Địa chỉ/ Address: 46 TRANG THI, HANOI",
    "Chi nhánh thực hiện/ Branch:",
    "Số tài khoản/Account number: CIF",
    "0011001932418 0002040837",
    "Loại tiền/Currency: VND",
    "Từ/ From: 01/09/2024 Đến/ To: 10/09/2024",
    "Ngày GD/",
    "TNX Date",
    "Số CT/ Doc No 01/09/2024",
    "Số tiền ghi nợ/ Debit",
    "Số tiền ghi có/ Credit",
    "Số dư/ Balance",
    "Nội dung chi tiết/ Transactions in detail",
    "Postal address:",
    "198 TRAN QUANG KHAI AVENUE HANOI - S.R. VIETNAM",
    "Telex : (0805) 411504 VCB - VT",
    "Swift : BFTV VNVX",
    "Website: www.vietcombank.com.vn",
    "Contact center: 1900.54.54.13",
    "Ngày GD/",
    "TNX Date",
    "Số CT/ Doc No 04/09/2024",
    "Số tiền ghi nợ/ Debit",
    "Số tiền ghi có/ Credit",
    "Số dư/ Balance",
    "Nội dung chi tiết/ Transactions in detail",
    "NGÂN HÀNG TMCP NGOẠI THƯƠNG VIỆT NAM (VCB",
    "Số tài khoản: 0011001932418",
    "STT",
    "NGÀY GIAO DỊCH",
    "SỐ TIỀN",
    "DIỄN GIẢI"
    "STT   NGÀY GIAO DỊCH    SỐ TIỀN                                      DIỄN GIẢI",
]

# list of patterns that we want to ignore
IGNORE_PATTERN_LIST = [
    r"Page\s+(\d+)\s+of\s+(\d+)",
    r"THỐNG\s+KÊ\s+GIAO\s+DỊCH\s+NGÀY\s+(\d+)[/](\d+)[/](\d+)",
    r"Số\s+CT[/]\s+Doc\s+No\s+(\d+)[/](\d+)[/](\d+)",
    r"Từ[/]\s+From[:]\s+(\d+)[/](\d+)[/](\d+)\s+Đến[/]\s+To[:]\s+(\d+)[/](\d+)[/](\d+)",
    r"Ngày\s+thực\s+hiện[/]\s+Date[:]\s+(\d+)[/](\d+)[/](\d+)",
    r"Ngày\s+GD[/]\s+TNX\s+Date",
    r"Số\s+tiền\s+ghi\s+nợ[/]\s+Debit",
]


def is_date(date_text, format):
    """Check if a date is valid"""
    try:
        datetime.strptime(date_text, format)
        return True
    except ValueError:
        return False


def is_int(val):
    """
    check if a value is an integer
    """
    try:
        int(val)
        return True
    except ValueError:
        return False

def is_float(val):
    """
    check if a value is a float
    """
    try:
        float(val)
        return True
    except ValueError:
        return False

def is_amount(val):
    """
    check if a value is an amount
    """
    if val == "":
        return False

    pattern = r'^\d{1,3}(?:\.\d{3})*$|^\d+$'

    if re.match(pattern, val):
        return True

    return False

def is_stt(val):
    """
    check if a value is an integer
    """

    if val == "":
        return False

    # slit value by space
    content = val.split(" ")

    try:
        int(content[0])
        return True
    except ValueError:
        return False

def ignore_line(line):
    """
    check if a line is in ignore list return True if it is;
    otherwise return False
    """
    for pattern in IGNORE_LIST:
        if pattern.lower() == line.lower():
            return True
    for pattern in IGNORE_PATTERN_LIST:
        if re.match(pattern, line):
            return True
    if line == "":
        return True
    return False


def build_result(result):
    """
    build a transaction from a result list
    """
    a = result[0]
    b = " ".join(result[1:])
    content = b.split(" ")
    id = content[0][0:ID_LENGTH]
    invalid_amount = False
    amount = content[0][ID_LENGTH:]
    if amount == "":
        amount = content[1]
        invalid_amount = True

    if invalid_amount:
        subject = " ".join(content[2:])
    else:
        subject = " ".join(content[1:])
    return [a, id, amount, subject]


def build_result_1(result, sequence):
    """
    build a transaction from a result list
    """
    date = ''
    stt = '';
    amount = ''
    subject = ''
    for i in range(len(result)):
        val = result[i]
        if is_date(val, DATE_FORMAT):
            date = val
        elif is_int(val):
            stt = val
        elif is_amount(val):
            amount = val
        else:
            subject =  subject + " " + val

    return [date, stt, amount, subject]



def read_file(path):
    """
    read file and extract transactions
    """
    # result list that contains all the results
    results = []
    # sequence of date time in file
    # corresponding to detect a transaction
    sequence = 0
    start_sequence = False
    # result list that contains a transaction
    result = []

    # read file
    with open(path, "r") as file:
        for line in file:
            val = line.strip()
            # global index
            # if index < 20:
            #     print(val)
            #     index += 1
            # detect a transaction that start from a sequence of integers
            # if is_date(val, DATE_FORMAT):
            # if a line is an integer
            # then we start a new transaction
            if is_stt(val):
                sequence += 1
                # add STT to the result list
                vals = val.split(" ")
                for v in [item for item in vals if item]:
                    result.append(v)

                if val[-1] == '"':
                    # build a transaction
                    # and add it to the result list
                    results.append(build_result_1(result, sequence))
                    # reset sequence and result list
                    sequence = 0
                    result = []
                    start_sequence = False
                    continue
            else:
                # if a line is in ignore list
                # then we skip it
                # otherwise we add it to the result list
                if ignore_line(val):
                    continue
                elif start_sequence:
                    result.append(f"{val}")
                    sequence += 1
                    if val[-1] == '"':
                        # build a transaction
                        # and add it to the result list
                        results.append(build_result_1(result, sequence))
                        # reset sequence and result list
                        sequence = 0
                        result = []
                        start_sequence = False
                        continue
                else:
                    result.append(f"{val}")
                    sequence += 1
                    if val[-1] == '"':
                        # build a transaction
                        # and add it to the result list
                        results.append(build_result_1(result, sequence))
                        # reset sequence and result list
                        sequence = 0
                        result = []
                        start_sequence = False
                        continue
                    if val[0] == '"':
                        start_sequence = True

    return results


def validate(q):
    """
    validate a query
    """
    if len(q) < 3:
        raise HTTPException(
            status_code=400,
            detail="Query is too short, should be at least 3 characters",
        )

    if len(q) > 50:
        raise HTTPException(
            status_code=400,
            detail="Query is too long, should be at most 50 characters",
        )

    return True


# default path to file
path = "./saoke12_9_layout.txt"
# if a path is provided"
# then we use it
# todo: add a check if the file exists
# todo: read path from environment variable
# otherwise we use the default path

# read file
results = read_file(path)


# import to pandas dataframe
df = pd.DataFrame(results, columns=["date", "stt", "amount", "subject"])

engine = create_engine(connectionString, echo=True)

df.to_sql("transactions", con=engine, if_exists="append", index=False)

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
    {"val": "CIF:", "opt": "in"},
    {"val": "Ngày thực hiện/ Date: 11/09/2024", "opt": "in"},
    {"val": "Chi nhánh thực hiện/ Branch", "opt": "in"},
    {"val": "SỞ GIAO DỊCH", "opt": "in"},
    {"val": "SAO KÊ TÀI KHOẢN", "opt" : "in"},
    {"val": "ACCOUNT STATEMENT", "opt" : "in"},
    {"val": "Loại tiền/Currency:", "opt" : "in"},
    {"val":"Từ/ From", "opt" : "int"},
    {"val":"Ngày GD/", "opt" : "in"},
    {"val": "Số tiền ghi nợ/", "opt": "in"},
    {"val": "TNX Date", "opt": "in"},
    {"val": "Số CT/ Doc No", "opt": "in"},
    {"val": "Số tài khoản/Account number: 0011001932418", "opt" : "in"},
    {"val": "Tên tài khoản/Account name:", "opt" : "in"},
    {"val": "SAO KÊ TÀI KHOẢN ACCOUNT STATEMENT", "opt" : "in"},
    {"val": "Tên tài khoản/Account name:", "opt" : "in"},
    {"val": "MAT TRAN TO QUOC VN - BAN CUU TRO TW", "opt" : "eq"},
    {"val": "Ngày thực hiện/ Date: 11/09/2024", "opt" : "in"},
    {"val": "SỞ GIAO DỊCH", "opt" : "in"},
    {"val": "Địa chỉ/ Address: 46 TRANG THI, HANOI", "opt" : "in"},
    {"val": "Chi nhánh thực hiện/ Branch:", "opt" : "in"},
    {"val": "Số tài khoản/Account number: CIF", "opt" : "in"},
    {"val": "0011001932418 0002040837", "opt" : "in"},
    {"val": "Loại tiền/Currency: VND", "opt" : "in"},
    {"val": "Từ/ From: 01/09/2024 Đến/ To: 10/09/2024", "opt" : "in"},
    {"val": "Ngày GD/", "opt" : "in"},
    {"val": "TNX Date", "opt" : "in"},
    {"val": "Số CT/ Doc No 01/09/2024", "opt" : "in"},
    {"val": "Số tiền ghi nợ/ Debit", "opt" : "in"},
    {"val": "Số tiền ghi có/ Credit", "opt" : "in"},
    {"val": "Số dư/ Balance", "opt" : "in"},
    {"val": "Nội dung chi tiết/ Transactions in detail", "opt" : "in"},
    {"val": "Postal address:", "opt" : "in"},
    {"val": "198 TRAN QUANG KHAI AVENUE HANOI - S.R. VIETNAM", "opt" : "in"},
    {"val": "Telex : (0805) 411504 VCB - VT", "opt" : "in"},
    {"val": "Swift : BFTV VNVX", "opt" : "in"},
    {"val": "Website: www.vietcombank.com.vn", "opt" : "in"},
    {"val": "Contact center: 1900.54.54.13", "opt" : "in"},
    {"val": "Ngày GD/", "opt" : "in"},
    {"val": "TNX Date", "opt" : "in"},
    {"val": "Số CT/ Doc No 04/09/2024", "opt" : "in"},
    {"val": "Số tiền ghi nợ/ Debit", "opt" : "in"},
    {"val": "Số tiền ghi có/ Credit", "opt" : "in"},
    {"val": "Số dư/ Balance", "opt" : "in"},
    {"val": "Nội dung chi tiết/ Transactions in detail", "opt" : "in"},
    {"val": "NGÂN HÀNG TMCP NGOẠI THƯƠNG VIỆT NAM (VCB", "opt" : "in"},
    {"val": "Số tài khoản: 0011001932418", "opt" : "in"},
    {"val": "STT", "opt" : "in"},
    {"val": "NGÀY GIAO DỊCH", "opt" : "in"},
    {"val": "SỐ TIỀN", "opt" : "in" },
    {"val": "DIỄN GIẢI", "opt" : "in"},
    {"val": "STT   NGÀY GIAO DỊCH    SỐ TIỀN                                      DIỄN GIẢI", "opt" : "in"},
    {"val": "Transactions in detail Số CT/ Doc No", "opt" : "in"},
    {"val": "Balance", "opt" : "in"},
    {"val": "Credit", "opt" : "in"},
    {"val": "198 TRAN QUANG KHAI AVENUE", "opt" : "in"},
    {"val": "HANOI - S.R. VIETNAM", "opt" : "in"},
    {"val": "Telex : (0805) 411504 VCB - VT", "opt" : "in"},
    {"val": "Swift : BFTV VNVX", "opt" : "in"},
    {"val": "Website: www.vietcombank.com.vn", "opt" : "in"},
    {"val": "Contact center: 1900.54.54.13", "opt" : "in"},
    {"val": "Ngày GD/ TNX Date", "opt" : "in"},
    {"val": "Số CT/ Doc No 01/09/2024", "opt" : "in"},
    {"val": "Số tiền ghi nợ/ Debit", "opt" : "in"},
    {"val": "Số tiền ghi có/ Credit", "opt" : "in"},
    {"val": "Số dư/ Balance", "opt" : "in"},
    {"val": "Nội dung chi tiết/ Transactions in detail", "opt" : "in"},
    {"val": "NGÂN HÀNG TMCP NGOẠI THƯƠNG VIỆT NAM (VCB", "opt" : "in"},
    {"val": "Số tài khoản: 0011001932418", "opt" : "in"},
    {"val": "STT", "opt" : "in"},
    {"val": "NGÀY GIAO DỊCH", "opt" : "in"},
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

    pattern = r'^(?!0\d)\d{1,3}(?:\.\d{3})+$'

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


def is_id(val):
    """
    check if a value is an id
    """
    if val == "":
        return False

    pattern = r'^\d{4,}\.\d+$'

    if re.match(pattern, val):
        return True

    return False

def ignore_line(line):
    """
    check if a line is in ignore list return True if it is;
    otherwise return False
    """
    for pattern in IGNORE_LIST:
        if pattern["opt"] == "eq" and pattern["val"].lower() == line.lower():
            return True
        elif pattern["opt"] == "in" and pattern["val"].lower() in line.lower():
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
    date = ''
    id = '';
    amount = ''
    subject = ''

    for i in range(len(result)):
        val = result[i]
        if is_date(val, DATE_FORMAT):
            date = val
        elif is_id(val):
            id = val
        elif is_amount(val):
            amount = val
        else:
            subject =  subject + " " + val

    return [date, id, amount, subject]


def build_result_1(result):
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
        elif is_stt(val):
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
    # result list that contains a transaction
    result = []

    # read file
    with open(path, "r") as file:
        for line in file:
            val = line.strip()
            # detect a transaction that start from a sequence of integers
            # if is_date(val, DATE_FORMAT):
            # if a line is an integer
            # then we start a new transaction
            if is_date(val, DATE_FORMAT):
                if sequence > 1:
                    # build a transaction
                    # and add it to the result list
                    results.append(build_result(result))
                    # reset sequence and result list
                    sequence = 0
                    result = []
                sequence += 1
                result.append(val)
            else:
                # if a line is in ignore list
                # then we skip it
                # otherwise we add it to the result list
                if ignore_line(val):
                    continue
                else:
                    data = val.split(' ')
                    if len(data) > 1 and is_amount(data[0]):
                        result.append(data[0])
                        filter_empty_strings = [x for x in data[1:] if x]
                        result.append(" ".join(filter_empty_strings))
                    else:
                        result.append(val)
                    sequence += 1

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
path = "./saoke19_119_layout.txt"
# if a path is provided"
# then we use it
# todo: add a check if the file exists
# todo: read path from environment variable
# otherwise we use the default path

# read file
results = read_file(path)

# for r in results[:50]:
#     print(r)
#
# print(len(results))

# import to pandas dataframe
df = pd.DataFrame(results, columns=["date", "stt", "amount", "subject"])

engine = create_engine(connectionString, echo=True)

df.to_sql("transactions", con=engine, if_exists="append", index=False)
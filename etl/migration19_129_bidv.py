"""
Extract transactions from a file
"""

from datetime import datetime
import re
import pandas as pd
from sqlalchemy import create_engine
import time

# connection string to database
connectionString = "postgresql://postgres:postgres@jetson.local:5432/saokedb"

# date time format in file
# 01/09/2024 10:27:58
DATE_FORMAT = "%d/%m/%Y %H:%M:%S"

# provider
PROVIDER = "BIDV"

# id length
ID_LENGTH = 10

# list of lines that we want to ignore
IGNORE_LIST = [
    {"val": "CIF:", "opt": "in"},
    {"val": "Ngày thực hiện/ Date: 11/09/2024", "opt": "in"},
    {"val": "Chi nhánh thực hiện/ Branch", "opt": "in"},
    {"val": "SỞ GIAO DỊCH", "opt": "in"},
    {"val": "SAO KÊ TÀI KHOẢN", "opt": "in"},
    {"val": "ACCOUNT STATEMENT", "opt": "in"},
    {"val": "Loại tiền/Currency:", "opt": "in"},
    {"val": "Từ/ From", "opt": "int"},
    {"val": "Ngày GD/", "opt": "in"},
    {"val": "Số tiền ghi nợ/", "opt": "in"},
    {"val": "TNX Date", "opt": "in"},
    {"val": "Số CT/ Doc No", "opt": "in"},
    {"val": "SAO KÊ TÀI KHOẢN ACCOUNT STATEMENT", "opt": "in"},
    {"val": "Tên tài khoản/Account name:", "opt": "in"},
    {"val": "MAT TRAN TO QUOC VN - BAN CUU TRO TW", "opt": "eq"},
    {"val": "Ngày thực hiện/ Date: 11/09/2024", "opt": "in"},
    {"val": "SỞ GIAO DỊCH", "opt": "in"},
    {"val": "Địa chỉ/ Address: 46 TRANG THI, HANOI", "opt": "in"},
    {"val": "Chi nhánh thực hiện/ Branch:", "opt": "in"},
    {"val": "Số tài khoản/Account number: CIF", "opt": "in"},
    {"val": "0011001932418 0002040837", "opt": "in"},
    {"val": "Loại tiền/Currency: VND", "opt": "in"},
    {"val": "Từ/ From: 01/09/2024 Đến/ To: 10/09/2024", "opt": "in"},
    {"val": "Ngày GD/", "opt": "in"},
    {"val": "TNX Date", "opt": "in"},
    {"val": "Số CT/ Doc No 01/09/2024", "opt": "in"},
    {"val": "Số tiền ghi nợ/ Debit", "opt": "in"},
    {"val": "Số tiền ghi có/ Credit", "opt": "in"},
    {"val": "Số dư/ Balance", "opt": "in"},
    {"val": "Nội dung chi tiết/ Transactions in detail", "opt": "in"},
    {"val": "Postal address:", "opt": "in"},
    {"val": "198 TRAN QUANG KHAI AVENUE HANOI - S.R. VIETNAM", "opt": "in"},
    {"val": "Telex : (0805) 411504 VCB - VT", "opt": "in"},
    {"val": "Swift : BFTV VNVX", "opt": "in"},
    {"val": "Website: www.vietcombank.com.vn", "opt": "in"},
    {"val": "Contact center: 1900.54.54.13", "opt": "in"},
    {"val": "Ngày GD/", "opt": "in"},
    {"val": "TNX Date", "opt": "in"},
    {"val": "Số CT/ Doc No 04/09/2024", "opt": "in"},
    {"val": "Số tiền ghi nợ/ Debit", "opt": "in"},
    {"val": "Số tiền ghi có/ Credit", "opt": "in"},
    {"val": "Số dư/ Balance", "opt": "in"},
    {"val": "Nội dung chi tiết/ Transactions in detail", "opt": "in"},
    {"val": "NGÂN HÀNG TMCP NGOẠI THƯƠNG VIỆT NAM (VCB", "opt": "in"},
    {"val": "Số tài khoản: 0011001932418", "opt": "in"},
    {"val": "STT", "opt": "in"},
    {"val": "NGÀY GIAO DỊCH", "opt": "in"},
    {"val": "SỐ TIỀN", "opt": "in"},
    {"val": "DIỄN GIẢI", "opt": "in"},
    {
        "val": "STT   NGÀY GIAO DỊCH    SỐ TIỀN                                      DIỄN GIẢI",
        "opt": "in",
    },
    {"val": "NGÂN HÀNG TMCP NGOẠI THƯƠNG VIỆT NAM", "opt": "in"},
    {"val": "NGÂN HÀNG TMCP ĐẦU TƯ VÀ PHÁT TRIỂN VIỆT NAM", "opt": "in"},
    {"val": "Bank for Investment and Development of VietNam JSC", "opt": "in"},
    {"val": "Chi nhánh: NHTMCP DT&PTVN-CN BA DINH", "opt": "in"},
    {
        "val": "SAO KÊ TÀI KHOẢN TIỀN GỬI KHÁCH HÀNG/ACCOUNT STATEMENT",
        "opt": "in",
    },
    {"val": "Từ ngày: 01/09/2024 đến ngày 12/9/2024", "opt": "in"},
    {"val": "Khách hàng:: UBTW MTTQ VIET NAM", "opt": "in"},
    {"val": "Địa chỉ: 46 TRANG THI, QUAN HOAN KIEM /,HA NOI", "opt": "in"},
    {"val": "Tên tài khoản: UBTW MTTQ VIET NAM", "opt": "in"},
    {"val": "Số tài khoản: 1261122666", "opt": "in"},
    {"val": "Loại tiền tệ: VND", "opt": "in"},
    {"val": "(Số tài khoản cũ: 12610001122666)", "opt": "in"},
    {"val": "Phát sinh có Diễn giải", "opt": "in"},
    {"val": "STT Ngày giao dịch", "opt": "in"},
    {"val":"(No)", "opt":"in"},
    {"val":"(Trans.Date)", "opt":"in"},
    {"val":"(Credit amount)", "opt":"in"},
    {"val":"(Txn. Description)", "opt":"in"}
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

    pattern = r"^\d{1,3}(?:\.\d{3})*$|^\d+$"

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
    content = list(filter(None, val.split(" ")))

    try:
        int(content[0])
        if len(content) < 3:
            return False
        date_time = f"{content[1]} {content[2]}"
        if is_date(date_time, DATE_FORMAT):
            return True
        return False
    except ValueError:
        return False


def ignore_line(line):
    """
    check if a line is in ignore list return True if it is;
    otherwise return False
    """
    for pattern in IGNORE_LIST:
        if pattern["opt"] == "in" and pattern["val"].lower() in line.lower():
            return True
        elif pattern["opt"] == "eq" and pattern["val"].lower() == line.lower():
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


index = 0


def build_result_1(result, meta_data):
    """
    build a transaction from a result list
    """
    date = ""
    stt = ""
    amount = ""
    subject = ""

    for i in range(len(result)):
        val = result[i]
        if is_date(val, DATE_FORMAT) and (
            meta_data.get("date") == val or meta_data == {}
        ):
            date = val
        elif is_int(val) and (meta_data.get("stt") == val or meta_data == {}):
            stt = val
        elif is_amount(val) and (
            meta_data.get("amount") == val or meta_data == {}
        ):
            amount = val
        else:
            subject = subject + " " + val

    global index
    index += 1
    if (
        stt == ""
        or index != int(stt)
        or is_amount(amount) == False
        or is_date(date, DATE_FORMAT) == False
    ):
        print(f"stt == '' = {stt == ''}; index != int(stt) = {index != int(stt)}; is_amount(amount) == False = {is_amount(amount) == False}; is_date(date, DATE_FORMAT) == False = {is_date(date, DATE_FORMAT) == False}")
        print("Begin ", index)
        print(f"Index: {index}, STT: {stt}")
        print("Result: ", result)
        print("Meta data: ", meta_data)
        print("Actual data: ", date, stt, amount, subject)
        print("End ", index)
        time.sleep(5)

    return [date, stt, amount, subject, PROVIDER]


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
    meta_data = {}

    local_index = 0

    # read file
    with open(path, "r") as file:
        for line in file:
            val = line.strip()
            # detect a transaction that start from a sequence of integers
            # if is_date(val, DATE_FORMAT):
            # if a line is an integer
            # then we start a new transaction
            if is_stt(val):
                sequence += 1
                # add STT to the result list
                vals = list(filter(None, val.split(" ")))
                if meta_data.get("stt") is not None:
                    # build a transaction
                    # and add it to the result list
                    results.append(build_result_1(result, meta_data))
                    # reset sequence and result list
                    sequence = 0
                    result = []
                    meta_data = {}
                meta_data["stt"] = vals[0]
                result.append(f"{vals[0]}")
                meta_data["date"] = f"{vals[1]} {vals[2]}"
                result.append(f"{vals[1]} {vals[2]}")
                meta_data["amount"] = vals[3]
                result.append(f"{vals[3]}")
                if len(vals) > 4:
                    meta_data["subject"] = " ".join(vals[4:])
                result.append(f"{' '.join(vals[4:])}")

                if val[-1] == '"' or (len(vals) > 3 and vals[3][0] == '"'):
                    # build a transaction
                    # and add it to the result list
                    results.append(build_result_1(result, meta_data))
                    # reset sequence and result list
                    sequence = 0
                    result = []
                    meta_data = {}
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
                        results.append(build_result_1(result, meta_data))
                        # reset sequence and result list
                        sequence = 0
                        result = []
                        meta_data = {}
                        start_sequence = False
                        continue
                else:
                    result.append(f"{val}")
                    sequence += 1
                    if val[-1] == '"':
                        # build a transaction
                        # and add it to the result list
                        results.append(build_result_1(result, meta_data))
                        # reset sequence and result list
                        sequence = 0
                        result = []
                        meta_data = {}
                        start_sequence = False
                        continue
                    if val[0] == '"':
                        start_sequence = True
                        if meta_data.get("stt") is not None:
                            # build a transaction
                            # and add it to the result list
                            results.append(build_result_1(result, meta_data))
                            # reset sequence and result list
                            sequence = 0
                            result = []
                            meta_data = {}
                            start_sequence = False
                            continue

    return results


# default path to file
path = "./bidv19_129_layout.txt"
# if a path is provided"
# then we use it
# todo: add a check if the file exists
# todo: read path from environment variable
# otherwise we use the default path

# read file
results = read_file(path)

# # print 20 first transactions
# for i in range(20):
#     print(results[i])
#
# print(len(results))

# import to pandas dataframe
df = pd.DataFrame(results, columns=["date", "stt", "amount", "subject", "provider"])

engine = create_engine(connectionString, echo=True)

df.to_sql("transactions", con=engine, if_exists="append", index=False)

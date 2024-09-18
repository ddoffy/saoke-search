import re
from datetime import datetime


# date time format in file
DATE_FORMAT = "%d/%m/%Y"


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


def is_date(date_text, format):
    """Check if a date is valid"""
    try:
        datetime.strptime(date_text, format)
        return True
    except ValueError:
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
        if is_date(content[1], DATE_FORMAT):
            return True
        return False
    except ValueError:
        return False


def is_id(val):
    """
    check if a value is an id
    """
    if val == "":
        return False

    pattern = r"^\d{4,}[\.]\d{2,}$"
    second_pattern = r"^\d{1}\.\d{2}$"
    # 33.14 pattern for id like this, fix 2 digits before dot and 2 digits
    # after dot
    third_pattern = r"^\d{2}\.\d{2}$"
    # 213.1531 pattern for id like this, fix 3 digits before dot and 4 digits
    # after dot
    forth_pattern = r"^\d{3}\.\d{4}$"
    # 55.202 pattern for id like this, fix 2 digits before dot and 3 digits
    # after dot
    fifth_pattern = r"^\d{2}\.\d{3}$"

    if (
        re.match(pattern, val)
        or re.match(second_pattern, val)
        or re.match(third_pattern, val)
        or re.match(forth_pattern, val)
        or re.match(fifth_pattern, val)
    ):
        return True

    return False


test_amount = ["50.000", "10.000", "100.000", "123.787"]

for amount in test_amount:
    print(f"Is amount of text {amount}: ", is_amount(amount))

#
# print(is_amount(test))

text = "344   14/09/2024     50.000"
print("Is stt of text: ", is_stt(text))

test_ids = [
    "5213.45946",
    "50.000",
    "1.48",
    "5390.133",
    "1.000",
    "5209.16",
    "33.45",
    "213.1531",
    "55.202",
    "8.94"
]
for test_id in test_ids:
    print(f"Is id of text {test_id}: ", is_id(test_id))

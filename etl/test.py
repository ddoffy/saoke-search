import re

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



test = "0351000762157"

print(is_amount(test))


def is_string_valid_int_bigger_than_zero(value: str) -> bool:
    """check if a string is a valid integer bigger than zero"""
    try:
        if int(value) <= 0:
            return False
    except ValueError:
        return False
    return True
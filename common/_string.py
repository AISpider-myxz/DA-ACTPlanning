import typing


def except_blank(str_list: typing.List) -> typing.List:
    if str_list:
        return [_str.strip() for _str in str_list if not _str.isspace()]
    return []

def del_esc(str):
    return str.replace("\r", " ").replace("\n", " ").replace("\t", " ").strip()

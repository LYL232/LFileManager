def name_single_character_check(name: str) -> bool:
    illegal_char = [',', ' ', '/', '\\']
    for each in illegal_char:
        if each in name:
            return False
    return True

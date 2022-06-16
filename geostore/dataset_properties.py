from string import ascii_letters, digits

TITLE_CHARACTERS = f"{ascii_letters}{digits}_-"
TITLE_PATTERN = f"^[{TITLE_CHARACTERS}]+$"

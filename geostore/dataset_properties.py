from string import ascii_letters, digits

DATASET_KEY_SEPARATOR = "-"

TITLE_CHARACTERS = f"{ascii_letters}{digits}_-"
TITLE_PATTERN = f"^[{TITLE_CHARACTERS}]+$"

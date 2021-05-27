def decapitalize(key: str) -> str:
    return f"{key[:1].lower()}{key[1:]}"


MESSAGE_ATTRIBUTE_TYPE_KEY = "type"
MESSAGE_ATTRIBUTE_TYPE_ROOT = "root"
MESSAGE_ATTRIBUTE_TYPE_DATASET = "dataset"
DATA_TYPE_KEY = "DataType"
DATA_TYPE_STRING = "String"
STRING_VALUE_KEY = "StringValue"
STRING_VALUE_KEY_LOWER = decapitalize(STRING_VALUE_KEY)

def decapitalize(key: str) -> str:
    """
    This method will be used to lower case the first character of SQS
    message attributes being received by Lambda to resolve inconsistencies.
    Issue outlined here: https://github.com/boto/boto3/issues/2582
    """
    return f"{key[:1].lower()}{key[1:]}"


MESSAGE_ATTRIBUTE_TYPE_KEY = "type"
MESSAGE_ATTRIBUTE_TYPE_ROOT = "root"
MESSAGE_ATTRIBUTE_TYPE_DATASET = "dataset"
DATA_TYPE_KEY = "DataType"
DATA_TYPE_STRING = "String"
STRING_VALUE_KEY = "StringValue"
STRING_VALUE_KEY_LOWER = decapitalize(STRING_VALUE_KEY)

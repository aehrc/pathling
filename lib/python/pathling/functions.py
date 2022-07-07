from pyspark.sql import Column
from pyspark.sql.functions import lit, struct


def to_coding(coding_column: Column, system: str, version: str = None):
    """
    Converts a Column containing codes into a Column that contains a Coding struct. The Coding
    struct Column can be used as an input to terminology functions such as `member_of` and
    `translate`.
    :param coding_column: the Column containing the codes
    :param system: the URI of the system the codes belong to
    :param version: the version of the code system
    :return: a Column containing a Coding struct
    """
    id_column = lit(None).alias('id')
    system_column = lit(system).alias('system')
    version_column = lit(version).alias('version')
    display_column = lit(None).alias('display')
    user_selected_column = lit(None).alias('userSelected')
    return struct(id_column, system_column, version_column, coding_column.alias("code"),
                  display_column, user_selected_column)

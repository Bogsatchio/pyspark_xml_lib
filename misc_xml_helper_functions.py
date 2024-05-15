from pyspark.sql.functions import col, when, size
from pyspark.sql.functions import first, lit, explode
from pyspark.sql import functions as F
from pyspark.sql.functions import explode


def check_column_exists(df, column_name):
    """
    Function simply checks whether column exists or not and returns bool
    """
    try:
        df[column_name]
        return True
    except:
        return False


def column_or_null(df, colName, name):
    """
    Function can handle missing columns in a spark select statement.
    Particularly useful when working with files without schema validation.
    Returns column when found or returns named column without data
    """
    if (check_column_exists(df, colName)):
        return col(colName).alias(name)
    else:
        return lit(None).alias(name)
    

def replace_null_with_list(column, n):
    """
    Replaces null value with an array of nulls that can be helpful for later exploding the data
    """
    return F.when(F.expr(f"size({column}) > 0"), F.col(column)).otherwise(F.array([F.lit(None)] * n))


def create_exploded_column_df(df, col_name):
    exploded_df = df.select(explode(col_name).alias(col_name))
    return exploded_df

    
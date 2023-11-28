from pyspark.sql.functions import col, when, size
from pyspark.sql.functions import first, lit, explode
from pyspark.sql import functions as F
from pyspark.sql.functions import explode


# Function to check if column exists
def check_column_exists(df, column_name):
    try:
        df[column_name]
        return True
    except:
        return False

# Function to Handle Missing Columns
def ColumnOrNull(df, colName, name):
    if (check_column_exists(df, colName)):
        return col(colName).alias(name)
    else:
        return lit(None).alias(name)
    
    
# Uzupełnienie nulli okok git commit
def replace_null_with_list(column, n):
    return F.when(F.expr(f"size({column}) > 0"), F.col(column)).otherwise(F.array([F.lit(None)] * n))


# Eksplodowanie nulli
def createColumnDf(df, col_name):
    exploded_df = df.select(explode(col_name).alias(col_name))
    return exploded_df

    
from pyspark.sql.functions import col, when, size
from pyspark.sql.functions import first, lit, explode
from pyspark.sql import functions as F
from pyspark.sql.functions import explode
import json


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

# Below functions can generate xpaths out of XML file read in pyspark dataframe
def output_x_strings(df, prefix_string):
    sch_json = df.schema.json()
    data = json.loads(sch_json)
    diction = data["fields"][0]
    xlist = []
    create_x_strings(xlist, diction, prefix_string)
    return xlist

def process_dict(valid_dict, x_str, x_list):
    for x in valid_dict:
        if isinstance(x["type"], str): # jeśli typ nie jest złozony a jest zwykłym stringiem to jest to tag końcowy
            final_x_string = x_str + '.' + x["name"]   # update tagu końcowego
            x_list.append(final_x_string)
            # print(f"update tagu końcowego {final_x_string}")
        elif isinstance(valid_dict, list):
            diction_internal = valid_dict
            continue_x_string = x_str + '.' + x["name"]
            if isinstance(diction_internal, list):
                # for x in diction_internal:
                # print(f"update tagu i zejście niżej listy {continue_x_string}")
                # print(x)
                create_x_strings(x_list, x, continue_x_string)
            else:
                # print("Nierozpoznany Przypadek")
                create_x_strings(x_list, diction_internal, continue_x_string)

def create_x_strings(x_list, diction, x_str):

    if isinstance(diction["type"], str):
        final_x_string = x_str + '.' + diction["name"]   # update tagu końcowego
        x_list.append(final_x_string)
        # print(f"update tagu końcowego {final_x_string}")

    else:
        try:
            fields = diction["type"]["fields"]
            process_dict(fields, x_str, x_list)
        except:
            fields = diction["type"]["elementType"]
            if isinstance(fields, str):
                x_list.append(x_str)
                # print(f"update tagu końcowego {x_str}")
            else:
                fields = diction["type"]["elementType"]["fields"]
                process_dict(fields, x_str, x_list)
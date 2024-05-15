from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, BooleanType, DoubleType, ArrayType
import xml.etree.ElementTree as ET
from pyspark import SparkConf
from timeit import default_timer as timer
from pyspark.sql.utils import AnalysisException
import datetime
from time import time
from pyspark.sql.functions import col, lit
import json


spark_type_dict = {
    'xs:string': StringType(),
    'xs:decimal': DoubleType(),
    'xs:date': DateType(),
    'xs:boolean': BooleanType()
}

def get_types(xsd_rdd):
    """
    Creates a map of tag names and types defined in XSD schema
    """
    root = ET.fromstring('\n'.join(xsd_rdd))
    types = {}
    type_id = 0
    children =  {}
    for element in root:
        if 'simpleType' in element.tag or 'complexType' in element.tag:
            name = element.attrib.get('name')
            #print("name: " + name)
            if name not in types.keys():
                types[name] = []
                children[name] = []
                for e in element.iter():
                    #print(e.tag, e.attrib)
                    if "element" in e.tag or "attribute" in e.tag:
                        t = e.attrib.get('type')
                        if "attribute" in e.tag:
                            n = "_" + e.attrib.get('name')
                        else:
                            n = e.attrib.get('name')
                        types[name].append({"name": n, "type": t})
                        children[name].append(t)
                        #types["children"].append({name: t})
                        if t in types.keys():
                            print(t)
    return types


def build_xpaths(xpaths_dict, types, curr_path, t, types_d):
    """
    Function uses recurrency to build xpaths based on the types map created by get_types function
    """
    try:
        for x1 in types[t]:
            #print(x1["name"])
            n1 = x1["name"]
            t1 = x1["type"]
            curr_path2 = curr_path + "." + n1
            build_xpaths(xpaths_dict, types, curr_path2, t1, types_d)
    except:
        # tutaj dajemy xpath jako klucz słownika a typ danych jako wartość
        try:
            xpaths_dict[curr_path] = types_d[t.split(":")[1]]
        except:
            xpaths_dict[curr_path] = "xs:string"
            

def generate_paths_dict(save_file_path, starting_struct, types_d, xsd_rdd, sub_rdds = []):
    """
    Defines starting struc field name and uses build_paths function to create map of xpaths and primitive data types.
    It can use just mai xsd schema as well as multiple sub_schemas for larger schemas.
    """
    types = get_types(xsd_rdd)
    if len(sub_rdds) > 0:
        for rdd in sub_rdds:
            types = {**types, **get_types(rdd)}
    xpaths_dict = {}
    for x in types[starting_struct]:
        n = x["name"]
        t = x["type"]
        build_xpaths(xpaths_dict, types, n, t, types_d)
        
    with open(save_file_path, 'w') as json_file:
        json.dump(xpaths_dict, json_file)
    return xpaths_dict







def generate_schema_from_xpaths(xpaths):
    """
    Takes in a dictionary structured with xpaths as keys and creates simple pyspark schema that allows each xpath to select data.
    """
    
    schema = StructType()
    for xpath in xpaths:
        fields = xpath.split('.')
        current_struct = schema
        for field in fields[:-1]:
            if field not in [f.name for f in current_struct.fields]:
                current_struct.add(field, StructType())
            current_struct = next(f.dataType for f in current_struct.fields if f.name == field)
        last_field_name = fields[-1]
        if last_field_name not in [f.name for f in current_struct.fields]:
            current_struct.add(StructField(last_field_name, spark_type_dict[xpaths[xpath]], True))
    return schema



def get_data_types(xsd_rdd):
    root = ET.fromstring('\n'.join(xsd_rdd))
    types = {}
    type_id = 0
    children =  {}
    for element in root:
        if 'simpleType' in element.tag or 'complexType' in element.tag:
            name = element.attrib.get('name')
            if name not in types.keys():
                types[name] = []
                children[name] = []
                for e in element.iter():
                    if "restriction" in e.tag or "attribute" in e.tag: 
                        types[name] = e.attrib.get('base')
    return types

def create_types_dict(sub_rdds, file_path):
    """
    Creates JSON file with primitive data types mapped to complex data types of XSD.
    It uses helper function get_data_types
    
    """
    types = {}
    for rdd in sub_rdds:
        types = {**types, **get_data_types(rdd)}
    with open(file_path, 'w') as json_file:
        json.dump(types, json_file)
    return types


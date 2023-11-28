
import json



# Generowanie XPAthów
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
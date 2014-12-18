
import sys, re, copy, os
import xml.etree.ElementTree as ET
import xml.dom.minidom

sys.path.append(sys.path[0] + "/modules")

td_ColumnType_meta_datatype_map = {
    "A1": "string",
    "AT": "string",
    "BF": "string",
    "BO": "string",
    "BV": "string",
    "CF": "string",
    "CO": "string",
    "CV": "string",
    "D" : "number",
    "DA": "string",
    "DH": "string",
    "DM": "string",
    "DS": "string",
    "DY": "string",
    "F" : "number",
    "HM": "string",
    "HR": "string",
    "HS": "string",
    "I1": "integer",
    "I2": "integer",
    "I8": "integer",
    "I" : "integer",
    "LF": "string",
    "LV": "string",
    "MI": "string",
    "MO": "string",
    "MS": "string",
    "N" : "string",
    "PD": "string",
    "PM": "string",
    "PS": "string",
    "PT": "string",
    "PZ": "string",
    "SC": "string",
    "SZ": "string",
    "TS": "string",
    "TZ": "string",
    "UF": "string",
    "UT": "string",
    "UV": "string",
    "YI": "string",
    "YM": "string",
    "YR": "string",
}


if len(sys.argv) < 4:
    print("invalid input")
    exit()

radd_name = sys.argv[1]
inputFile = sys.argv[2]
keysList = sys.argv[3:]

try:
    f = open(inputFile, "r")
except IOError:
    print("input open error")
    exit()
else:
    root = ET.Element("DynamicDatasetMetadata")
    root.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
    root.set("xsi:noNamespaceSchemaLocation", "dynamic_dataset_metadata.xsd")
    root.set("maxrows", "10000000")
    root.set("frequency", "daily")
    root.set("name", "IDI_" + radd_name)
    root.set("source", "Risk")
    root.set("author", "Radd_Admin")
    root.set("time", "UNIQ_DTMY2S")
    root.set("action", "ACTN_PPTYPE")

    PrimaryKeys = ET.SubElement(root, "PrimaryKeys")
    Variables = ET.SubElement(root, "Variables")

    default_field = ET.Element("Field")
    default_field.set("name", "default_name")
    default_field.set("index", "0")
    default_field.set("datatype", "string")

    with f:
        key_idx = 0
        var_idx = 0
        for line in f.readlines():
            line = line.rstrip("\r\n")
            if re.match("^COLUMN\|", line) is None:
                continue

            col_data = line.split('|')
            field = ET.Element("Field")
            field.set("name", col_data[4].upper())
            if len(col_data[4]) > 27:
                print("WARNING: field name " + col_data[4] + " longer than 27")
                
            field.set("datatype", td_ColumnType_meta_datatype_map[col_data[6]])
            if col_data[4] in keysList:
                field.set("index", str(key_idx))
                PrimaryKeys.append(field)
                key_idx += 1
            else:
                field.set("index", str(var_idx))
                Variables.append(field)
                var_idx += 1

    print(xml.dom.minidom.parseString(ET.tostring(root)).toprettyxml(indent = "  "))
    #print(ET.tostring(field))

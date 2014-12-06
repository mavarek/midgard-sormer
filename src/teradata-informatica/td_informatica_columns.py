td_type_desc = {
    "A1": "One dimensional ARRAY data type",
    "AT": "ANSI Time",
    "BF": "BYTE Fixed",
    "BO": "Byte Large Object",
    "BV": "Byte Varying",
    "CF": "Character Fixed",
    "CO": "Character Large Object",
    "CV": "Character Varying Latin",
    "D" : "Decimal",
    "DA": "Date",
    "DH": "Interval Day To Hour",
    "DM": "Interval Day To Minute",
    "DS": "Interval Day To Second",
    "DY": "Interval Day",
    "F" : "Float",
    "HM": "Interval Hour To Minute",
    "HR": "Interval Hour",
    "HS": "Interval Hour To Second",
    "I1": "1 Byte Integer",
    "I2": "2 Byte Integer",
    "I8": "8 Byte Integer",
    "I" : "4 Byte Integer",
    "LF": "Pre-TD12.0 Character Fixed Locale (Kanji1 or Latin)",
    "LV": "Pre-TD12.0 Character Varying Locale (Kanji1 or Latin)",
    "MI": "Interval Minute",
    "MO": "Interval Month",
    "MS": "Interval Minute To Second",
    "N" : "Number",
    "PD": "PERIOD(DATE)",
    "PM": "PERIOD(TIMESTAMP(n) WITH TIME ZONE)",
    "PS": "PERIOD(TIMESTAMP(n))",
    "PT": "PERIOD(TIME(n))",
    "PZ": "PERIOD(TIME(n) WITH TIME ZONE)",
    "SC": "Interval Second",
    "SZ": "Timestamp With Time Zone",
    "TS": "Timestamp",
    "TZ": "ANSI Time With Time Zone",
    "UF": "Character Fixed Unicode",
    "UT": "UDT Type",
    "UV": "Character Varying Unicode",
    "YI": "Year Interval",
    "YM": "Interval Year To Month",
    "YR": "Year"
}

ColumnType = "CF"
print(td_type_desc.get(ColumnType, "Unknown"))

class tdColumn:
    '''Teradata column properties from DBC.Columns'''
    CharType = None
    ColumnName = None
    ColumnFormat = None
    ColumnType = None
    ColumnLength = None
    DecimalTotalDigits = None
    DecimalFractionalDigits = None


class infaColumn:
    '''Informatica column properties'''

    '''
    Valid TransformationTypes:
        Source Definition, Source Qualifier, Expression, Target Definition
    '''
    TRANSFORMATIONTYPE = None
    '''
    Valid DatabaseTypes:
        Transformation, Teradata, Flat File
    '''
    DATABASETYPE = None
    DATATYPE = None
    FIELDNUMBER = None
    FIELDTYPE = None '''ELEMITEM'''
    LENGTH = None
    NAME = None
    NULLABLE = None
    OFFSET = None
    PRECISION = None
    SCALE = None

    trans_type_element_tag_map = {
        "Expression": "TRANSFORMFIELD",
        "Source Definition": "SOURCEFIELD",
        "Source Qualifier": "TRANSFORMFIELD",
        "Target Definition": "TARGETFIELD"
    }

    def __init__(self):
        __init__(self, "Expression")

    def __init__(self, TRANSFORMATIONTYPE):
        __init__(self, TRANSFORMATIONTYPE, "Transformation")

    def __init__(self, TRANSFORMATIONTYPE, DATABASETYPE):
        self.TRANSFORMATIONTYPE = TRANSFORMATIONTYPE
        self.DATABASETYPE = DATABASETYPE

    def validate(): pass
    
    def validateBIGINT():
        if  (NAME is None) or (NULLABLE is None) or
            (PRECISION != 19) or (SCALE != 0):
            return -1
        
#td_infaSQ_type_map = {'

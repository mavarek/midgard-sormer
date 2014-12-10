import xml.etree.ElementTree as ET

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


class tdColumn(object):
    "Teradata column properties from DBC.Columns"

    CharType = None
    ColumnName = None
    ColumnFormat = None
    ColumnType = None
    ColumnLength = None
    DecimalTotalDigits = None
    DecimalFractionalDigits = None
    Nullable = None

    td_infa_ColumnType_map = {
        "AT": "time",
        "BF": "byte",
        "BV": "varbyte",
        "CF": "char",
        "CV": "varchar",
        "D" : "decimal",
        "DA": "date",
        "F" : "float",
        "I" : "integer",
        "I1": "byteint",
        "I2": "smallint",
        "I8": "bigint",
        "TS": "timestamp"
    }

    td_infa_Nullable_map = {
        "N": "NOTNULL",
        "Y": "NULL"
    }

    def __init__(self): pass

    def __toInfaPrecision(self):
        if self.ColumnType == "AT":
            infa_precision = "8"
        elif self.ColumnType == "BF":
            infa_precision = "10"
        elif self.ColumnType == "BV" or self.ColumnType == "CF" or \
             self.ColumnType == "CV":
            infa_precision = self.ColumnLength
        elif self.ColumnType == "D":
            infa_precision = self.DecimalTotalDigits
        elif self.ColumnType == "DA":
            infa_precision = "10"
        elif self.ColumnType == "F":
            infa_precision = "15"
        elif self.ColumnType == "I":
            infa_precision = "10"
        elif self.ColumnType == "I1":
            infa_precision = "3"
        elif self.ColumnType == "I2":
            infa_precision = "5"
        elif self.ColumnType == "I8":
            infa_precision = "19"
        elif self.ColumnType == "TS":
            infa_precision = "26"
        else:
            infa_precision = "Unknown"

        return infa_precision

    def toInfaSource(self):
        return self.__toInfaColumn("Source Definition")

    def toInfaTarget(self):
        return self.__toInfaColumn("Target Definition")

    def __toInfaColumn(self, Type):
        "Type is Source Definition or Target Definition"
        infa_col = infaColumn(Type, "Teradata")

        infa_col_type = self.td_infa_ColumnType_map[self.ColumnType]
        infa_col.DATATYPE = infa_col_type
        infa_col.NAME = self.ColumnName
        infa_col.NULLABLE = self.td_infa_Nullable_map[self.Nullable]
        infa_col.PRECISION = self.__toInfaPrecision()
        if infa_col_type.upper() in infaColumn.reqScale:
            infa_col.SCALE = self.DecimalFractionalDigits
        else:
            infa_col.SCALE = "0"

        return infa_col

class infaColumn(object):
    "Informatica column properties"

    '''
    Valid TransformationTypes:
        Source Definition, Source Qualifier, Expression, Target Definition
    '''
    TRANSFORMATIONTYPE = None
    '''
    Valid DatabaseTypes:
        Flat File, Teradata, Transformation
    '''
    DATABASETYPE = None
    DATATYPE = None
    FIELDNUMBER = None
    FIELDTYPE = None
    LENGTH = None
    NAME = None
    NULLABLE = None
    OFFSET = None
    PORTTYPE = None
    PRECISION = None
    SCALE = None
    properties = None

    reqScale = ["DECIMAL", "TIMESTAMP"]

    trans_type_element_tag_map = {
        "Expression": "TRANSFORMFIELD",
        "Source Definition": "SOURCEFIELD",
        "Source Qualifier": "TRANSFORMFIELD",
        "Target Definition": "TARGETFIELD"
    }

    td_trans_datatype_map = {
        "BIGINT": "BIGINT",
        "BYTE": "BINARY",
        "BYTEINT": "SMALL INTEGER",
        "CHAR": "STRING",
        "DATE": "DATE/TIME",
        "DECIMAL": "DECIMAL",
        "FLOAT": "DOUBLE",
        "INTEGER": "INTEGER",
        "SMALLINT": "SMALL INTEGER",
        "TIME": "DATE/TIME",
        "TIMESTAMP": "DATE/TIME",
        "VARBYTE": "BINARY",
        "VARCHAR": "STRING"
    }

    trans_ff_datatype_map = {
        "BIGINT": "BIGINT",
        "DATE/TIME": "DATETIME",
        "DOUBLE": "DOUBLE",
        "INTEGER": "INT",
        "NSTRING": "NSTRING",
        "NTEXT": "NSTRING",
        "SMALL INTEGER": "NUMBER",
        "STRING": "STRING",
        "TEXT": "STRING"
    }

    def __init__(self, TRANSFORMATIONTYPE = "Expression", \
                 DATABASETYPE = None, properties = None):
        self.TRANSFORMATIONTYPE = TRANSFORMATIONTYPE
        if DATABASETYPE is not None:
            self.DATABASETYPE = DATABASETYPE
        else:
            if self.TRANSFORMATIONTYPE == "Source Definition" or \
               self.TRANSFORMATIONTYPE == "Target Definition":
                self.DATABASETYPE = "Flat File"
            else:
                self.DATABASETYPE = "Transformation"
        if properties is None:
            self.properties = dict()
        self.DATATYPE = "string"
        self.NAME = "column"
        self.NULLABLE = "NULL"
        self.PORTTYPE = "INPUT/OUTPUT"
        self.PRECISION = "10"
        self.SCALE = "0"

    def validate():
        return self.datatype_validate_map[self.DATATYPE.upper()](self)
    
    def validateBIGINT():
        if (self.NAME is None) or (self.NULLABLE is None) or \
           (self.PRECISION != 19) or (self.SCALE != 0):
            return -1
        else:
            return 0
        
    def validateVARCHAR():
        if (self.NAME is None) or (self.NULLABLE is None) or \
           (self.SCALE != 0):
            return -1
        else:
            return 0
        
    def getElement(self):
        return self.db_type_getElement_map[self.DATABASETYPE](self)

    def getElementFlatFile(self):
        el = ET.Element(self.trans_type_element_tag_map[self.TRANSFORMATIONTYPE])
        el.attrib["DATATYPE"] = self.DATATYPE
        el.attrib["NAME"] = self.NAME
        el.attrib["NULLABLE"] = self.NULLABLE
        el.attrib["PRECISION"] = self.PRECISION
        el.attrib["SCALE"] = self.SCALE
        return el

    def getElementTeradata(self):
        el = ET.Element(self.trans_type_element_tag_map[self.TRANSFORMATIONTYPE])
        el.attrib["DATATYPE"] = self.DATATYPE
        el.attrib["NAME"] = self.NAME
        el.attrib["NULLABLE"] = self.NULLABLE
        el.attrib["PRECISION"] = self.PRECISION
        el.attrib["SCALE"] = self.SCALE
        return el

    def getElementTransformation(self):
        el = ET.Element(self.trans_type_element_tag_map[self.TRANSFORMATIONTYPE])
        el.attrib["DATATYPE"] = self.DATATYPE
        el.attrib["NAME"] = self.NAME
        el.attrib["NULLABLE"] = self.NULLABLE
        el.attrib["PORTTYPE"] = self.PORTTYPE
        el.attrib["PRECISION"] = self.PRECISION
        el.attrib["SCALE"] = self.SCALE

        if self.TRANSFORMATIONTYPE == "Expression":
            if self.PORTTYPE == "INPUT/OUTPUT" or self.PORTTYPE == "OUTPUT":
                el.set("EXPRESSION", self.properties.get("EXPRESSION", ""))
        return el

    def toFlatFileSource(self):
        return self.__toSourceDefinition("Flat File")

    def toFlatFileTarget(self):
        return self.__toTargetDefinition("Flat File")

    def toTeradataSource(self):
        return self.__toSourceDefinition("Teradata")

    def toTeradataTarget(self):
        return self.__toTargetDefinition("Teradata")

    def toSourceQualifier(self):
        return self.__toTransformation("Source Qualifier")

    def toExpression(self):
        return self.__toTransformation("Expression")

    def __toTransformation(self, TRANSFORMATIONTYPE):
        new_col = infaColumn(TRANSFORMATIONTYPE, "Transformation")
        #new_col.DATATYPE = self.DATATYPE
        new_col.NAME = self.NAME
        new_col.NULLABLE = self.NULLABLE
        new_col.PORTTYPE = "INPUT/OUTPUT"
        new_col.PRECISION = self.PRECISION
        new_col.SCALE = self.SCALE

        if self.DATABASETYPE == "Flat File":
            new_col.DATATYPE = self.DATATYPE
        elif self.DATABASETYPE == "Teradata":
            new_col.DATATYPE = \
                self.td_trans_datatype_map.get(self.DATATYPE.upper(), "Unknown")
        elif self.DATABASETYPE == "Transformation":
            new_col.DATATYPE = self.DATATYPE
        else:
            new_col.DATATYPE = self.DATATYPE

        if new_col.DATATYPE.upper() == "SMALL INTEGER":
            new_col.PRECISION = "5"
        if self.DATATYPE.upper() == "DATE/TIME":
            new_col.PRECISION = "29"
            new_col.SCALE = "9"

        return new_col

    def __toSourceDefinition(self, DATABASETYPE): pass

    def __toTargetDefinition(self, DATABASETYPE):
        new_col = infaColumn("Target Definition", DATABASETYPE)
        #new_col.DATATYPE = self.DATATYPE
        new_col.NAME = self.NAME
        new_col.NULLABLE = self.NULLABLE
        new_col.PRECISION = self.PRECISION
        new_col.SCALE = self.SCALE

        if new_col.DATABASETYPE == "Flat File":
            new_col.DATATYPE = \
                self.trans_ff_datatype_map.get(self.DATATYPE.upper(), "Unknown")
        elif new_col.DATABASETYPE == "Teradata":
            new_col.DATATYPE = self.DATATYPE
        else:
            new_col.DATATYPE = self.DATATYPE

        if new_col.DATATYPE.upper() == "BIGINT":
            new_col.PRECISION = "19"
        if new_col.DATATYPE.upper() == "DATETIME":
            new_col.PRECISION = "29"
            new_col.SCALE = "9"
        if new_col.DATATYPE.upper() == "INT":
            new_col.SCALE = "0"
        if new_col.DATATYPE.upper() == "NSTRING":
            new_col.SCALE = "0"
        if new_col.DATATYPE.upper() == "STRING":
            new_col.SCALE = "0"

        return new_col

    datatype_validate_map = {
        "BIGINT": validateBIGINT,
#        "BYTE": validateBYTE,
#        "BYTEINT": validateBYTEINT,
#        "CHAR": validateCHAR,
#        "DATE": validateDATE,
#        "DECIMAL": validateDECIMAL,
#        "FLOAT": validateFLOAT,
#        "INTEGER": validateINTEGER,
#        "SMALLINT": validateSMALLINT,
#        "TIME": validateTIME,
#        "VARBYTE": validateVARBYTE,
        "VARCHAR": validateVARCHAR
    }

    db_type_getElement_map = {
        "Flat File": getElementFlatFile,
        "Teradata": getElementTeradata,
        "Transformation": getElementTransformation
    }



class infaTransform(object):
    "Represents an Informatica transformation object"

    NAME = None
    REUSABLE = None
    TYPE = None
    columns_list = None
    properties = None

    type_trans_name_map = {
        "Expression": "TRANSFORMATION",
        "Source Definition": "SOURCE",
        "Source Qualifier": "TRANSFORMATION",
        "Target Definition": "TARGET"
    }

    def __init__(self, NAME = None, REUSABLE = None, TYPE = None, \
                 columns_list = None, properties = None):
        if NAME is None:
            self.NAME = "Exp1"
        else:
            self.NAME = NAME

        if REUSABLE is None:
            self.REUSABLE = "NO"
        else:
            self.REUSABLE = REUSABLE

        if TYPE is None:
            self.TYPE = "Expression"
        else:
            self.TYPE = TYPE

        if columns_list is None:
            self.columns_list = []

        if properties is None:
            self.properties = dict()

    def getElement(self):
        el = ET.Element(self.type_trans_name_map.get(self.TYPE, ""))

        if self.TYPE == "Source Definition":
            el.set("DATABASETYPE", self.properties.get("DATABASETYPE", ""))
            el.set("DBDNAME", self.properties.get("DBDNAME", ""))
            el.set("NAME", self.NAME)
        elif self.TYPE == "Target Definition":
            el.set("DATABASETYPE", self.properties.get("DATABASETYPE", ""))

            if el.get("DATABASETYPE") == "Flat File":
                ff = ET.SubElement(el, "FLATFILE")
                ff.set("CODEPAGE", self.properties.get("CODEPAGE", "UTF-8"))
                ff.set("DELIMITED", self.properties.get("DELIMITED", "YES"))
                ff.set("DELIMITERS", self.properties.get("DELIMITERS", ","))

            el.set("NAME", self.NAME)
        else:
            el.set("NAME", self.NAME)
            el.set("REUSABLE", self.REUSABLE)
            el.set("TYPE", self.TYPE)

        for col in self.columns_list:
            el.append(col.getElement())

        if self.TYPE == "Source Qualifier":
            ta = ET.SubElement(el, "TABLEATTRIBUTE")
            ta.set("NAME", "Sql Query")
            ta.set("VALUE", self.properties.get("Sql Query", ""))

        return el

    def getInstance(self):
        return infaInstance(self)
        #return infaInstance(NAME = self.NAME, REUSABLE = self.REUSABLE, \
        #    TRANSFORMATION_TYPE = self.TYPE, TYPE = "TRANSFORMATION")



class infaInstance(object):
    "Represents Informatica mapping instance"

    NAME = None
    REUSABLE = None
    TRANSFORMATION_NAME = None
    TRANSFORMATION_TYPE = None
    TYPE = None
    infaMapObj = None
    properties = None

    trans_instance_type_map = {
        "Expression": "TRANSFORMATION",
        "Source Definition": "SOURCE",
        "Source Qualifier": "TRANSFORMATION",
        "Target Definition": "TARGET"
    }

    #def __init__(self, NAME = None, REUSABLE = None, \
    #    TRANSFORMATION_NAME = None, TRANSFORMATION_TYPE = None, TYPE = None):
    def __init__(self, infaMapObj = infaTransform()):
        self.NAME = infaMapObj.NAME
        self.REUSABLE = infaMapObj.REUSABLE
        self.TRANSFORMATION_NAME = infaMapObj.NAME
        self.TRANSFORMATION_TYPE = infaMapObj.TYPE
        self.TYPE = self.trans_instance_type_map[infaMapObj.TYPE]
        self.infaMapObj = infaMapObj
        self.properties = dict()

    def getElement(self):
        el = ET.Element("INSTANCE")

        el.set("NAME", self.NAME)
        el.set("REUSABLE", self.REUSABLE)
        el.set("TRANSFORMATION_NAME", self.TRANSFORMATION_NAME)
        el.set("TRANSFORMATION_TYPE", self.TRANSFORMATION_TYPE)
        el.set("TYPE", self.TYPE)

        if self.TYPE == "SOURCE":
            el.set("DBDNAME", self.infaMapObj.properties.get("DBDNAME", ""))
        if self.TYPE == "TRANSFORMATION" and \
           self.TRANSFORMATION_TYPE == "Source Qualifier":
            se = ET.SubElement(el, "ASSOCIATED_SOURCE_INSTANCE")
            se.set("NAME", self.infaMapObj.properties \
              .get("ASSOCIATED_SOURCE_INSTANCE_NAME", ""))

        return el



class infaMapConnector(object):
    "Single connector"

    pass


def main():
    #a = infaColumn("Source Definition")
    #print(a.__dict__)

    #el = a.getElement()
    #print(el)
    #print(ET.tostring(el))
    #print("")

    #b = tdColumn()
    #b.ColumnName = "c_varchar"
    #b.ColumnType = "CV"
    #b.ColumnLength = "10"
    #b.Nullable = "Y"

    #c = b.toInfaSource()
    #print(ET.tostring(c.getElement()))
    #c = b.toInfaTarget()
    #print(ET.tostring(c.getElement()))

    #print(c.__dict__)
    #print(c.DATATYPE.upper())
    #print(infaColumn.td_trans_datatype_map.get(c.DATATYPE.upper(), "Unknown"))
    #d = c.toSourceQualifier()
    #print(ET.tostring(d.getElement()))
    pass


if __name__ == "__main__":
    main()

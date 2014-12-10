
import sys, re, copy
import xml.etree.ElementTree as ET
import xml.dom.minidom

sys.path.append(sys.path[0] + "/modules")

from informatica import *

if len(sys.argv) < 5:
    print("invalid input")
    exit()

table_name = sys.argv[1]
radd_name = sys.argv[2]
inputFile = sys.argv[3]
keysList = sys.argv[4:]
#outputFile = sys.argv[2]

try:
    f = open(inputFile, "r")
except IOError:
    print("input open error")
    exit()
else:
    # build source
    source = infaTransform(NAME = table_name, TYPE = "Source Definition")
    source.properties["DATABASETYPE"] = "Teradata"
    source.properties["DBDNAME"] = "Mozart"

    with f:
        for line in f.readlines():
            line = line.rstrip("\r\n")
            if re.match("^COLUMN\|", line) is None:
                continue

            col_data = line.split('|')
            #print(col_data)

            td_col = tdColumn()
            #td_col.DatabaseName = col_data[1]
            #td_col.TableName = col_data[2]
            td_col.CharType = col_data[3]
            td_col.ColumnName = col_data[4]
            td_col.ColumnFormat = col_data[5]
            td_col.ColumnType = col_data[6]
            td_col.ColumnLength = col_data[7]
            td_col.DecimalTotalDigits = col_data[8]
            td_col.DecimalFractionalDigits = col_data[9]
            td_col.Nullable = col_data[10]

            infa_src_col = td_col.toInfaSource()
            #print(ET.tostring(infa_src_col.getElement()))
            source.columns_list.append(infa_src_col)

    # source qualifier
    src_qual = infaTransform(NAME = "SQ_" + table_name, TYPE = "Source Qualifier")
    src_qual.properties["ASSOCIATED_SOURCE_INSTANCE_NAME"] = source.NAME
    for col in source.columns_list:
        sq_col = col.toSourceQualifier()
        src_qual.columns_list.append(sq_col)

    # build select query
    sel_query = "SELECT"
    for idx, col in enumerate(source.columns_list):
        if col.DATATYPE.upper() == "CHAR":
            sel_query += " TRIM(CAST(" + col.NAME + " AS VARCHAR(" \
                + col.PRECISION + ")))"
        elif col.DATATYPE.upper() == "DECIMAL" and col.PRECISION > 18:
            sel_query += " CAST(CAST(" + col.NAME + " AS DECIMAL(38, " + \
                col.SCALE + ") FORMAT '--(37)9') AS VARCHAR(38))"
        else:
            sel_query += " " + col.NAME

        if idx < (len(source.columns_list) - 1):
            sel_query += ","

    sel_query += " FROM pp_risk_ops_views." + table_name
        
    src_qual.properties["Sql Query"] = sel_query

    # exp transform
    exp = infaTransform(NAME = "exp_composite_keys", TYPE = "Expression")
    composite_col = infaColumn(TRANSFORMATIONTYPE = "Expression")
    composite_col.DATATYPE = "string"
    composite_col.NAME="composite_keys"
    composite_col.PORTTYPE = "OUTPUT"
    composite_col.PRECISION = "0"
    composite_col.properties["EXPRESSION"] = ""
    composite_keys = []
    composite_var = None
    for idx, col in enumerate(src_qual.columns_list):
        exp_col = col.toExpression()
        exp_col.NAME = "i_" + col.NAME
        exp_col.PORTTYPE = "INPUT"
        exp.columns_list.insert(idx, exp_col)

        if col.NAME in keysList:
            composite_keys.append(col.NAME)
            composite_col.PRECISION = str(int(composite_col.PRECISION) \
                + int(col.PRECISION) + 1)
        elif composite_var is None:
            composite_var = col.NAME
            composite_col.PRECISION = str(int(composite_col.PRECISION) \
                + int(col.PRECISION) + 1)
        else:
            exp_col = col.toExpression()
            exp_col.NAME = "o_" + col.NAME
            exp_col.PORTTYPE = "OUTPUT"
            exp_col.properties["EXPRESSION"] = "i_" + col.NAME
            exp.columns_list.append(exp_col)

    for key in keysList:
        if key in composite_keys:
            composite_col.properties["EXPRESSION"] += "i_" + key + " || ',' || "
    if composite_var is None:
        composite_var = " "
    else:
        composite_var = "i_" + composite_var
    composite_col.properties["EXPRESSION"] += composite_var
    exp.columns_list.insert(len(src_qual.columns_list), composite_col)

    date_updated = infaColumn(TRANSFORMATIONTYPE = "Expression")
    date_updated.NAME = "date_updated"
    date_updated.DATATYPE = "string"
    date_updated.PORTTYPE = "OUTPUT"
    date_updated.PRECISION = "10"
    date_updated.SCALE = "0"
    date_updated.properties["EXPRESSION"] = \
        "TO_CHAR(SESSSTARTTIME, 'YYYY-MM-DD')"
    exp.columns_list.append(date_updated)

    #print(xml.dom.minidom.parseString(ET.tostring(exp.getElement())).toprettyxml())

    # target
    tgt = infaTransform(NAME = table_name, TYPE = "Target Definition")
    tgt.properties["DATABASETYPE"] = "Flat File"
    tgt.properties["CODEPAGE"] = "UTF-8"
    tgt.properties["DELIMITED"] = "YES"
    tgt.properties["DELIMITERS"] = "|"
    for col in exp.columns_list:
        if col.PORTTYPE == "INPUT/OUTPUT" or col.PORTTYPE == "OUTPUT":
            tgt_col = col.toFlatFileTarget()
            tgt_col.NAME = re.sub("^o_", "", tgt_col.NAME)
            tgt.columns_list.append(tgt_col)

    #print(xml.dom.minidom.parseString(ET.tostring(tgt.getElement())).toprettyxml())

    # build mapping
    mapping = ET.Element("MAPPING")
    mapping.set("NAME", "m_" + radd_name)
    ins_src = source.getInstance()
    ins_src.NAME = "src_" + source.NAME
    #print(xml.dom.minidom.parseString(ET.tostring(ins_src.getElement())).toprettyxml())
    ins_sq = src_qual.getInstance()
    #print(xml.dom.minidom.parseString(ET.tostring(ins_sq.getElement())).toprettyxml())
    ins_exp = exp.getInstance()
    #print(xml.dom.minidom.parseString(ET.tostring(ins_exp.getElement())).toprettyxml())
    ins_tgt = tgt.getInstance()
    ins_tgt.NAME = "tgt_" + tgt.NAME
    #print(xml.dom.minidom.parseString(ET.tostring(ins_tgt.getElement())).toprettyxml())

    mapping.append(src_qual.getElement())
    mapping.append(exp.getElement())
    mapping.append(ins_src.getElement())
    mapping.append(ins_sq.getElement())
    mapping.append(ins_exp.getElement())
    mapping.append(ins_tgt.getElement())

    map_conn = ET.Element("CONNECTOR")

    map_conn.set("FROMINSTANCE", ins_src.NAME)
    map_conn.set("FROMINSTANCETYPE", ins_src.TRANSFORMATION_TYPE)
    map_conn.set("TOINSTANCE", ins_sq.NAME)
    map_conn.set("TOINSTANCETYPE", ins_sq.TRANSFORMATION_TYPE)
    for col in source.columns_list:
        map_conn.set("FROMFIELD", col.NAME)
        map_conn.set("TOFIELD", col.NAME)
        mapping.append(copy.deepcopy(map_conn))

    map_conn.set("FROMINSTANCE", ins_sq.NAME)
    map_conn.set("FROMINSTANCETYPE", ins_sq.TRANSFORMATION_TYPE)
    map_conn.set("TOINSTANCE", ins_exp.NAME)
    map_conn.set("TOINSTANCETYPE", ins_exp.TRANSFORMATION_TYPE)
    for col in src_qual.columns_list:
        map_conn.set("FROMFIELD", col.NAME)
        map_conn.set("TOFIELD", "i_" + col.NAME)
        mapping.append(copy.deepcopy(map_conn))

    map_conn.set("FROMINSTANCE", ins_exp.NAME)
    map_conn.set("FROMINSTANCETYPE", ins_exp.TRANSFORMATION_TYPE)
    map_conn.set("TOINSTANCE", ins_tgt.NAME)
    map_conn.set("TOINSTANCETYPE", ins_tgt.TRANSFORMATION_TYPE)
    for col in exp.columns_list:
        if col.PORTTYPE == "INPUT/OUTPUT" or col.PORTTYPE == "OUTPUT":
            map_conn.set("FROMFIELD", col.NAME)
            map_conn.set("TOFIELD", re.sub("^o_", "", col.NAME))
            mapping.append(copy.deepcopy(map_conn))

    #print(xml.dom.minidom.parseString(ET.tostring(mapping)).toprettyxml())


    # build workflow
    workflow = ET.Element("WORKFLOW")
    workflow.set("NAME", "wf_" + radd_name)
    workflow.set("REUSABLE_SCHEDULER", "NO")
    workflow.set("SCHEDULERNAME", "Scheduler")

    scheduler = ET.SubElement(workflow, "SCHEDULER")
    scheduler.set("NAME", "Scheduler")
    scheduler.set("REUSABLE", "NO")
    scheduleinfo = ET.SubElement(scheduler, "SCHEDULEINFO")
    scheduleinfo.set("SCHEDULETYPE", "ONDEMAND")

    start_task = ET.SubElement(workflow, "TASK")
    start_task.set("NAME", "Start")
    start_task.set("REUSABLE", "NO")
    start_task.set("TYPE", "Start")

    session = ET.SubElement(workflow, "SESSION")
    session.set("MAPPINGNAME", mapping.get("NAME", "mapping"))
    session.set("NAME", "s_" + mapping.get("NAME", "mapping"))
    session.set("REUSABLE", "NO")

    sess_trans_inst = ET.SubElement(session, "SESSTRANSFORMATIONINST")
    sess_trans_inst.set("ISREPARTITIONPOINT", "NO")
    sess_trans_inst.set("SINSTANCENAME", ins_src.NAME)
    sess_trans_inst.set("TRANSFORMATIONNAME", ins_src.NAME)
    sess_trans_inst.set("TRANSFORMATIONTYPE", ins_src.TRANSFORMATION_TYPE)
    sess_trans_inst = ET.SubElement(session, "SESSTRANSFORMATIONINST")
    sess_trans_inst.set("ISREPARTITIONPOINT", "YES")
    sess_trans_inst.set("SINSTANCENAME", ins_sq.NAME)
    sess_trans_inst.set("TRANSFORMATIONNAME", ins_sq.NAME)
    sess_trans_inst.set("TRANSFORMATIONTYPE", ins_sq.TRANSFORMATION_TYPE)
    sess_trans_inst = ET.SubElement(session, "SESSTRANSFORMATIONINST")
    sess_trans_inst.set("ISREPARTITIONPOINT", "NO")
    sess_trans_inst.set("SINSTANCENAME", ins_exp.NAME)
    sess_trans_inst.set("TRANSFORMATIONNAME", ins_exp.NAME)
    sess_trans_inst.set("TRANSFORMATIONTYPE", ins_exp.TRANSFORMATION_TYPE)
    sess_trans_inst = ET.SubElement(session, "SESSTRANSFORMATIONINST")
    sess_trans_inst.set("ISREPARTITIONPOINT", "YES")
    sess_trans_inst.set("SINSTANCENAME", ins_tgt.NAME)
    sess_trans_inst.set("TRANSFORMATIONNAME", ins_tgt.NAME)
    sess_trans_inst.set("TRANSFORMATIONTYPE", ins_tgt.TRANSFORMATION_TYPE)

    conf_ref = ET.SubElement(session, "CONFIGREFERENCE")
    conf_ref.set("REFOBJECTNAME", "default_session_config")
    conf_ref.set("TYPE", "Session config")
    attribute = ET.SubElement(conf_ref, "ATTRIBUTE")
    attribute.set("NAME", "Save session log for these runs")
    attribute.set("VALUE", "5")
    attribute = ET.SubElement(conf_ref, "ATTRIBUTE")
    attribute.set("NAME", "Stop on errors")
    attribute.set("VALUE", "1")

    sess_extn = ET.SubElement(session, "SESSIONEXTENSION")
    sess_extn.set("DSQINSTNAME", ins_sq.NAME)
    sess_extn.set("DSQINSTTYPE", ins_sq.TRANSFORMATION_TYPE)
    sess_extn.set("NAME", "Teradata FastExport Reader")
    sess_extn.set("SINSTANCENAME", ins_sq.infaMapObj.NAME)
    sess_extn.set("SUBTYPE", "Teradata FastExport Reader")
    sess_extn.set("TRANSFORMATIONTYPE", ins_sq.infaMapObj.TYPE)
    sess_extn.set("TYPE", "READER")
    conn_ref = ET.SubElement(sess_extn, "CONNECTIONREFERENCE")
    conn_ref.set("CNXREFNAME", "Teradata FastExport Connection")
    conn_ref.set("CONNECTIONNAME", "TDW_FASTEXP")
    conn_ref.set("CONNECTIONNUMBER", "1")
    conn_ref.set("CONNECTIONSUBTYPE", "Teradata FastExport Connection")
    conn_ref.set("CONNECTIONTYPE", "Application")
    attribute = ET.SubElement(sess_extn, "ATTRIBUTE")
    attribute.set("NAME", "Temporary File Name")
    attribute.set("VALUE", "$PMTempDir\\")

    sess_extn = ET.SubElement(session, "SESSIONEXTENSION")
    sess_extn.set("NAME", "File Writer")
    sess_extn.set("SINSTANCENAME", ins_tgt.NAME)
    sess_extn.set("SUBTYPE", "File Writer")
    sess_extn.set("TRANSFORMATIONTYPE", ins_tgt.TRANSFORMATION_TYPE)
    sess_extn.set("TYPE", "WRITER")
    attribute = ET.SubElement(sess_extn, "ATTRIBUTE")
    attribute.set("NAME", "Output file directory")
    attribute.set("VALUE", "/informatica/paypal/data/IDI_RADD")
    attribute = ET.SubElement(sess_extn, "ATTRIBUTE")
    attribute.set("NAME", "Output filename")
    attribute.set("VALUE", radd_name + ".txt")

    attribute = ET.SubElement(session, "ATTRIBUTE")
    attribute.set("NAME", "Write Backward Compatible Session Log File")
    attribute.set("VALUE", "YES")
    attribute = ET.SubElement(session, "ATTRIBUTE")
    attribute.set("NAME", "Session Log File Name")
    attribute.set("VALUE", session.get("NAME", "session") + ".log")
    attribute = ET.SubElement(session, "ATTRIBUTE")
    attribute.set("NAME", "Enable high precision")
    attribute.set("VALUE", "YES")

    taskinstance = ET.SubElement(workflow, "TASKINSTANCE")
    taskinstance.set("NAME", "Start")
    taskinstance.set("REUSABLE", "NO")
    taskinstance.set("TASKNAME", "Start")
    taskinstance.set("TASKTYPE", "Start")

    taskinstance = ET.SubElement(workflow, "TASKINSTANCE")
    taskinstance.set("FAIL_PARENT_IF_INSTANCE_DID_NOT_RUN", "YES")
    taskinstance.set("FAIL_PARENT_IF_INSTANCE_FAILS", "YES")
    taskinstance.set("NAME", session.get("NAME", "session"))
    taskinstance.set("REUSABLE", "NO")
    taskinstance.set("TASKNAME", session.get("NAME", "session"))
    taskinstance.set("TASKTYPE", "Session")

    workflowlink = ET.SubElement(workflow, "WORKFLOWLINK")
    workflowlink.set("FROMTASK", start_task.get("NAME", "Start"))
    workflowlink.set("TOTASK", session.get("NAME", "session"))

    attribute = ET.SubElement(workflow, "ATTRIBUTE")
    attribute.set("NAME", "Write Backward Compatible Workflow Log File")
    attribute.set("VALUE", "YES")
    attribute = ET.SubElement(workflow, "ATTRIBUTE")
    attribute.set("NAME", "Workflow Log File Name")
    attribute.set("VALUE", workflow.get("NAME", "workflow") + ".log")
    attribute = ET.SubElement(workflow, "ATTRIBUTE")
    attribute.set("NAME", "Save workflow log for these runs")
    attribute.set("VALUE", "3")

    #print(xml.dom.minidom.parseString(ET.tostring(workflow)).toprettyxml())

    # build final xml
    root = ET.Element("POWERMART")
    repository = ET.SubElement(root, "REPOSITORY")
    repository.set("CODEPAGE", "UTF-8")
    repository.set("NAME", "autogenerated")
    folder = ET.SubElement(repository, "FOLDER")
    folder.set("NAME", "ETL_PROD_DS_IDI_RADD")
    folder.append(source.getElement())
    folder.append(tgt.getElement())
    folder.append(mapping)
    folder.append(workflow)

    print(xml.dom.minidom.parseString(ET.tostring(root)).toprettyxml())








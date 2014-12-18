import sys, re, time
import xml.etree.ElementTree as ET
import xml.dom.minidom

def process_severity_1(event, idx, events):
    response = None
    message = event.get("message")

    if re.search("Error message is \[<<Expression Fatal Error>> \[ABORT\]:", message):
        response = ET.Element("Error")
        response.set("Class", "Duplicate time")
        resp_msg = ET.SubElement(response, "message")
        resp_msg.text = message
    elif re.search("^Database errors occurred: \nORA-01400: cannot insert NULL into", message):
        response = ET.Element("Error")
        response.set("Class", "Null KEY_1")
        resp_msg = ET.SubElement(response, "message")
        resp_msg.text = message

        find_bad_rec = 0
        for i in range(idx + 1, len(events)):
            if re.search("^\nRow # \[1\] in bad file\n", events[i].get("message", "")) and i + 1 <= len(events):
                resp_msg = ET.SubElement(response, "message")
                resp_msg.text = events[i + 1].get("message")
                break

    return response

def process_severity_3(event, idx, events):
    response = None
    message = event.get("message")

    if re.search("^Writing session output to log file \[.*\]\.$", message):
        data = re.sub(r"^.*\[(.*)\]\.$", r"\1", message)
        response = ET.Element("Info")
        response.set("SessionLogFile", data)
    elif re.search("^Initializing session \[[^\]]+\] at \[[^\]]+\]\.$", message):
        data = re.sub("^Initializing session \[([^\]]+)\] at \[([^\]]+)\]\.$", r"\1,\2", message).split(",")
        response = ET.Element("Info")
        response.set("SessionName", data[0])
        #response.set("SessionStartTime", data[1])
        response.set("SessionStartTime", time.strftime("%Y%m%d%H%M%S", time.strptime(data[1], "%a %b %d %H:%M:%S %Y")))
        response.set("Service", event.get("service"))
        response.set("ClientNode", event.get("clientNode"))
    elif re.search("^Workflow: \[([^\]]+)\] Run Instance Name: \[([^\]]+)\] Run Id: \[([^\]]+)\]$", message):
        data = re.sub("^Workflow: \[([^\]]+)\] Run Instance Name: \[([^\]]+)\] Run Id: \[([^\]]+)\]$", r"\1,\2,\3", message)
        response = ET.Element("Info")
        response.set("WorkflowName", data[0])
        response.set("RunInstanceName", data[1])
        response.set("RunId", data[2])
    elif re.search("^\n\nSESSION LOAD SUMMARY\n================================================\n$", message):
        response = ET.Element("SessionLoadSummary")
        summary_type = "Unknown"
        for i in range(idx + 1, len(events)):
            evt_msg = events[i].get("message")
            if evt_msg == "Source Load Summary.":
                summary_type = "Source"
            elif evt_msg == "Target Load Summary.":
                summary_type = "Target"
            elif re.search("^Table: \[", evt_msg):
                resp_msg = ET.SubElement(response, summary_type)
                resp_msg.text = evt_msg
            elif evt_msg == "\n===================================================\n":
                break
    elif message == "Session run completed with failure.":
        response = ET.Element("Info")
        response.set("Status", "Failure")
    elif message == "Session run completed successfully.":
        response = ET.Element("Info")
        response.set("Status", "Succeeded")
    elif re.search("^Session \[([^\]]+)\] completed at \[([^\]]+)\]\.$", message):
        data = re.sub("^Session \[([^\]]+)\] completed at \[([^\]]+)\]\.$", r"\1,\2", message).split(",")
        response = ET.Element("Info")
        #response.set("SessionEndTime", data[1])
        response.set("SessionEndTime", time.strftime("%Y%m%d%H%M%S", time.strptime(data[1], "%a %b %d %H:%M:%S %Y")))

    return response

def process_severity_5(event, idx, events):
    pass

def process_none(event, idx, events):
    pass

process_severity = {
    "1": process_severity_1,
    "3": process_severity_3,
    "5": process_severity_5
}

def parse_infa_log(log_file = None):
    try:
        log_file = sys.argv[1]
        tree = ET.parse(log_file)
        root = tree.getroot()
        if root.tag != "log":
            print("Not Informatica log xml")
            exit()

        log_detail = ET.Element("logDetails")
        events = root.findall("logEvent")

        for idx, event in enumerate(events):
            evt_severity = event.get("severity")
            response = process_severity.get(evt_severity, process_none)(event, idx, events)
            if ET.iselement(response):
                log_detail.append(response)

        return log_detail
    
    except:
        raise

def main(log_file = None):
    if len(sys.argv) != 2:
        print("Wrong number of args")
    else:
        result = parse_infa_log(sys.argv[1])
        print(xml.dom.minidom.parseString( \
            ET.tostring(result)).toprettyxml(indent = "  "))

if __name__ == "__main__":
    main(sys.argv)

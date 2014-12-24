import sys, re, time
from xml.etree import ElementTree as ET
#from lxml import etree as ET
from xml.dom import minidom
import cProfile

def process_severity_1(event, idx, events):
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
            if re.search("^\nRow # \[1\] in bad file\n",
                         events[i].get("message", "")) and i + 1 <= len(events):
                resp_msg = ET.SubElement(response, "message")
                resp_msg.text = events[i + 1].get("message")
                break
    else:
        response = None

    return response

def process_severity_3(event, idx, events):
    message = event.get("message")

    if re.search("^Writing session output to log file \[.*\]\.$", message):
        data = re.sub(r"^.*\[(.*)\]\.$", r"\1", message)
        response = ET.Element("Info")
        response.set("SessionLogFile", data)
    elif re.search("^Initializing session \[[^\]]+\] at \[[^\]]+\]\.$",
                   message):
        data = re.sub("^Initializing session \[([^\]]+)\] at \[([^\]]+)\]\.$",
                      r"\1,\2", message).split(",")
        response = ET.Element("Info")
        response.set("SessionName", data[0])
        response.set("SessionStartTime",
                     time.strftime("%Y%m%d%H%M%S",
                                   time.strptime(data[1],
                                                 "%a %b %d %H:%M:%S %Y")))
        response.set("Service", event.get("service"))
        response.set("ClientNode", event.get("clientNode"))
    elif re.search("^Workflow: \[([^\]]+)\] Run Instance Name: \[([^\]]+)\] Run Id: \[([^\]]+)\]$",
                   message):
        data = re.sub("^Workflow: \[([^\]]+)\] Run Instance Name: \[([^\]]+)\] Run Id: \[([^\]]+)\]$",
                      r"\1,\2,\3", message).split(",")
        response = ET.Element("Info")
        response.set("WorkflowName", data[0])
        response.set("RunInstanceName", data[1])
        response.set("RunId", data[2])
    elif re.search("^\n\nSESSION LOAD SUMMARY\n================================================\n$",
                   message):
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
    elif re.search("^Session \[([^\]]+)\] completed at \[([^\]]+)\]\.$",
                   message):
        data = re.sub("^Session \[([^\]]+)\] completed at \[([^\]]+)\]\.$",
                      r"\1,\2", message).split(",")
        response = ET.Element("Info")
        response.set("SessionEndTime",
                     time.strftime("%Y%m%d%H%M%S",
                                   time.strptime(data[1],
                                                 "%a %b %d %H:%M:%S %Y")))
    else:
        response = None

    return response

def process_severity_5(event, idx, events):
    pass

def process_none(events, idx):
    pass

process_severity = {
    "1": process_severity_1,
    "3": process_severity_3,
    "5": process_severity_5
}

def process_director_1(events, idx):
    response = ET.Element("Error")
    response.text = events[idx].get("message")
    return response

def process_director_3(events, idx):
    #print("director 3")
    message = events[idx].get("message")

    if re.search("^Writing session output to log file \[.*\]\.$", message):
        data = re.sub(r"^.*\[(.*)\]\.$", r"\1", message)
        response = ET.Element("Info")
        response.set("SessionLogFile", data)
    elif re.search("^Initializing session \[[^\]]+\] at \[[^\]]+\]\.$",
                   message):
        data = re.sub("^Initializing session \[([^\]]+)\] at \[([^\]]+)\]\.$",
                      r"\1,\2", message).split(",")
        response = ET.Element("Info")
        response.set("SessionName", data[0])
        response.set("SessionStartTime",
                     time.strftime("%Y%m%d%H%M%S",
                                   time.strptime(data[1],
                                                 "%a %b %d %H:%M:%S %Y")))
        response.set("Service", events[idx].get("service"))
        response.set("ClientNode", events[idx].get("clientNode"))
    elif re.search("^Workflow: \[([^\]]+)\] Run Instance Name: \[([^\]]+)\] Run Id: \[([^\]]+)\]$",
                   message):
        data = re.sub("^Workflow: \[([^\]]+)\] Run Instance Name: \[([^\]]+)\] Run Id: \[([^\]]+)\]$",
                      r"\1,\2,\3", message).split(",")
        response = ET.Element("Info")
        response.set("WorkflowName", data[0])
        response.set("RunInstanceName", data[1])
        response.set("RunId", data[2])
    elif re.search("^\n\nSESSION LOAD SUMMARY\n================================================\n$",
                   message):
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
    elif re.search("^Session \[([^\]]+)\] completed at \[([^\]]+)\]\.$",
                   message):
        data = re.sub("^Session \[([^\]]+)\] completed at \[([^\]]+)\]\.$",
                      r"\1,\2", message).split(",")
        response = ET.Element("Info")
        response.set("SessionEndTime",
                     time.strftime("%Y%m%d%H%M%S",
                                   time.strptime(data[1],
                                                 "%a %b %d %H:%M:%S %Y")))
    else:
        response = None

    return response

def process_manager_1(events, idx):
    response = ET.Element("Error")
    response.text = events[idx].get("message")
    return response

def process_manager_3(events, idx):
    pass

def process_writer_1(events, idx):
    message = events[idx].get("message")

    if re.search("^Database errors occurred: \nORA-01400: cannot insert NULL into", message):
        response = ET.Element("Error")
        response.set("Class", "Null KEY_1")
        resp_msg = ET.SubElement(response, "message")
        resp_msg.text = message

        find_bad_rec = 0
        for i in range(idx + 1, len(events)):
            if re.search("^\nRow # \[[0-9]+\] in bad file\n",
                         events[i].get("message", "")) and i + 1 <= len(events):
                resp_msg = ET.SubElement(response, "message")
                resp_msg.text = events[i + 1].get("message")
                break
    else:
        response = None

    return response

def process_writer_3(events, idx):
    pass

process_thread_severity = {
    ("DIRECTOR", "1"): process_director_1,
    ("DIRECTOR", "3"): process_director_3,
    ("MANAGER", "1"): process_manager_1,
    ("MANAGER", "3"): process_manager_3,
    ("WRITER_1_*_1", "1"): process_writer_1,
    ("WRITER_1_*_1", "3"): process_writer_3,
    ("WRITER_1_*_2", "1"): process_writer_1,
    ("WRITER_1_*_2", "3"): process_writer_3,
    ("WRITER_1_*_3", "1"): process_writer_1,
    ("WRITER_1_*_3", "3"): process_writer_3,
    ("WRITER_1_*_4", "1"): process_writer_1,
    ("WRITER_1_*_4", "3"): process_writer_3,
    ("WRITER_1_*_5", "1"): process_writer_1,
    ("WRITER_1_*_5", "3"): process_writer_3
}

def parse(log_file = None):
    tree = ET.parse(log_file)
    root = tree.getroot()
    if root.tag != "log":
        print("Not Informatica log xml")
        exit()

    log_detail = ET.Element("logDetails")
    events = root.findall("logEvent")

    idx = 0
    for idx in range(idx, len(events)):
        evt_thread = events[idx].get("threadName")
        evt_severity = events[idx].get("severity")
        response = process_thread_severity.get(
            (evt_thread, evt_severity), process_none)(events, idx)
#        response = process_severity.get(evt_severity, process_none)(event, idx, events)
#        if evt_severity == 1:
#            response = process_severity.get(evt_severity, process_none)(event, idx, events)
#        else:
#            response = None
        #response = None
        if ET.iselement(response):
            log_detail.append(response)

    return log_detail

def main(log_file = None):
    if len(sys.argv) != 2:
        print("Wrong number of args")
    else:
        result = parse(sys.argv[1])
        print(minidom.parseString(ET.tostring(result))
              .toprettyxml(indent = "  "))

if __name__ == "__main__":
    main(sys.argv)
    #cProfile.run("main(sys.argv)")


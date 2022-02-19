#!/usr/bin/env python3

import os
import pickledb
import logging
import signal
import argparse
import xml.etree.ElementTree as ET
from xml.dom import minidom
from queue import Queue

import b9py

QMAX = 10
_parameter_namespace_pubs = {}


# Control-C handler
def shutdown_handler(_sig, _frame):
    os.kill(os.getpid(), signal.SIGKILL)


def get_parameter_from_db(namespace, name):
    global dbParam

    param_name = namespace + "/" + name
    if not dbParam.exists(param_name):
        logging.debug("{} parameter is not set.".format(param_name))
        result = {'namespace': namespace,
                  'type': 'String',
                  'name': name,
                  'value': '?',
                  'found': False}
        msg = b9py.MessageFactory.create_message_dictionary(result)
    else:
        existing_item = dbParam.get(param_name)
        existing_item['found'] = True
        msg = b9py.MessageFactory.create_message_dictionary(existing_item)

    return msg


def put_parameter_in_db(namespace, nodename, value_type, name, value, publish_change=False):
    global dbParam

    # Create the parameter namespace queue
    if namespace not in _parameter_namespace_pubs.keys():
        q = Queue(maxsize=QMAX)
        _parameter_namespace_pubs[namespace] = [q]

    pname = name
    pos = None
    if ':' in name:
        pname, pos = name.split(":")

    # Create the change entry and place it in the parameter's namespace queue
    param_name = namespace + "/" + pname
    param_entry = {'nodename': nodename,
                   'namespace': namespace,
                   'type': value_type,
                   'name': pname,
                   'position': pos,
                   'value': value}

    # Add an element to a List or Dict
    if pos is not None:
        # Add an element to an existing List of Dict parameter value
        if dbParam.exists(param_name):
            # Only can modify an existing parameter List or Dict value
            existing_item = dbParam.get(param_name)
            if type(existing_item['value']) == list:
                if pos == '[':
                    # Add to head
                    existing_item['value'].insert(0, value)
                elif pos == ']':
                    # Add to tail
                    existing_item['value'].append(value)
                else:
                    logging.error("Invalid position specified for a list. Must be a '[' or ']'")
                param_entry['value'] = existing_item['value']
                param_entry['type'] = b9py.Message.MSGTYPE_LIST

            elif type(existing_item['value']) == dict:
                existing_item['value'][pos] = value
                param_entry['value'] = existing_item['value']
                param_entry['type'] = b9py.Message.MSGTYPE_DICT

            else:
                logging.warning(
                    "Parameter '{}' must be a List or Dict to add elements.".format(param_name))
        else:
            logging.warning(
                "Parameter '{}' must initialized to a List or Dict to add elements.".format(param_name))

    dbParam.set(param_name, param_entry)

    if publish_change:
        # Queue up changed parameter to be published
        queue: Queue = _parameter_namespace_pubs[namespace][0]
        if queue.qsize() == QMAX:
            queue.get_nowait()
        queue.put_nowait(param_entry)


def load_parameter_from_xml(element: ET.Element, publish_change):
    put_parameter_in_db(element.attrib['ns'], "default",
                        element.attrib['type'], element.attrib['name'], element.attrib['value'],
                        publish_change=publish_change)


def load_parameters_from_file(filename, namespace, publish_change):
    try:
        tree = ET.parse("parameters/" + filename)

        # Process each parameter node
        root = tree.getroot()
        for child in root:
            if child.tag == "parameter":
                if namespace == '@' or namespace == child.attrib['ns']:
                    load_parameter_from_xml(child, publish_change)

        logging.info("Loaded parameters file named '{}'.".format(args['parameters']))
        return True

    except FileNotFoundError:
        logging.warning("Parameters file named '{}' not found.".format(args['parameters']))
        return False


def save_parameters_to_file(filename, namespace):
    global dbParam

    root = ET.Element('parameters')
    for pname in dbParam.getall():
        existing_item = dbParam.get(pname)

        if namespace == '@' or namespace == existing_item['namespace']:
            param_el = ET.SubElement(root, 'parameter')
            param_el.attrib["ns"] = existing_item['namespace']
            param_el.attrib["name"] = existing_item['name']
            param_el.attrib["type"] = existing_item['type']
            param_el.attrib["value"] = existing_item['value']

    xmlstr = minidom.parseString(ET.tostring(root)).toprettyxml(indent="   ")
    with open("parameters/" + filename, "w") as f:
        f.write(xmlstr)


def parameter_cb(_request_topic, message: b9py.Message):
    global dbParam

    msg = b9py.MessageFactory.create_message_string("OK", param_srv.name)

    # Put a value in the parameter db
    if message.data['cmd'].lower() == 'put':
        put_parameter_in_db(message.data['namespace'], message.data['nodename'],
                            message.data['type'], message.data['name'], message.data['value'],
                            publish_change=True)

    # Get a value from the parameter db
    elif message.data['cmd'].lower() == 'get':
        msg = get_parameter_from_db(message.data['namespace'], message.data['name'])

    # List all name/values in the parameter db
    elif message.data['cmd'].lower() == 'list':
        item_list = []
        for pname in dbParam.getall():
            existing_item = dbParam.get(pname)
            if message.data['namespace'] == '@' or message.data['namespace'] == existing_item['namespace']:
                item_list.append(existing_item)
        msg = b9py.MessageFactory.create_message_list(item_list)

    # Purge all values from the parameter db
    elif message.data['cmd'].lower() == 'purge':
        if message.data['namespace'] == '@':
            dbParam.deldb()
        else:
            pkeys = []
            for key in dbParam.getall():
                pkeys.append(key)
            for pname in pkeys:
                existing_item = dbParam.get(pname)
                if message.data['namespace'] == existing_item['namespace']:
                    dbParam.rem(pname)

    # Save all parameter db to an XML file
    elif message.data['cmd'].lower() == 'save':
        save_parameters_to_file(message.data['filename'], message.data['namespace'])

    # Load parameter db from an XML file
    elif message.data['cmd'].lower() == 'load':
        load_parameters_from_file(message.data['filename'], message.data['namespace'], message.data['publish_change'])

    return msg


def process_changes():
    global _parameter_namespace_pubs

    for param_ns in _parameter_namespace_pubs:
        entry = _parameter_namespace_pubs[param_ns]
        if len(entry) == 1:
            # Create parameter changed publisher for this parameter's namespace
            change_topic = "parameter/changed"
            pub = b9.create_publisher(change_topic, b9py.Message.MSGTYPE_DICT, param_ns)
            status = pub.advertise()
            if status.is_successful:
                _parameter_namespace_pubs[param_ns] = [entry[0], pub]
            else:
                _parameter_namespace_pubs[param_ns] = [entry[0], None]
                logging.error("Publisher for /{}/{} failed to advertise.".format(param_ns, change_topic))
        else:
            if entry[1] is not None:
                # We have a publisher so publish the damn parameter change to anyone who fucking cares
                queue, pub = _parameter_namespace_pubs[param_ns]
                if queue.qsize() > 0:
                    param_entry = queue.get_nowait()
                    logging.info("Parameter change: {}, {} = {}".format(param_ns,
                                                                        param_entry['name'],
                                                                        param_entry['value']))
                    msg = b9py.MessageFactory.create_message_dictionary(param_entry)
                    pub.publish(msg)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown_handler)

    ap = argparse.ArgumentParser()
    ap.add_argument("-n", "--nodename", type=str, default="parameter_server", help="node name")
    ap.add_argument("-s", "--namespace", type=str, default="", help="parameter service namespace")
    ap.add_argument("-t", "--topic", type=str, default="parameters", help="parameter service topic")
    ap.add_argument('-p', '--parameters', default="default.parm", help="default parameters file name")
    ap.add_argument('-c', '--publish_change', default=True, help="create change publishers")
    args = vars(ap.parse_args())

    b9 = b9py.B9(args['nodename'])
    b9.start_logger(level=logging.INFO)

    # Database
    dbParam = pickledb.load('param_reg.db', auto_dump=False)

    # Setup Parameter Service
    param_srv = b9.create_service(args['topic'], b9py.Message.MSGTYPE_PARAMETER, parameter_cb, args['namespace'])
    stat = param_srv.advertise()
    if stat.is_successful:
        logging.info("Started parameter server on topic {}".format(param_srv.topic))
    else:
        logging.error("Unable to start parameter service node. {}".format(stat.status_type))

    # Load default parameters into db
    load_parameters_from_file(args['parameters'], '@', args['publish_change'])

    while b9.spin_once(1.0 / QMAX):
        process_changes()

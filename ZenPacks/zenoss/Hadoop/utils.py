######################################################################
#
# Copyright (C) Zenoss, Inc. 2014, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is
# installed.
#
######################################################################
import json
import re
from base64 import encodestring

from Products.DataCollector.plugins.DataMaps import ObjectMap
from Products.ZenUtils.Utils import prepId
import xml.etree.cElementTree as ET

# Useful for components' ids.
NAME_SPLITTER = '(.)'


NODE_HEALTH_NORMAL = 'Normal'
NODE_HEALTH_DEAD = 'Dead'
NODE_HEALTH_DECOM = 'Decommissioned'


def node_oms(log, device, data, state, result, remodel=False):
    """Builds node OMs"""
    maps = []
    nodes = json.loads(data)
    # Get Data Node port
    datanode = ('dfs.datanode.http.address')
    try:
        data_node_port = get_attr(datanode, ET.fromstring(result))
        port = data_node_port.split(':')[1]
    except (IndexError, AttributeError, ET.ParseError):
        port = '50075'

    # Get device id
    try:
        dev_id = device.id
    except AttributeError:
        dev_id = device.config_key[0]

    for node_name, node_data in nodes.iteritems():
        title = prep_ip(device, node_name + ':' + port)
        log.debug(node_name)
        node_id = dev_id + NAME_SPLITTER + title.split(':')[0]
        # Do not update 'last_contacted' property on remodeling
        # to avoid 'Change/Set' events, as it changes a lot.
        if remodel:
            maps.append(ObjectMap({
                'id': prepId(node_id),
                'title': title,
                'health_state': state,
            }))
        else:
            maps.append(ObjectMap({
                'id': prepId(node_id),
                'title': title,
                'health_state': state,
                'last_contacted': node_data['lastContact']
            }))
    return maps


def prep_ip(device, val, data=None):
    """
    Check if node IP is equal to host or it has a specified identifier
    and replace with needed IP
    """
    if not val:
        return ""
    ip, port = val.split(":")
    local = ['127.0.0.1', '0.0.0.0', 'localhost.localdomain', 'localhost']
    match = re.match('.*\$\{([\w\d\.]+)\}.*', ip)
    if data and match:
        ip = get_attr(match.group(1), data)

    if ip in local:
        ip = device.manageIp
    return ip + ":" + port


def get_attr(attrs, result, default=""):
    """
    Look for the attribute in configuration data.

    @param attrs: possible names of the attribute in conf data
    @type attrs: tuple
    @param result: parsed result of command output
    @type result: ET Element object
    @param default: optional, a value to be returned as a default
    @type default: str
    @return: the attribute value
    """
    for prop in result.findall('property'):
        if prop.findtext('name') in attrs:
            return prop.findtext('value')
    return default


def hadoop_url(scheme, port, host, endpoint):
    """
    Constructs URL to access Hadoop REST interface.
    """
    url = '{}://{}:{}{}'.format(scheme, host, port, endpoint)
    return url


def hadoop_headers(accept, username, passwd):
    """
    Constructs headers to access Hadoop REST interface.
    """
    auth = encodestring(
        '{}:{}'.format(username, passwd)
    )
    authHeader = "Basic " + auth.strip()
    return {
        "Accept": accept,
        "Authorization": authHeader,
        'Proxy-Authenticate': authHeader
    }

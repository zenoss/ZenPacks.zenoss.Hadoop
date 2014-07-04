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

from Products.DataCollector.plugins.DataMaps import ObjectMap
from Products.ZenUtils.Utils import prepId

# Useful for components' ids.
NAME_SPLITTER = '(.)'


NODE_HEALTH_NORMAL = 'Normal'
NODE_HEALTH_DEAD = 'Dead'
NODE_HEALTH_DECOM = 'Decommissioned'


def node_oms(log, dev_id, data, state, remodel=False):
    """Builds node OMs"""
    maps = []
    nodes = json.loads(data)
    for node_name, node_data in nodes.iteritems():
        log.debug(node_name)
        node_id = dev_id + NAME_SPLITTER + node_name
        # Do not update 'last_contacted' property on remodeling
        # to avoid 'Change/Set' events, as it changes a lot.
        if remodel:
            maps.append(ObjectMap({
                'id': prepId(node_id),
                'title': node_name,
                'health_state': state,
            }))
        else:
            maps.append(ObjectMap({
                'id': prepId(node_id),
                'title': node_name,
                'health_state': state,
                'last_contacted': node_data['lastContact']
            }))
    return maps

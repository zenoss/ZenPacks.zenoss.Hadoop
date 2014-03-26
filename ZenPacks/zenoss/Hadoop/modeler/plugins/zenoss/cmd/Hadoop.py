##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2014, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################

import json
import collections
from itertools import chain
import re

from Products.ZenUtils.Utils import prepId
from Products.DataCollector.plugins.DataMaps import ObjectMap, RelationshipMap
from Products.DataCollector.plugins.CollectorPlugin import CommandPlugin

from ZenPacks.zenoss.Hadoop.utils import NAME_SPLITTER


class Hadoop(CommandPlugin):
    """
    A command plugin for Hadoop
    """

    command = "/usr/bin/curl -i http://localhost:50070/jmx"

    def process(self, device, results, log):
        log.info('Collecting Hadoop nodes for device %s' % device.id)

        maps = collections.OrderedDict([
            ('hadoop_service_nodes', []),
            ('hadoop_data_nodes', []),
        ])

        # TODO: add try ... except on below code to catch bad-data

        #print results

        # Skip HTTP header
        res = '\n'.join(results.split('\n')[4:]) 

        data = json.loads(res)
        for bean in data['beans']:
            if bean['name'] == 'Hadoop:service=NameNode,name=NameNodeInfo':
                self._node_oms(maps, bean["LiveNodes"], 'Normal')
                self._node_oms(maps, bean["DeadNodes"], 'Dead')
                self._node_oms(maps, bean["DecomNodes"], 'Decommissioned')

        return list(chain.from_iterable(maps.itervalues()))

    def _node_oms(self, maps, data, health_state):
        """Builds node OMs"""

        nodes = json.loads(data)
        for node_name, node_data in nodes.iteritems():
            maps['hadoop_data_nodes'].append(ObjectMap({
                'id': prepId(node_name),
                'title': node_name,
                'health_state': health_state,
                'last_contacted': node_data['lastContact']
            }))

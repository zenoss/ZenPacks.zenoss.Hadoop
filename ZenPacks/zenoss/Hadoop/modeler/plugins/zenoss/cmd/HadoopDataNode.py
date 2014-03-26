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

from ZenPacks.zenoss.Hadoop import MODULE_NAME
from ZenPacks.zenoss.Hadoop.utils import NAME_SPLITTER


class HadoopDataNode(CommandPlugin):
    """
    A command plugin for Hadoop to look for Data Nodes
    """

    command = "/usr/bin/curl -i -s http://localhost:50070/jmx"

    def process(self, device, results, log):
        log.info('Collecting Hadoop nodes for device %s' % device.id)

        maps = collections.OrderedDict([
            # ('hadoop_service_nodes', []),
            ('hadoop_data_nodes', []),
        ])

        # TODO: add try ... except on below code to catch bad-data

        # print results

        # Skip HTTP header
        res = '\n'.join(results.split('\n')[4:]) 

        data = json.loads(res)
        node_oms = []
        for bean in data['beans']:
            if bean['name'] == 'Hadoop:service=NameNode,name=NameNodeInfo':
                node_oms.extend(
                    self._node_oms(bean["LiveNodes"], 'Normal'))
                node_oms.extend(
                    self._node_oms(bean["DeadNodes"], 'Dead'))
                node_oms.extend(
                    self._node_oms(bean["DecomNodes"], 'Decommissioned'))

        maps['hadoop_data_nodes'].append(RelationshipMap(
            relname='hadoop_data_nodes',
            modname=MODULE_NAME['HadoopDataNode'],
            objmaps=node_oms))

        # Clear non-existing component events.
        # maps['device'].append(ObjectMap({
        #     'getClearEvents': True
        # }))

        log.info(
            'Modeler %s finished processing data for device %s',
            self.name(), device.id
        )

        return list(chain.from_iterable(maps.itervalues()))

    def _node_oms(self, data, health_state):
        """Builds node OMs"""
        maps = []
        nodes = json.loads(data)
        for node_name, node_data in nodes.iteritems():
            maps.append(ObjectMap({
                'id': prepId(node_name),
                'title': node_name,
                'health_state': health_state,
                'last_contacted': node_data['lastContact']
            }))
        return maps

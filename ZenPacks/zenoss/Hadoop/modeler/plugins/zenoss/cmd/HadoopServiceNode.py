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


class HadoopServiceNode(CommandPlugin):
    """
    A command plugin for Hadoop to look for Job Tracker / Secondary
    Name nodes
    """

    command = "/usr/bin/curl -i -s http://localhost:50070/conf"

    def process(self, device, results, log):
        log.info('Collecting Hadoop nodes for device %s' % device.id)

        maps = collections.OrderedDict([
            ('hadoop_service_nodes', []),
            # ('hadoop_data_nodes', []),
        ])

        # TODO: add try ... except on below code to catch bad-data

        # print results

        # # Skip HTTP header if you plan to parse XML
        # res = '\n'.join(results.split('\n')[4:])

        node_oms = []

        jobtracker = self._get_attr('mapred.job.tracker.http.address', results)
        log.debug('Jobtracker Node: %s' % jobtracker)
        if jobtracker:
            node_oms.append(ObjectMap({
                'id': prepId(jobtracker),
                'title': jobtracker,
                'node_type': 'Job Tracker',
            }))

        secondary = self._get_attr('dfs.secondary.http.address', results)
        log.debug('Secondary Name Node: %s' % secondary)
        if secondary:
            node_oms.append(ObjectMap({
                'id': prepId(secondary),
                'title': secondary,
                'node_type': 'Secondary Name Node',
            }))

        maps['hadoop_service_nodes'].append(RelationshipMap(
            relname='hadoop_service_nodes',
            modname=MODULE_NAME['HadoopServiceNode'],
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

    def _get_attr(self, attr, val, default=""):
        """Look for attribute in configuration"""
        try:
            res = re.search('<name>%s</name><value>(.+?)</value>' % attr, val).group(1)
            return res or default
        except AttributeError:
            return default

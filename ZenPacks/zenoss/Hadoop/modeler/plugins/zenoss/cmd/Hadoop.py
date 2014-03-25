##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2014, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################


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

        return list(chain.from_iterable(maps.itervalues()))

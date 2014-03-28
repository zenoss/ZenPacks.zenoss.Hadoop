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

    command = "/usr/bin/curl -s http://localhost:50070/conf"

    def process(self, device, results, log):
        log.info('Collecting Hadoop nodes for device %s' % device.id)

        maps = collections.OrderedDict([
            ('hadoop_job_tracker', []),
            ('hadoop_secondary_name_node', []),
        ])

        # TODO: add try ... except on below code to catch bad-data

        # print results

        jobtracker = self._prep_ip(device,
            self._get_attr('mapred.job.tracker.http.address', results)
        )
        log.debug('Jobtracker Node: %s' % jobtracker)
        if jobtracker:
            maps['hadoop_job_tracker'].append(RelationshipMap(
                relname='hadoop_job_tracker',
                modname=MODULE_NAME['HadoopJobTracker'],
                objmaps=[
                    ObjectMap({
                        'id': prepId(jobtracker),
                        'title': jobtracker,
                        'node_type': 'Job Tracker',
                    })
                ]))


        secondary = self._prep_ip(device,
            self._get_attr('dfs.secondary.http.address', results)
        )
        log.debug('Secondary Name Node: %s' % secondary)
        if secondary:
            maps['hadoop_secondary_name_node'].append(RelationshipMap(
                relname='hadoop_secondary_name_node',
                modname=MODULE_NAME['HadoopSecondaryNameNode'],
                objmaps=[
                    ObjectMap({
                        'id': prepId(secondary),
                        'title': secondary,
                        'node_type': 'Secondary Name Node',
                    })
                ]))

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

    def _prep_ip(self, device, val):
        """Check if node IP is equal to host and replace with host's IP"""
        if not val:
            return ""

        ip, port = val.split(":")
        if ip == "0.0.0.0":
            ip = device.manageIp

        return ip + ":" + port

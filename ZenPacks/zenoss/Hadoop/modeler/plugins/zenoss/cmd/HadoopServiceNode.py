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
import xml.etree.cElementTree as ET

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
    command = ("/usr/bin/curl -s http://localhost:50070/conf &&"
               " /usr/bin/curl -s http://localhost:50070/jmx")

    def process(self, device, results, log):
        log.info('Collecting Hadoop nodes for device %s' % device.id)

        maps = collections.OrderedDict([
            ('hadoop_job_tracker', []),
            ('hadoop_task_tracker', []),
            ('hadoop_secondary_name_node', []),
            ('hadoop_resource_manager', []),
            ('hadoop_node_manager', []),
            ('hadoop_job_history', []),
        ])

        dict_components = {
            'HadoopJobTracker': (
                'Job Tracker',
                'hadoop_job_tracker',
                (
                    'mapred.job.tracker.http.address',  # Deprecated
                    'mapreduce.jobtracker.http.address'  # New
                )
            ),
            'HadoopTaskTracker': (
                'Task Tracker',
                'hadoop_task_tracker',
                (
                    'mapred.task.tracker.http.address',  # Deprecated
                    'mapreduce.tasktracker.http.address'  # New
                )
            ),
            'HadoopSecondaryNameNode': (
                'Secondary Name Node',
                'hadoop_secondary_name_node',
                (
                    'dfs.secondary.http.address',  # Deprecated
                    'dfs.namenode.secondary.http-address'  # New
                )
            ),
            'HadoopResourceManager': (
                'Resource Manager',
                'hadoop_resource_manager',
                (
                    'yarn.resourcemanager.webapp.address'
                )
            ),
            'HadoopNodeManager': (
                'Node Manager',
                'hadoop_node_manager',
                (
                    'yarn.nodemanager.webapp.address'
                )
            ),
            'HadoopJobHistory': (
                'Job History',
                'hadoop_job_history',
                (
                    'mapreduce.jobhistory.webapp.address'
                )
            )
        }

        try:
            result = results.split('</configuration>')
            results = ET.fromstring(result[0] + '</configuration>')
            data = json.loads(result[1])
        except (TypeError, IndexError, ValueError, ET.ParseError) as err:
            log.error('Modeler %s failed to parse the result.' % self.name())
            return

        for bean in data['beans']:
            if bean['name'] == 'Hadoop:service=NameNode,name=NameNodeInfo':
                hadoop_version = bean["Version"]

        def build_relations(component):
            """
            Receive component's name and build relationships to device
            """
            data = dict_components[component]
            component_name = self._get_attr(data[2], results)
            log.debug(data[0] + ': {}'.format(component_name))
            if component_name:
                maps[data[1]].append(RelationshipMap(
                    relname=data[1],
                    modname=MODULE_NAME[component],
                    objmaps=[
                        ObjectMap({
                            'id': prepId(component_name),
                            'title': component_name,
                            'node_type': data[0],
                        })
                    ]))

        build_relations('HadoopSecondaryNameNode')

        # Check Hadoop version and add components according it.
        if hadoop_version.startswith('0') or hadoop_version.startswith('1'):
            build_relations('HadoopJobTracker')
            build_relations('HadoopTaskTracker')
        else:
            build_relations('HadoopResourceManager')
            build_relations('HadoopNodeManager')
            build_relations('HadoopJobHistory')

            # Clear non-existing component events.
            # maps['device'].append(ObjectMap({
            #     'getClearEvents': True
            # }))

            log.info(
                'Modeler %s finished processing data for device %s',
                self.name(), device.id
            )
        return list(chain.from_iterable(maps.itervalues()))

    def _get_attr(self, attrs, result, default=""):
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

    def _prep_ip(self, device, val):
        """Check if node IP is equal to host and replace with host's IP"""
        if not val:
            return ""

        ip, port = val.split(":")
        if ip == "0.0.0.0":
            ip = device.manageIp

        return ip + ":" + port

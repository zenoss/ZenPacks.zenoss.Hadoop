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

        secondary_property_names = (
            'dfs.secondary.http.address',  # Deprecated
            'dfs.namenode.secondary.http-address'  # New
        )
        secondary = self._prep_ip(
            device, self._get_attr(secondary_property_names, results)
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

        # Check Hadoop version and add components according it.
        if hadoop_version.startswith('0') or hadoop_version.startswith('1'):
            jobtracker_property_names = (
                'mapred.job.tracker.http.address',  # Deprecated
                'mapreduce.jobtracker.http.address'  # New
            )
            jobtracker = self._prep_ip(
                device, self._get_attr(jobtracker_property_names, results)
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

            tasktracker_property_names = (
                'mapred.task.tracker.http.address',  # Deprecated
                'mapreduce.tasktracker.http.address'  # New
            )
            tasktracker = self._prep_ip(
                device, self._get_attr(tasktracker_property_names, results)
            )
            log.debug('Task Tracker: %s' % tasktracker)
            if jobtracker:
                maps['hadoop_task_tracker'].append(RelationshipMap(
                    relname='hadoop_task_tracker',
                    modname=MODULE_NAME['HadoopTaskTracker'],
                    objmaps=[
                        ObjectMap({
                            'id': prepId(tasktracker),
                            'title': tasktracker,
                            'node_type': 'Task Tracker',
                        })
                    ]))

        else:
            resourcemanager_property_names = (
                'yarn.resourcemanager.webapp.address'
            )
            resourcemanager = self._prep_ip(
                device, self._get_attr(resourcemanager_property_names, results)
            )
            log.debug('Resource Manager: %s' % resourcemanager)
            if resourcemanager:
                maps['hadoop_resource_manager'].append(RelationshipMap(
                    relname='hadoop_resource_manager',
                    modname=MODULE_NAME['HadoopResourceManager'],
                    objmaps=[
                        ObjectMap({
                            'id': prepId(resourcemanager),
                            'title': resourcemanager,
                            'node_type': 'Resource Manager',
                        })
                    ]))

            nodemanager_property_names = (
                'yarn.nodemanager.webapp.address'
            )
            nodemanager = self._prep_ip(
                device, self._get_attr(nodemanager_property_names, results)
            )
            log.debug('Node Manager: %s' % nodemanager)
            if nodemanager:
                maps['hadoop_node_manager'].append(RelationshipMap(
                    relname='hadoop_node_manager',
                    modname=MODULE_NAME['HadoopNodeManager'],
                    objmaps=[
                        ObjectMap({
                            'id': prepId(nodemanager),
                            'title': nodemanager,
                            'node_type': 'Node Manager',
                        })
                    ]))

            jobhistory_property_names = (
                'mapreduce.jobhistory.webapp.address'
            )
            jobhistory = self._prep_ip(
                device, self._get_attr(jobhistory_property_names, results)
            )
            log.debug('Job History: %s' % jobhistory)
            if nodemanager:
                maps['hadoop_job_history'].append(RelationshipMap(
                    relname='hadoop_job_history',
                    modname=MODULE_NAME['HadoopJobHistory'],
                    objmaps=[
                        ObjectMap({
                            'id': prepId(jobhistory),
                            'title': jobhistory,
                            'node_type': 'Job History',
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
        print list(chain.from_iterable(maps.itervalues()))
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

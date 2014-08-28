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
import zope.component

from Products.ZenUtils.Utils import prepId
from Products.DataCollector.plugins.DataMaps import ObjectMap, RelationshipMap
from Products.ZenCollector.interfaces import IEventService
from Products.DataCollector.plugins.CollectorPlugin import PythonPlugin
from twisted.web.client import getPage
from twisted.internet import defer
from ZenPacks.zenoss.Hadoop import MODULE_NAME
from ZenPacks.zenoss.Hadoop.utils import \
    NAME_SPLITTER, hadoop_url, hadoop_headers, get_attr, prep_ip, check_error


class HadoopServiceNode(PythonPlugin):
    """
    A python plugin for Hadoop to look for Job Tracker / Secondary
    Name nodes
    """

    deviceProperties = PythonPlugin.deviceProperties + (
        'zHadoopScheme',
        'zHadoopUsername',
        'zHadoopPassword',
        'zHadoopNameNodePort',
    )
    _eventService = zope.component.queryUtility(IEventService)

    @defer.inlineCallbacks
    def collect(self, device, log):

        result = {}

        jmx_url = hadoop_url(
            scheme=device.zHadoopScheme,
            port=device.zHadoopNameNodePort,
            host=device.manageIp,
            endpoint='/jmx'
        )
        conf_url = hadoop_url(
            scheme=device.zHadoopScheme,
            port=device.zHadoopNameNodePort,
            host=device.manageIp,
            endpoint='/conf'
        )
        headers = hadoop_headers(
            accept='application/json',
            username=device.zHadoopUsername,
            passwd=device.zHadoopPassword
        )

        try:
            result['jmx'] = yield getPage(jmx_url, headers=headers)
            result['conf'] = yield getPage(conf_url, headers=headers)
        except Exception, e:
            self.on_error(log, device, e)
        self.on_success(log, device)
        defer.returnValue(result)

    def process(self, device, result, log):

        log.info('Collecting Hadoop nodes for device %s' % device.id)

        hadoop_version = None

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
            results = ET.fromstring(result['conf'])
            data = json.loads(result['jmx'])
        except (TypeError, KeyError, ValueError, ET.ParseError):
            log.error('Modeler %s failed to parse the result.' % self.name())
            return

        for bean in data['beans']:
            if bean['name'] == 'Hadoop:service=NameNode,name=NameNodeInfo':
                hadoop_version = bean["Version"]

        if hadoop_version is None:
            log.error('HadoopServiceNode: Error parsing collected data')
            return

        def build_relations(component):
            """
            Receive component's name and build relationships to device
            """
            data = dict_components[component]
            component_name = prep_ip(
                device, get_attr(data[2], results), results
            )
            log.debug('{0}: {1}'.format(data[0], component_name))
            if component_name:
                maps[data[1]].append(RelationshipMap(
                    relname=data[1],
                    modname=MODULE_NAME[component],
                    objmaps=[
                        ObjectMap({
                            'id': prepId(
                                device.id + NAME_SPLITTER + component_name
                            ),
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

        log.info(
            'Modeler %s finished processing data for device %s',
            self.name(), device.id
        )
        return list(chain.from_iterable(maps.itervalues()))

    def on_error(self, log, device, failure):

        try:
            e = failure.value
        except:
            e = failure  # no twisted failure
        e = check_error(e, device.id) or e
        log.error(e)
        self._send_event(str(e), device.id, 5)
        raise e

    def on_success(self, log, device):
        self._send_event("Successfull modeling", device.id, 0)

    def _send_event(self, reason, id, severity, force=False):

        """
        Send event for device with specified id, severity and
        error message.
        """

        if self._eventService:

            self._eventService.sendEvent(dict(
                summary=reason,
                eventClass='/Status',
                device=id,
                eventKey='HadoopServiceNode_ConnectionError',
                severity=severity,
            ))
            return True
        else:
            if force or (severity > 0):
                self.device_om = ObjectMap({
                    'setErrorNotification': reason
                })

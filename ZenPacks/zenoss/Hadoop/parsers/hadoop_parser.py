##############################################################################
#
# Copyright (C) Zenoss, Inc. 2014, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

import json
import logging

import zope.interface
from twisted.python.failure import Failure

from Products.DataCollector.plugins.DataMaps import ObjectMap, RelationshipMap
from Products.ZenCollector.interfaces import ICollector
from Products.ZenEvents import ZenEventClasses
from Products.ZenRRD.CommandParser import CommandParser
from Products.ZenUtils.Utils import getExitMessage

from ZenPacks.zenoss.Hadoop import MODULE_NAME
from ZenPacks.zenoss.Hadoop.utils import (
    NODE_HEALTH_NORMAL, NODE_HEALTH_DEAD, NODE_HEALTH_DECOM, node_oms)


log = logging.getLogger("zen.HadoopParser")

DS_TO_RELATION = {
    'DataNodeMonitor': ('hadoop_data_nodes', 'HadoopDataNode'),
    'SecondaryNameNodeMonitor': ('hadoop_secondary_name_node', 'HadoopSecondaryNameNode'),
    'JobTrackerMonitor': ('hadoop_job_tracker', 'HadoopJobTracker')
}


class hadoop_parser(CommandParser):

    def processResults(self, cmd, result):
        """
        Parse the results of the hadoop datasource.
        """
        points_to_convert = {
            'heap_memory_capacity_bytes': ('HeapMemoryUsage', 'max'),
            'heap_memory_used_bytes': ('HeapMemoryUsage', 'used'),
            'non_heap_memory_capacity_bytes': ('NonHeapMemoryUsage', 'max'),
            'non_heap_memory_used_bytes': ('NonHeapMemoryUsage', 'used'),
            'dead_nodes_count': 'DeadNodes',
            'live_nodes_count': 'LiveNodes',
            'total_files': 'TotalFiles',
            'threads': 'Threads',
        }

        if cmd.result.exitCode != 0:
            msg = 'Error parsing collected data: {}'.format(
                getExitMessage(cmd.result.exitCode) if not
                'Unknown error code' in getExitMessage(cmd.result.exitCode)
                else 'No monitoring data received.'
            )
            add_event(result, cmd, msg)
            # Change the health state for components.
            self.apply_maps(cmd, state=NODE_HEALTH_DEAD)
            return result

        self.apply_maps(cmd, state=NODE_HEALTH_NORMAL)

        try:
            data = json.loads(cmd.result.output)
        except Exception, ex:
            msg = ('Error parsing collected data.'
                   '|||Error parsing collected data: {}'.format(ex))
            add_event(result, cmd, msg)
            return result

        for point in cmd.points:
            item = points_to_convert.get(point.id)\
                if points_to_convert.get(point.id) else point.id

            for value in data.get('beans'):
                if cmd.ds == "NameNodeMonitor":
                    if isinstance(item, tuple):
                        if value.get(item[0]) is not None:
                            result.values.append((
                                point, value[item[0]][item[1]]
                            ))
                    else:
                        if value.get(item) is not None:
                            if isinstance(value[item], unicode):
                                result.values.append((
                                    point, len(json.loads(value[item]))
                                ))
                            else:
                                result.values.append((point, value[item]))
                elif cmd.ds == "DataNodeMonitor":
                    if isinstance(item, tuple):
                        if value.get(item[0]) is not None:
                            result.values.append((
                                point, value[item[0]][item[1]]
                            ))
                    else:
                        if value.get(item) is not None:
                            result.values.append((point, value[item]))
                elif cmd.ds == "TaskTrackerMonitor":
                    if isinstance(item, tuple):
                        if value.get(item[0]) is not None:
                            result.values.append((
                                point, value[item[0]][item[1]]
                            ))
                    else:
                        if value.get(item) is not None:
                            result.values.append((point, value[item]))
                elif cmd.ds == 'JobTrackerMonitor':
                    if isinstance(item, tuple):
                        if value.get(item[0]) is not None:
                            result.values.append((
                                point, value[item[0]][item[1]]
                            ))
                    else:
                        if value.get('modelerType') == 'JobTrackerMetrics':
                            if value.get(item) is not None:
                                result.values.append((point, value[item]))
                elif cmd.ds == 'SecondaryNameNodeMonitor':
                    if isinstance(item, tuple):
                        if value.get(item[0]) is not None:
                            result.values.append((
                                point, value[item[0]][item[1]]
                            ))
                    else:
                        if value.get(item) is not None:
                            result.values.append((point, value[item]))
        msg = 'Successfully parsed collected data.'
        add_event(result, cmd, msg)
        log.debug((cmd.ds, '<<<---datasource', result.values, '<<<---result'))

        # Nodes remodeling.
        if cmd.ds == "NameNodeMonitor":
            maps = self.data_nodes_remodel(data)
            self.apply_maps(cmd, maps=maps)
        return result

    def data_nodes_remodel(self, data):
        nodes_oms = []
        for value in data.get('beans'):
            if value.get('name') == 'Hadoop:service=NameNode,name=NameNodeInfo':
                nodes_oms.extend(
                    node_oms(log, value.get('LiveNodes'), NODE_HEALTH_NORMAL))
                nodes_oms.extend(
                    node_oms(log, value.get('DeadNodes'), NODE_HEALTH_DEAD))
                nodes_oms.extend(
                    node_oms(log, value.get('DecomNodes'), NODE_HEALTH_DECOM))
        if nodes_oms:
            return [RelationshipMap(
                relname='hadoop_data_nodes',
                modname=MODULE_NAME['HadoopDataNode'],
                objmaps=nodes_oms)]

    def service_nodes_remodel(self, cmd, state):
        module = DS_TO_RELATION.get(cmd.ds)
        if module:
            return [ObjectMap({
                "compname": "{}/{}".format(module[0], cmd.component),
                "modname": module[1],
                'health_state': state
            })]

    def apply_maps(self, cmd, maps=None, state=None):
        if state:
            maps = self.service_nodes_remodel(cmd, state)
            # No need to apply maps for this datasource.
            if not maps:
                return

        collector = zope.component.queryUtility(ICollector)
        remoteProxy = collector.getRemoteConfigServiceProxy()
        dev_id = cmd.deviceConfig.configId
        changed = remoteProxy.callRemote('applyDataMaps', dev_id, maps)
        changed.addCallbacks(
            lambda mes: self.callback_success(cmd.component, mes),
            lambda err: self.callback_error(cmd.component, err)
        )

    def callback_success(self, comp, message):
        if message:
            log.debug('Changes applied to %s', comp)
        log.debug('No changes applied to %s', comp)

    def callback_error(self, comp, error):
        if isinstance(error, Failure):
            log.debug(error.value)


def add_event(result, cmd, msg):
    severity = ZenEventClasses.Error if 'Error' in msg \
        else ZenEventClasses.Clear
    msg = msg.split('|||')
    result.events.append(dict(
        severity=severity,
        summary=msg[0],
        message=msg[0] if len(msg) == 1 else msg[1],
        eventKey='hadoop_json_parse',
        eventClassKey='json_parse',
        eventClass='/Status',
        component=cmd.component,
    ))

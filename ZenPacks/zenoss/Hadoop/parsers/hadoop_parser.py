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
    'HBaseDiscoverMonitor': ('hadoop_data_nodes', 'HadoopDataNode'),
    'SecondaryNameNodeMonitor': ('hadoop_secondary_name_node',
                                 'HadoopSecondaryNameNode'),
    'JobTrackerMonitor': ('hadoop_job_tracker', 'HadoopJobTracker')
}

MSG_SUCCESS = 'Successfully parsed collected data.'


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
            'blocks_read': 'BlocksRead',
            'blocks_removed': 'BlocksRemoved',
            'blocks_written': 'BlocksWritten',
        }

        if cmd.result.exitCode != 0:
            msg = 'Error parsing collected data: {}'.format(
                getExitMessage(cmd.result.exitCode) if not
                'Unknown error code' in getExitMessage(cmd.result.exitCode)
                else 'No monitoring data received for {}.'.format(cmd.name)
            )
            add_event(result, cmd, msg)
            # Change the health state for components.
            self.apply_maps(cmd, state=NODE_HEALTH_DEAD)
            return result

        self.apply_maps(cmd, state=NODE_HEALTH_NORMAL)

        # HBase autodiscover
        if cmd.ds == "HBaseDiscoverMonitor":
            maps = self.hbase_autodiscover(cmd, result)
            self.apply_maps(cmd, maps=maps)
            add_event(result, cmd, MSG_SUCCESS)
            return result

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
                        elif value.get(point.id) is not None:
                            result.values.append((point, value[point.id]))
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
                elif cmd.ds == "NodeManagerMonitor":
                    if isinstance(item, tuple):
                        if value.get(item[0]) is not None:
                            result.values.append((
                                point, value[item[0]][item[1]]
                            ))
                    else:
                        if value.get(item) is not None:
                            result.values.append((point, value[item]))
                elif cmd.ds == "ResourceManagerMonitor":
                    if isinstance(item, tuple):
                        if value.get(item[0]) is not None:
                            result.values.append((
                                point, value[item[0]][item[1]]
                            ))
                    else:
                        if value.get(item) is not None:
                            result.values.append((point, value[item]))
                elif cmd.ds == "JobHistoryMonitor":
                    if isinstance(item, tuple):
                        if value.get(item[0]) is not None:
                            result.values.append((
                                point, value[item[0]][item[1]]
                            ))

        add_event(result, cmd, MSG_SUCCESS)
        log.debug((cmd.ds, '<<<---datasource', result.values, '<<<---result'))

        # Nodes remodeling.
        if cmd.ds == "NameNodeMonitor":
            dev_id = cmd.deviceConfig.id
            maps = self.data_nodes_remodel(data, dev_id)
            self.apply_maps(cmd, maps=maps)
        return result

    def data_nodes_remodel(self, data, dev_id):
        """
        Create RelationshipMap for data nodes remodeling.

        @param data: parsed result of command execution
        @type data: dict
        @return: list of RelationshipMap
        """
        nodes_oms = []
        for value in data.get('beans'):
            if value.get('name') == 'Hadoop:service=NameNode,name=NameNodeInfo':
                for key, val in (('LiveNodes', NODE_HEALTH_NORMAL),
                                 ('DeadNodes', NODE_HEALTH_DEAD),
                                 ('DecomNodes', NODE_HEALTH_DECOM)):
                    nodes_oms.extend(node_oms(log, dev_id, value.get(key), val, True))
        return [RelationshipMap(
                relname='hadoop_data_nodes',
                modname=MODULE_NAME['HadoopDataNode'],
                objmaps=nodes_oms)]

    def hbase_autodiscover(self, cmd, result):
        """
        Looks for presence of HBase status in command stdout
        """
        data = cmd.result.output
        module = DS_TO_RELATION.get(cmd.ds)

        # HBase has redirect to /master-status in HTML body
        if ("master-status" in data) and module:
            result.events.append(dict(
                severity=2,
                summary='HBase was discovered on %s data node' % cmd.component,
                message='HBase was discovered on %s data node' % cmd.component,
                eventKey='hadoop_hbase',
                eventClassKey='hadoop_hbase',
                eventClass='/Status',
                component=cmd.component,
            ))

            return [ObjectMap({
                "compname": None,
                "modname": module[1],
                "setHBaseAutodiscover": cmd.component
            })]
        return []

    def service_nodes_remodel(self, cmd, state):
        """
        Create ObjectMap for service nodes remodeling.

        @param cmd: cmd instance
        @type cmd: instance
        @param state: health state of the component (Normal or Dead)
        @type state: str
        @return: list of ObjectMap
        """
        module = DS_TO_RELATION.get(cmd.ds)
        if module:
            return [ObjectMap({
                "compname": "{}/{}".format(module[0], cmd.component),
                "modname": module[1],
                'health_state': state
            })]

    def apply_maps(self, cmd, maps=[], state=None):
        """
        Call remote CommandPerformanceConfig instance to apply maps.

        @param cmd: cmd instance
        @type cmd: instance
        @param maps: list of RelationshipMap|ObjectMap
        @type maps: list
        @param state: health state of the component (Normal or Dead)
        @type state: str
        @return: None
        """
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
        """Called on success."""
        if message:
            log.debug('Changes applied to %s', comp)
        log.debug('No changes applied to %s', comp)

    def callback_error(self, comp, error):
        """Called on error."""
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
        eventKey='Hadoop{}'.format(cmd.ds),
        eventClassKey='hadoop_data_parse',
        eventClass='/Status',
        component=cmd.component,
    ))

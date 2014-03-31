##############################################################################
#
# Copyright (C) Zenoss, Inc. 2014, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################


from Products.ZenRRD.CommandParser import CommandParser
from Products.ZenUtils.Utils import getExitMessage
from Products.ZenEvents import ZenEventClasses
import json
import logging

log = logging.getLogger("zen.HadoopParser")


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
        return result


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

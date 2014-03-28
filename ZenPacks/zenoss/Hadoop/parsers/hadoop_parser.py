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
        Parse the results of the hadoop service node command.
        """
        if cmd.result.exitCode != 0:
            result.events.append(dict(
                severity=ZenEventClasses.Error,
                summary='Parsing collected data: {}'.format(
                    getExitMessage(cmd.result.exitCode)
                ),
                eventKey='json_parse',
                eventClassKey='hadoop_json_parse',
                component=cmd.component,
            ))

            return result

        try:
            data = json.loads(cmd.result.output)
        except Exception, ex:
            result.events.append(dict(
                severity=ZenEventClasses.Error,
                summary='Error parsing collected data',
                message='Error parsing collected data: %s' % ex,
                eventKey='hadoop_json_parse',
                eventClassKey='json_parse_error',
                component=cmd.component,
            ))

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

        for point in cmd.points:
            item = points_to_convert.get(point.id)\
                if points_to_convert.get(point.id) else point.id

            for value in data['beans']:
                if cmd.ds == "NameNodeMonitor":
                    if isinstance(item, tuple):
                        if value.get(item[0]) is not None:
                            result.values.append((point, value[item[0]][item[1]]))
                    else:
                        if value.get(item) is not None:
                            if isinstance(value[item], unicode):
                                result.values.append((point, len(json.loads(value[item]))))
                            else:
                                result.values.append((point, value[item]))
                elif cmd.ds == "DataNodeMonitor":
                    if isinstance(item, tuple):
                        if value.get(item[0]) is not None:
                            result.values.append((point, value[item[0]][item[1]]))
                    else:
                        if value.get(item) is not None:
                            result.values.append((point, value[item]))
                elif cmd.ds == "TaskTrackerMonitor":
                    if isinstance(item, tuple):
                        if value.get(item[0]) is not None:
                            result.values.append((point, value[item[0]][item[1]]))
                    else:
                        if value.get(item) is not None:
                            result.values.append((point, value[item]))
                elif cmd.ds == 'JobTrackerMonitor':
                    if isinstance(item, tuple):
                        if value.get(item[0]) is not None:
                            result.values.append((point, value[item[0]][item[1]]))
                    else:
                        if value.get('modelerType') == 'JobTrackerMetrics':
                            if value.get(item) is not None:
                                result.values.append((point, value[item]))
                elif cmd.ds == 'SecondaryNameNodeMonitor':
                    if isinstance(item, tuple):
                        if value.get(item[0]) is not None:
                            result.values.append((point, value[item[0]][item[1]]))
                    else:
                        if value.get(item) is not None:
                            result.values.append((point, value[item]))

        log.debug((cmd.ds, '<<<---datasourse', result.values, '<<<----parser result values'))
        return result

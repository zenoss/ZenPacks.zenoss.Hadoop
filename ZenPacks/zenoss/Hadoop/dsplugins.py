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

from Products.ZenEvents import ZenEventClasses
from ZenPacks.zenoss.Hadoop import MODULE_NAME
from ZenPacks.zenoss.Hadoop.utils import (
    NODE_HEALTH_NORMAL, NODE_HEALTH_DEAD, NODE_HEALTH_DECOM, node_oms,
    hadoop_url, hadoop_headers, HadoopException, check_error)


from twisted.web.client import getPage
from Products.DataCollector.plugins.DataMaps import RelationshipMap, ObjectMap
from twisted.python.failure import Failure
from twisted.internet import defer
from ZenPacks.zenoss.PythonCollector.datasources.PythonDataSource \
    import PythonDataSourcePlugin


log = logging.getLogger("zen.HadoopParser")

DS_TO_RELATION = {
    'DataNodeMonitor': ('hadoop_data_nodes', 'HadoopDataNode'),
    'HBaseDiscoverMonitor': ('hadoop_data_nodes', 'HadoopDataNode'),
    'SecondaryNameNodeMonitor': ('hadoop_secondary_name_node',
                                 'HadoopSecondaryNameNode'),
    'JobTrackerMonitor': ('hadoop_job_tracker', 'HadoopJobTracker'),
    'TaskTrackerMonitor': ('hadoop_task_tracker', 'HadoopTaskTracker'),
    'NodeManagerMonitor': ('hadoop_node_manager', 'HadoopNodeManager'),
    'ResourceManagerMonitor': ('hadoop_resource_manager',
                               'HadoopResourceManager'),
    'JobHistoryMonitor': ('hadoop_job_history', 'HadoopJobHistory'),
}


class HadoopPlugin(PythonDataSourcePlugin):

    proxy_attributes = (
        'zHadoopScheme',
        'zHadoopUsername',
        'zHadoopPassword',
        'zHadoopNameNodePort',
        'zHbaseAutodiscover',
        'zHBaseMasterPort',
        'title',
    )

    # A variable to store component ids of added components.
    component = None

    @defer.inlineCallbacks
    def collect(self, config):
        """
        This method return a Twisted deferred. The deferred results will
        be sent to the onResult then either onSuccess or onError callbacks
        below.
        """

        results = self.new_data()
        for ds in config.datasources:
            self.component = ds.component
            try:
                ip, port = ds.title.split(':')
            except ValueError:
                # Exception when runing NameNodeMonitor on Device
                ip = ds.manageIp
                port = ds.zHadoopNameNodePort

            if ds.datasource == 'NameNodeMonitor':
                conf_url = hadoop_url(
                    scheme=ds.zHadoopScheme,
                    port=ds.zHadoopNameNodePort,
                    host=ds.manageIp,
                    endpoint='/conf'
                )

            jmx_url = hadoop_url(
                scheme=ds.zHadoopScheme,
                port=port,
                host=ip,
                endpoint='/jmx'
            )
            headers = hadoop_headers(
                accept='application/json',
                username=ds.zHadoopUsername,
                passwd=ds.zHadoopPassword
            )

            res = {}
            try:
                if ds.datasource == 'NameNodeMonitor':
                    res['conf'] = yield getPage(conf_url, headers=headers)
                res['jmx'] = yield getPage(jmx_url, headers=headers)
            except Exception as e:
                # Add event if can't connect to some node
                e = check_error(e, ds.device) or e
                severity = ZenEventClasses.Error
                summary = str(e)
                results['maps'].extend(self.add_maps(
                    res, ds, state=NODE_HEALTH_DEAD)
                )

                # if isinstance(e, SSLError):
                #     summary = 'Connection lost for {}. HTTPS was not configured'.format(
                #         ds.device
                #     )

            if res.get('jmx'):
                severity = ZenEventClasses.Clear
                summary = 'Monitoring ok'
                results['values'][self.component] = self.form_values(
                    res['jmx'], ds
                )
                results['maps'].extend(self.add_maps(
                    res, ds, state=NODE_HEALTH_NORMAL)
                )

            results['events'].append({
                'component': self.component,
                'summary': summary,
                'eventKey': ds.datasource,
                'eventClass': '/Status',
                'severity': severity,
            })

        defer.returnValue(results)

    def onError(self, result, config):
        """Called only on error. After onResult, before onComplete."""
        data = self.new_data()
        ds = config.datasources[0]
        if isinstance(result, Failure):
            result = result.value
        data['events'].append({
            'component': self.component,
            'summary': str(result),
            'eventKey': ds.datasource,
            'eventClass': '/Status',
            'severity': ZenEventClasses.Warning,
        })
        return data

    def add_maps(self, result, ds, state):

        """
        Create Object/Relationship map for component remodeling.

        @param result: the data returned from getPage call
        @type result: str
        @param datasource: device datasourse
        @type datasource: instance of PythonDataSourceConfig
        @return: ObjectMap|RelationshipMap
        """
        if ds.datasource == 'NameNodeMonitor' \
                and result.get('jmx') and result.get('conf'):
            return self.data_nodes_remodel(result, ds)
        else:
            return self.service_nodes_remodel(ds, state)

    def service_nodes_remodel(self, ds, state):
        """
         Create ObjectMap for service nodes remodeling.

        @param cmd: cmd instance
        @type cmd: instance
        @param state: health state of the component (Normal or Dead)
        @type state: str
        @return: list of ObjectMap
        """
        module = DS_TO_RELATION.get(ds.datasource)
        if module:
            return [ObjectMap({
                "compname": "{}/{}".format(module[0], ds.component),
                "modname": module[1],
                'health_state': state
            })]
        return []

    def data_nodes_remodel(self, data, device):
        """
        Create RelationshipMap for data nodes remodeling.

        @param data: parsed result of command execution
        @type data: dict
        @param device: object which has config_key and manageIP attributes
        @type device: object
        @return: list of RelationshipMap
        """
        nodes_oms = []
        try:
            values = json.loads(data['jmx'])
        except Exception:
            raise HadoopException('Error parsing collected data for {} '
                                  'monitoring template'.format(device.template))
        for value in values.get('beans'):
            if value.get('name') == 'Hadoop:service=NameNode,name=NameNodeInfo':
                for key, val in (('LiveNodes', NODE_HEALTH_NORMAL),
                                 ('DeadNodes', NODE_HEALTH_DEAD),
                                 ('DecomNodes', NODE_HEALTH_DECOM)):
                    nodes_oms.extend(
                        node_oms(
                            log, device, value.get(key), val, data['conf'], True
                        )
                    )
        rm = RelationshipMap(
            relname='hadoop_data_nodes',
            modname=MODULE_NAME['HadoopDataNode'],
            objmaps=nodes_oms)
        if list(rm):
            return [rm]
        return []

    def form_values(self, result, ds):
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

        try:
            data = json.loads(result)
        except Exception:
            raise HadoopException('Error parsing collected data for {} '
                                  'monitoring template'.format(ds.template))

        result = {}
        for point in ds.points:
            item = points_to_convert.get(point.id)\
                if points_to_convert.get(point.id) else point.id

            for value in data.get('beans'):
                if isinstance(item, tuple):
                    if value.get(item[0]) is not None:
                        result[point.id] = (value[item[0]][item[1]], 'N')
                else:
                    if value.get(item) is not None:
                        result[point.id] = (value[item], 'N')
                if ds.datasource == "NameNodeMonitor":
                    if value.get(item) is not None:
                        if isinstance(value[item], unicode):
                            result[point.id] = (
                                len(json.loads(value[item])), 'N'
                            )
                        elif value.get("name") == "Hadoop:service=NameNode,name=NameNodeInfo":
                            if value.get(point.id) is not None:
                                result[point.id] = (value[point.id], 'N')
                elif ds.datasource == "DataNodeMonitor":
                    if value.get(point.id) is not None:
                        result[point.id] = (value[point.id], 'N')
                elif ds.datasource == 'JobTrackerMonitor':
                    if value.get('modelerType') == 'JobTrackerMetrics':
                        if value.get(item) is not None:
                            result[point.id] = (value[item], 'N')
        return result


class HadoopHBasePlugin(HadoopPlugin):
    '''
    Looks for presence of HBase on Hadoop
    '''

    @defer.inlineCallbacks
    def collect(self, config):
        """
        This method return a Twisted deferred. The deferred results will
        be sent to the onResult then either onSuccess or onError callbacks
        below.
        """

        results = self.new_data()
        for ds in config.datasources:
            ip = ds.title.split(':')[0]
            url = hadoop_url(
                scheme=ds.zHadoopScheme,
                port=ds.zHBaseMasterPort,
                host=ip,
                endpoint='/master-status'
            )
            headers = hadoop_headers(
                accept='application/json',
                username=ds.zHadoopUsername,
                passwd=ds.zHadoopPassword
            )
            try:
                # Check if HBase into Hadoop Data Node
                check = yield getPage(url, headers=headers)
            except Exception:
                continue
            module = DS_TO_RELATION.get('DataNodeMonitor')
            if ds.zHbaseAutodiscover and module:
                results['maps'].append(ObjectMap({
                    "compname": "{}/{}".format(module[0], ds.component),
                    "modname": module[1],
                    "setHBaseAutodiscover": ip
                }))
                results['events'].append({
                    'component': ds.component,
                    'summary': 'HBase was discovered on %s data node' % ds.title,
                    'eventKey': ds.datasource,
                    'eventClass': '/Status',
                    'severity': ZenEventClasses.Info,
                })
        defer.returnValue(results)

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
from OpenSSL.SSL import Error as SSLError
import zope.component

from Products.DataCollector.plugins.DataMaps import ObjectMap, RelationshipMap
from Products.ZenCollector.interfaces import IEventService
from Products.DataCollector.plugins.CollectorPlugin import PythonPlugin
from twisted.web.client import getPage
from twisted.internet import defer
from ZenPacks.zenoss.Hadoop import MODULE_NAME
from ZenPacks.zenoss.Hadoop.utils import NODE_HEALTH_NORMAL, \
    NODE_HEALTH_DEAD, NODE_HEALTH_DECOM, node_oms, hadoop_url, hadoop_headers


class HadoopDataNode(PythonPlugin):
    """
    PythonCollector plugin for modelling Hadoop data nodes
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

        result = {}
        try:
            result['jmx'] = yield getPage(jmx_url, headers=headers)
            result['conf'] = yield getPage(conf_url, headers=headers)
        except Exception, e:
            self.on_error(log, device, e)
        self.on_success(log, device)
        defer.returnValue(result)

    def process(self, device, results, log):

        log.info('Collecting Hadoop nodes for device %s' % device.id)

        maps = collections.OrderedDict([
            ('hadoop_data_nodes', []),
        ])

        try:
            data = json.loads(results['jmx'])
        except (TypeError, KeyError, ValueError):
            log.error('Modeler %s failed to parse the result.' % self.name())
            return

        nodes_oms = []
        for bean in data['beans']:
            if bean['name'] == 'Hadoop:service=NameNode,name=NameNodeInfo':
                log.debug('Collecting live nodes')
                nodes_oms.extend(
                    node_oms(
                        log, device, bean["LiveNodes"],
                        NODE_HEALTH_NORMAL, results['conf']))
                log.debug('Collecting dead nodes')
                nodes_oms.extend(
                    node_oms(
                        log, device, bean["DeadNodes"],
                        NODE_HEALTH_DEAD, results['conf']))
                log.debug('Collecting decommissioned nodes')
                nodes_oms.extend(
                    node_oms(
                        log, device, bean["DecomNodes"],
                        NODE_HEALTH_DECOM, results['conf']))

        maps['hadoop_data_nodes'].append(RelationshipMap(
            relname='hadoop_data_nodes',
            modname=MODULE_NAME['HadoopDataNode'],
            objmaps=nodes_oms))

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
        if isinstance(e, SSLError):
            e = SSLError(
                'Connection lost for {}. HTTPS was not configured'.format(
                    device.id
                ))
        log.error(e)
        self._send_event(str(e).capitalize(), device.id, 5)
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
                eventKey='ConnectionError',
                severity=severity,
            ))

            return True
        else:
            if force or (severity > 0):
                self.device_om = ObjectMap({
                    'setErrorNotification': reason
                })

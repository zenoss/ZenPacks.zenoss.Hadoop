##############################################################################
#
# Copyright (C) Zenoss, Inc. 2014, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################
"""
Custom ZenPack initialization code. All code defined in this module will be
executed at startup time in all Zope clients.
"""
import logging
log = logging.getLogger('zen.Hadoop')

import Globals

from zope.event import notify

from Products.ZenModel.Device import Device
from Products.ZenModel.ZenPack import ZenPack as ZenPackBase
from Products.ZenRelations.RelSchema import ToManyCont, ToOne
from Products.ZenRelations.zPropertyCategory import setzPropertyCategory
from Products.ZenUtils.Utils import unused, monkeypatch
from Products.ZenUtils.IpUtil import getHostByName
from Products.Zuul.interfaces import ICatalogTool
from Products.Zuul.catalog.events import IndexingEvent

unused(Globals)


# Categorize zProperties.
# setzPropertyCategory('zHadoop', 'Hadoop')
setzPropertyCategory('zHbaseAutodiscover', 'Hadoop')
setzPropertyCategory('zHbaseDeviceClass', 'Hadoop')
setzPropertyCategory('zHadoopScheme', 'Hadoop')
setzPropertyCategory('zHadoopUsername', 'Hadoop')
setzPropertyCategory('zHadoopPassword', 'Hadoop')
setzPropertyCategory('zHadoopNameNodePort', 'Hadoop')


# Modules containing model classes. Used by zenchkschema to validate
# bidirectional integrity of defined relationships.
productNames = (
    'HadoopJobTracker',
    'HadoopTaskTracker',
    'HadoopSecondaryNameNode',
    'HadoopResourceManager',
    'HadoopNodeManager',
    'HadoopJobHistory',
    'HadoopDataNode'
)

# Useful to avoid making literal string references to module and class names
# throughout the rest of the ZenPack.
ZP_NAME = 'ZenPacks.zenoss.Hadoop'
MODULE_NAME = {}
CLASS_NAME = {}
for product_name in productNames:
    MODULE_NAME[product_name] = '.'.join([ZP_NAME, product_name])
    CLASS_NAME[product_name] = '.'.join([ZP_NAME, product_name, product_name])

# Define new device relations.
NEW_DEVICE_RELATIONS = (
    ('hadoop_job_tracker', 'HadoopJobTracker'),
    ('hadoop_task_tracker', 'HadoopTaskTracker'),
    ('hadoop_secondary_name_node', 'HadoopSecondaryNameNode'),
    ('hadoop_resource_manager', 'HadoopResourceManager'),
    ('hadoop_node_manager', 'HadoopNodeManager'),
    ('hadoop_job_history', 'HadoopJobHistory'),
    ('hadoop_data_nodes', 'HadoopDataNode'),
)

NEW_COMPONENT_TYPES = (
    'ZenPacks.zenoss.Hadoop.HadoopJobTracker.HadoopJobTracker',
    'ZenPacks.zenoss.Hadoop.HadoopTaskTracker.HadoopTaskTracker',
    'ZenPacks.zenoss.Hadoop.HadoopSecondaryNameNode.HadoopSecondaryNameNode',
    'ZenPacks.zenoss.Hadoop.HadoopResourceManager.HadoopResourceManager',
    'ZenPacks.zenoss.Hadoop.HadoopNodeManager.HadoopNodeManager',
    'ZenPacks.zenoss.Hadoop.HadoopJobHistory.HadoopJobHistory',
    'ZenPacks.zenoss.Hadoop.HadoopDataNode.HadoopDataNode',
)

# Add new relationships to Device if they don't already exist.
for relname, modname in NEW_DEVICE_RELATIONS:
    if relname not in (x[0] for x in Device._relations):
        Device._relations += (
            (relname,
             ToManyCont(ToOne, '.'.join((ZP_NAME, modname)), 'hadoop_host')),
        )


# Add ErrorNotification to Device
def setErrorNotification(self, msg):
    if msg == 'clear':
        self.dmd.ZenEventManager.sendEvent(dict(
            device=self.id,
            summary=msg,
            eventClass='/Status',
            eventKey='ConnectionError',
            severity=0,
        ))
    else:
        self.dmd.ZenEventManager.sendEvent(dict(
            device=self.id,
            summary=msg,
            eventClass='/Status',
            eventKey='ConnectionError',
            severity=5,
        ))

    return


def getErrorNotification(self):
    return


def setHBaseAutodiscover(self, node_name):
    """
    One of HadoopDataNode can be occupied by HBase.
    """
    hbase_device = None
    old_hbase_device = None
    dc = self.dmd.getOrganizer(self.zHbaseDeviceClass)

    # a) Check if HBase changed it's node
    for node in self.hadoop_data_nodes():
        if node.hbase_device_id == node_name:
            # Nothing changed
            return

    # b) Lookup for old HBase node
    for node in self.hadoop_data_nodes():
        if node.hbase_device_id:
            old_hbase_device = self.findDeviceByIdExact(node.hbase_device_id)
            node.hbase_device_id = None
            node.index_object()
            break

    if old_hbase_device:
        # Changing IP to node_name
        hbase_device = old_hbase_device
        hbase_device.setManageIp(node_name)

        hbase_device.index_object()
        notify(IndexingEvent(hbase_device))
    else:
        ip = self._sanitizeIPaddress(node_name)
        if not ip:
            ip = getHostByName(node_name)

        if not ip:
            log.warn("Cann't resolve %s into IP address" % node_name)
            return

        hbase_device = self.findDevice(ip)

        if hbase_device:
            log.info("HBase device found in existing devices")
        else:
            # Check if HBase ZenPack is installed
            try:
                self.zHBasePassword
            except AttributeError:
                log.warn("HBase ZenPack is requaried")
                return

            log.info("HBase device created")
            hbase_device = dc.createInstance(ip)
            hbase_device.title = node_name
            hbase_device.setManageIp(ip)
            hbase_device.setPerformanceMonitor(self.getPerformanceServer().id)
            hbase_device.index_object()
            hbase_device.zCollectorPlugins = list(
                hbase_device.zCollectorPlugins
            ).extend(
                ['HBaseCollector', 'HBaseTableCollector']
            )
            hbase_device.zHBasePassword = self.zHBasePassword
            hbase_device.zHBaseUsername = self.zHBaseUsername
            hbase_device.zHBasePort = self.zHBasePort

        hbase_device.index_object()
        notify(IndexingEvent(hbase_device))

        # Schedule a modeling job for the new device.
        # hbase_device.collectDevice(setlog=False, background=True)

    # Setting HBase device ID as node property for back link from UI
    for node in self.hadoop_data_nodes():
        if str(node.title).split(':')[0] == node_name:
            node.hbase_device_id = hbase_device.id
            node.index_object()

    return


def getHBaseAutodiscover(self):
    return True


@monkeypatch('Products.ZenModel.Device.Device')
def getRRDTemplates(self):
    """
    Returns all the templates bound to this Device and
    add Hadoop monitoring template if HadoopDataNode or
    HadoopServiceNode collectors are used.
    """

    result = original(self)
    # Check if 'Hadoop' monitoring template is bound to device and bind if not
    if filter(lambda x: 'Hadoop' in x.id, result):
        return result

    collectors = self.getProperty('zCollectorPlugins')
    if 'HadoopDataNode' in collectors or 'HadoopServiceNode' in collectors:
        self.bindTemplates([x.id for x in result] + ['Hadoop'])
        result = original(self)
    return result


Device.setErrorNotification = setErrorNotification
Device.getErrorNotification = getErrorNotification
Device.setHBaseAutodiscover = setHBaseAutodiscover
Device.getHBaseAutodiscover = getHBaseAutodiscover


class ZenPack(ZenPackBase):
    """
    ZenPack loader that handles custom installation and removal tasks.
    """

    packZProperties = [
        ('zHbaseAutodiscover', False, 'bool'),
        ('zHbaseDeviceClass', '/Server/Linux', 'string'),
        ('zHadoopScheme', 'http', 'string'),
        ('zHadoopUsername', '', 'string'),
        ('zHadoopPassword', '', 'string'),
        ('zHadoopNameNodePort', '50070', 'string'),
    ]

    def install(self, app):
        super(ZenPack, self).install(app)

        log.info('Adding Hadoop relationships to existing devices')
        self._buildDeviceRelations()

    def remove(self, app, leaveObjects=False):
        if not leaveObjects:
            log.info('Removing Hadoop components')
            cat = ICatalogTool(app.zport.dmd)
            for brain in cat.search(types=NEW_COMPONENT_TYPES):
                component = brain.getObject()
                component.getPrimaryParent()._delObject(component.id)

            # Remove our Device relations additions.
            Device._relations = tuple(
                [x for x in Device._relations
                    if x[0] not in NEW_DEVICE_RELATIONS])

            log.info('Removing Hadoop device relationships')
            self._buildDeviceRelations()

        super(ZenPack, self).remove(app, leaveObjects=leaveObjects)

    def _buildDeviceRelations(self):
        for d in self.dmd.Devices.getSubDevicesGen():
            d.buildRelations()

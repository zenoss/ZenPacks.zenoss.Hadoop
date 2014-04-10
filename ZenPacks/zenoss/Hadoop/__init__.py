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
import math
import logging
log = logging.getLogger('zen.Hadoop')

import Globals

from zope.event import notify

from Products.ZenEvents.EventManagerBase import EventManagerBase
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


# Modules containing model classes. Used by zenchkschema to validate
# bidirectional integrity of defined relationships.
productNames = (
    'HadoopJobTracker',
    'HadoopSecondaryNameNode',
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
    ('hadoop_secondary_name_node', 'HadoopSecondaryNameNode'),
    ('hadoop_data_nodes', 'HadoopDataNode'),
    )

NEW_COMPONENT_TYPES = (
    'ZenPacks.zenoss.Hadoop.HadoopJobTracker.HadoopJobTracker',
    'ZenPacks.zenoss.Hadoop.HadoopSecondaryNameNode.HadoopSecondaryNameNode',
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
            # print "Nothing changed"
            return

    # b) Lookup for old HBase node
    for node in self.hadoop_data_nodes():
        if node.hbase_device_id:
            old_hbase_device = self.findDeviceByIdExact(node.hbase_device_id)
            node.hbase_device_id = None
            node.index_object()
            break

    if old_hbase_device:
        # print "Old HBase device exists ", old_hbase_device
        # print "Changing IP to", node_name
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
        # print ip
        # print hbase_device
        if hbase_device:
            log.info("HBase device found in existing devices")
        else:
            # print "Created"
            log.info("HBase device created")
            hbase_device = dc.createInstance(ip)
            hbase_device.title = node_name
            hbase_device.setManageIp(ip)
            # hbase_device.setProdState(self._running_prodstate)
            hbase_device.setPerformanceMonitor(self.getPerformanceServer().id)

        hbase_device.index_object()
        notify(IndexingEvent(hbase_device))

        # Schedule a modeling job for the new device.
        # hbase_device.collectDevice(setlog=False, background=True)

        # self.dmd.ZenEventManager.sendEvent(dict(
        #     device=self.id,
        #     summary='HBase was discovered on %s' % node_name,
        #     eventClass='/Status',
        #     eventKey='HBaseAutodiscover',
        #     severity=2,
        #     ))

    # Setting HBase device ID as node property for back link from UI
    for node in self.hadoop_data_nodes():
        if node.title == node_name:
            node.hbase_device_id = hbase_device.id
            node.index_object()

    return


def getHBaseAutodiscover(self):
    return True


Device.setErrorNotification = setErrorNotification
Device.getErrorNotification = getErrorNotification
Device.setHBaseAutodiscover = setHBaseAutodiscover
Device.getHBaseAutodiscover = getHBaseAutodiscover


# @monkeypatch('Products.ZenCollector.services.config.CollectorConfigService')
@monkeypatch('Products.ZenHub.services.CommandPerformanceConfig.CommandPerformanceConfig')
def remote_applyDataMaps(self, device, datamaps):
    from Products.DataCollector.ApplyDataMap import ApplyDataMap
    device = self.getPerformanceMonitor().findDevice(device)
    applicator = ApplyDataMap(self)

    changed = False
    for datamap in datamaps:
        if applicator._applyDataMap(device, datamap):
            changed = True

    return changed


class ZenPack(ZenPackBase):
    """
    ZenPack loader that handles custom installation and removal tasks.
    """

    packZProperties = [
        # ('zHadoop', False, 'bool'),
        ('zHbaseAutodiscover', False, 'bool'),
        ('zHbaseDeviceClass', '/Server/Linux', 'string'),
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

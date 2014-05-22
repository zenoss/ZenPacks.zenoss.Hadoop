######################################################################
#
# Copyright (C) Zenoss, Inc. 2014, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is
# installed.
#
######################################################################
from zope.component import adapts
from zope.interface import implements

from Products.ZenModel.ManagedEntity import ManagedEntity
from Products.ZenModel.ZenossSecurity import ZEN_CHANGE_DEVICE
from Products.ZenRelations.RelSchema import ToManyCont, ToOne

from Products.Zuul.decorators import info
from Products.Zuul.form import schema
from Products.Zuul.infos import ProxyProperty
from Products.Zuul.infos.component import ComponentInfo
from Products.Zuul.interfaces.component import IComponentInfo
from Products.Zuul.utils import ZuulMessageFactory as _t

from .HadoopComponent import HadoopComponent


class HadoopDataNode(HadoopComponent):
    meta_type = portal_type = "HadoopDataNode"

    health_state = None
    last_contacted = None
    hbase_device_id = None

    _properties = HadoopComponent._properties + (
        {'id': 'health_state', 'type': 'string'},
        {'id': 'last_contacted', 'type': 'string'},
        {'id': 'hbase_device_id', 'type': 'string'},
    )

    _relations = HadoopComponent._relations + (
        ('hadoop_host', ToOne(
            ToManyCont, 'Products.ZenModel.Device.Device', 'hadoop_data_nodes')),
    )

    def device(self):
        return self.hadoop_host()


class IHadoopDataNodeInfo(IComponentInfo):
    '''
    API Info interface for HadoopDataNode.
    '''
    device = schema.Entity(title=_t(u'Device'))
    health_state = schema.TextLine(title=_t(u'Health State'))
    last_contacted = schema.TextLine(title=_t(u'Last Contacted'))
    hbase_device = schema.TextLine(title=_t(u'HBase Device'))


class HadoopDataNodeInfo(ComponentInfo):
    '''
    API Info adapter factory for HadoopDataNode.
    '''
    implements(IHadoopDataNodeInfo)
    adapts(HadoopDataNode)

    health_state = ProxyProperty('health_state')
    last_contacted = ProxyProperty('last_contacted')

    @property
    @info
    def hbase_device(self):
        dev_id = self._object.hbase_device_id
        if dev_id:
            obj = self._object.findDeviceByIdExact(dev_id)
            if obj:
                return '<a href="{}">{}</a>'.format(
                            obj.getPrimaryUrlPath(),
                            obj.titleOrId()
                        )
        return ''

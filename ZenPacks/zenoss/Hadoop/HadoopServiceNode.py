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

from Products.Zuul.form import schema
from Products.Zuul.infos import ProxyProperty
from Products.Zuul.infos.component import ComponentInfo
from Products.Zuul.interfaces.component import IComponentInfo
from Products.Zuul.utils import ZuulMessageFactory as _t

from .HadoopComponent import HadoopComponent


class HadoopServiceNode(HadoopComponent):
    meta_type = portal_type = "HadoopServiceNode"

    node_type = None
    health_state = None

    _properties = HadoopComponent._properties + (
        {'id': 'node_type', 'type': 'string'},
        {'id': 'health_state', 'type': 'string'},
    )

    def device(self):
        return self.hadoop_host()


class IHadoopServiceNodeInfo(IComponentInfo):
    '''
    API Info interface for HadoopServiceNode.
    '''
    device = schema.Entity(title=_t(u'Device'))
    node_type = schema.TextLine(title=_t(u'Node Type'))
    health_state = schema.TextLine(title=_t(u'Health State'))


class HadoopServiceNodeInfo(ComponentInfo):
    '''
    API Info adapter factory for HadoopServiceNode.
    '''
    implements(IHadoopServiceNodeInfo)
    adapts(HadoopServiceNode)

    node_type = ProxyProperty('node_type')
    health_state = ProxyProperty('health_state')

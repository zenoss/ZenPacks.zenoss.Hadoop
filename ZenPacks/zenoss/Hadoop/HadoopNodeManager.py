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

from Products.ZenRelations.RelSchema import ToManyCont, ToOne

from .HadoopServiceNode import HadoopServiceNode, IHadoopServiceNodeInfo, \
    HadoopServiceNodeInfo


class HadoopNodeManager(HadoopServiceNode):
    meta_type = portal_type = "HadoopNodeManager"

    _relations = HadoopServiceNode._relations + (
        ('hadoop_host', ToOne(
            ToManyCont,
            'Products.ZenModel.Device.Device', 'hadoop_node_manager')),
    )


class IHadoopNodeManagerInfo(IHadoopServiceNodeInfo):
    '''
    API Info interface for HadoopNodeManager.
    '''


class HadoopNodeManagerInfo(HadoopServiceNodeInfo):
    '''
    API Info adapter factory for HadoopNodeManager.
    '''
    implements(IHadoopNodeManagerInfo)
    adapts(HadoopNodeManager)

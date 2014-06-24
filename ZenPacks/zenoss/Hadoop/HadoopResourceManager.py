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


class HadoopResourceManager(HadoopServiceNode):
    meta_type = portal_type = "HadoopResourceManager"

    _relations = HadoopServiceNode._relations + (
        ('hadoop_host', ToOne(
            ToManyCont,
            'Products.ZenModel.Device.Device', 'hadoop_resource_manager')),
    )


class IHadoopResourceManagerInfo(IHadoopServiceNodeInfo):
    '''
    API Info interface for HadoopResourceManager.
    '''


class HadoopResourceManagerInfo(HadoopServiceNodeInfo):
    '''
    API Info adapter factory for HadoopResourceManager.
    '''
    implements(IHadoopResourceManagerInfo)
    adapts(HadoopResourceManager)

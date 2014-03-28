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


class HadoopSecondaryNameNode(HadoopServiceNode):
    _relations = HadoopServiceNode._relations + (
        ('hadoop_host', ToOne(
            ToManyCont, 'Products.ZenModel.Device.Device', 'hadoop_secondary_name_node')),
    )


class IHadoopSecondaryNameNodeInfo(IHadoopServiceNodeInfo):
    '''
    API Info interface for HadoopSecondaryNameNode.
    '''


class HadoopSecondaryNameNodeInfo(HadoopServiceNodeInfo):
    '''
    API Info adapter factory for HadoopSecondaryNameNode.
    '''
    implements(IHadoopSecondaryNameNodeInfo)
    adapts(HadoopSecondaryNameNode)

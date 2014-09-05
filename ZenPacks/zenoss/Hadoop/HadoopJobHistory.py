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


class HadoopJobHistory(HadoopServiceNode):
    meta_type = portal_type = "HadoopJobHistory"

    _relations = HadoopServiceNode._relations + (
        ('hadoop_host', ToOne(
            ToManyCont,
            'Products.ZenModel.Device.Device', 'hadoop_job_history')),
    )


class IHadoopJobHistoryInfo(IHadoopServiceNodeInfo):
    '''
    API Info interface for HadoopJobHistory.
    '''


class HadoopJobHistoryInfo(HadoopServiceNodeInfo):
    '''
    API Info adapter factory for HadoopJobHistory.
    '''
    implements(IHadoopJobHistoryInfo)
    adapts(HadoopJobHistory)

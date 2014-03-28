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


class HadoopJobTracker(HadoopServiceNode):
    _relations = HadoopServiceNode._relations + (
        ('hadoop_host', ToOne(
            ToManyCont, 'Products.ZenModel.Device.Device', 'hadoop_job_tracker')),
    )


class IHadoopJobTrackerInfo(IHadoopServiceNodeInfo):
    '''
    API Info interface for HadoopJobTracker.
    '''


class HadoopJobTrackerInfo(HadoopServiceNodeInfo):
    '''
    API Info adapter factory for HadoopJobTracker.
    '''
    implements(IHadoopJobTrackerInfo)
    adapts(HadoopJobTracker)

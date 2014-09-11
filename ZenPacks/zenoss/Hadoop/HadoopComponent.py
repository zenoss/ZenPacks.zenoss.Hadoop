##############################################################################
#
# Copyright (C) Zenoss, Inc. 2014, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

from Products.ZenModel.Device import Device
from Products.ZenModel.DeviceComponent import DeviceComponent
from Products.ZenModel.ManagedEntity import ManagedEntity
from Products.ZenModel.ZenossSecurity import ZEN_CHANGE_DEVICE


class HadoopComponent(DeviceComponent, ManagedEntity):
    ''' Base class for all components in this ZenPack.  '''

    # Explicit inheritence.
    _properties = ManagedEntity._properties
    _relations = ManagedEntity._relations

    # Meta-data: Zope object views and actions
    factory_type_information = ({
        'actions': ({
            'id': 'perfConf',
            'name': 'Template',
            'action': 'objTemplates',
            'permissions': (ZEN_CHANGE_DEVICE,),
        },),
    },)

    # remodel component if ZooKeeper added or removed
    remodel = None

    def check_zookeeper(self):
        '''
        Check if ZooKeeper component is on device
        '''
        if hasattr(self.hadoop_host(), 'zookeepers') and \
                getattr(self.hadoop_host(), 'zookeepers')():
            return True

    def getStatus(self):
        return super(HadoopComponent, self).getStatus("/Status")

    def getIconPath(self):
        '''
        Return the path to an icon for this component.
        '''
        return '/++resource++ZenPacks_zenoss_Hadoop/img/%s.png' \
               % self.meta_type

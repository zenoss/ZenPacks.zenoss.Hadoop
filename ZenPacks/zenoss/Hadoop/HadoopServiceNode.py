######################################################################
#
# Copyright (C) Zenoss, Inc. 2014, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is
# installed.
#
######################################################################

from Products.ZenModel.ManagedEntity import ManagedEntity
from Products.ZenModel.ZenossSecurity import ZEN_CHANGE_DEVICE
from Products.ZenRelations.RelSchema import ToManyCont, ToOne

from Products.Zuul.infos.component import ComponentInfo
from Products.Zuul.interfaces.component import IComponentInfo
from Products.Zuul.catalog.paths import DefaultPathReporter

from .HadoopComponent import HadoopComponent


class HadoopServiceNode(HadoopComponent):
    meta_type = portal_type = "HadoopServiceNode"

    attributeOne = None
    attributeTwo = None
	
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

    # Custom components must always implement the device method. The method
    # should return the device object that contains the component.
    def device(self):
        return self.exampleDevice()


class IHadoopServiceNodeInfo(IComponentInfo):
    '''
    API Info interface for HadoopServiceNode.
    '''


class HadoopServiceNodeInfo(ComponentInfo):
    '''
    API Info adapter factory for HadoopServiceNode.
    '''


class HadoopServiceNodePathReporter(DefaultPathReporter):
    ''' Path reporter for HadoopServiceNode.  '''

    def getPaths(self):
        return super(HadoopServiceNodePathReporter, self).getPaths()

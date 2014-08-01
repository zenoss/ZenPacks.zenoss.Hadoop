##############################################################################
#
# Copyright (C) Zenoss, Inc. 2014, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

import logging
import json
import xml.etree.cElementTree as ET

from mock import Mock

from Products.DataCollector.ApplyDataMap import ApplyDataMap
from Products.ZenTestCase.BaseTestCase import BaseTestCase

from ZenPacks.zenoss.Hadoop.modeler.plugins.HadoopDataNode \
    import HadoopDataNode
from ZenPacks.zenoss.Hadoop.modeler.plugins.HadoopServiceNode \
    import HadoopServiceNode
from ZenPacks.zenoss.Hadoop.tests.utils import test_device, load_data
from ZenPacks.zenoss.Hadoop.utils import node_oms, NAME_SPLITTER, \
    get_attr, prep_ip

log = logging.getLogger('zen.HadoopTest')


class MockJar(object):
    """Mock object for x._p_jar.

    Used to trick ApplyDataMap into not aborting transactions after adding
    non-persistent objects. Without doing this, all sub-components will
    cause ugly tracebacks in modeling tests.

    """
    def sync(self):
        pass


class HadoopComponentsTestCase(BaseTestCase):

    def afterSetUp(self):
        super(HadoopComponentsTestCase, self).afterSetUp()

        dc = self.dmd.Devices.createOrganizer('/Server')
        self.d = dc.createInstance('hadoop.testDevice')
        self.d.manageIp = '10.10.10.10'
        self.d.dmd._p_jar = MockJar()
        self.applyDataMap = ApplyDataMap()._applyDataMap

    def _loadZenossData(self):
        if hasattr(self, '_loaded'):
            return

        data = {}
        d_modeler = HadoopDataNode()
        tab_modeler_results = load_data('hadoop_data_node_data.txt')
        result = tab_modeler_results.split('</configuration>')
        data['conf'] = result[0] + '</configuration>'
        data['jmx'] = result[1]

        for data_map in d_modeler.process(self.d, data, log):
            self.applyDataMap(self.d, data_map)

        s_modeler = HadoopServiceNode()
        tab_modeler_results = load_data('hadoop_service_node_data.txt')
        result = tab_modeler_results.split('</configuration>')
        data['conf'] = result[0] + '</configuration>'
        data['jmx'] = result[1]

        for data_map in s_modeler.process(self.d, data, log):
            self.applyDataMap(self.d, data_map)

        self._loaded = True

    def test_HadoopDataNode(self):
        self._loadZenossData()

        # Test live datanodes
        data_node = self.d.hadoop_data_nodes._getOb(
            'hadoop.testDevice' + NAME_SPLITTER + 'localhost.live'
        )
        self.assertEquals(data_node.device().id, 'hadoop.testDevice')
        self.assertEquals(data_node.last_contacted, 0)
        self.assertEquals(data_node.health_state, 'Normal')

        # Test dead datanodes
        data_node = self.d.hadoop_data_nodes._getOb(
            'hadoop.testDevice' + NAME_SPLITTER + 'localhost.dead'
        )
        self.assertEquals(data_node.last_contacted, 1)
        self.assertEquals(data_node.health_state, 'Dead')

        # Test decommissioned datanodes
        data_node = self.d.hadoop_data_nodes._getOb(
            'hadoop.testDevice' + NAME_SPLITTER + 'localhost.decom'
        )
        self.assertEquals(data_node.last_contacted, 2)
        self.assertEquals(data_node.health_state, 'Decommissioned')

    def test_HadoopSecondaryNameNode(self):
        self._loadZenossData()

        second_name_node = self.d.hadoop_secondary_name_node._getOb(
            'hadoop.testDevice' + NAME_SPLITTER + '10.10.10.10_50090'
        )
        self.assertEquals(second_name_node.device().id, 'hadoop.testDevice')
        self.assertEquals(second_name_node.title, '10.10.10.10:50090')
        self.assertEquals(second_name_node.health_state, None)

    def test_HadoopJobTracker(self):
        self._loadZenossData()

        job_tracker = self.d.hadoop_job_tracker._getOb(
            'hadoop.testDevice' + NAME_SPLITTER + '192.192.0.0_50030'
        )
        self.assertEquals(job_tracker.device().id, 'hadoop.testDevice')
        self.assertEquals(job_tracker.title, '192.192.0.0:50030')
        self.assertEquals(job_tracker.health_state, None)


class HadoopModelerHelpersTestCase(BaseTestCase):

    def afterSetUp(self):
        super(HadoopModelerHelpersTestCase, self).afterSetUp()
        self.d_modeler = HadoopDataNode()
        self.s_modeler = HadoopServiceNode()

    def test_node_oms(self):
        tab_modeler_results = load_data('hadoop_data_node_data.txt')
        result = tab_modeler_results.split('</configuration>')
        result = result[0] + '</configuration>'

        data = '{"localhost":{"usedSpace":49152,"lastContact":1}}'
        device = Mock()
        device.id = 'hadoop.testDevice'
        device.manageIp = '10.10.10.10'
        om = node_oms(log, device, data, "Normal", result)[0]
        self.assertEquals(
            om.id, device.id + NAME_SPLITTER + '10.10.10.10'
        )
        self.assertEquals(om.last_contacted, 1)
        self.assertEquals(om.health_state, 'Normal')

    def test_get_attr(self):
        results = load_data('hadoop_service_node_data.txt')
        result = results.split('</configuration>')
        data = ET.fromstring(result[0] + '</configuration>')
        attr = 'mapred.job.tracker.http.address'
        self.assertEquals(
            get_attr(attr, data), '192.192.0.0:50030'
        )
        self.assertEquals(get_attr('test', data), '')

    def test_prep_ip(self):
        device = Mock()
        ip_port = prep_ip(device, '192.192.0.0:50030')
        self.assertEquals(ip_port, '192.192.0.0:50030')
        device.manageIp = '10.10.10.10'
        ip_port = prep_ip(device, '0.0.0.0:50030')
        self.assertEquals(ip_port, '10.10.10.10:50030')

        results = load_data('hadoop_data_node_data.txt')
        result = results.split('</configuration>')
        data = ET.fromstring(result[0] + '</configuration>')
        ip_port = prep_ip(device, '${yarn.resourcemanager.hostname}:8088', data)
        self.assertEquals(ip_port, '10.10.10.10:8088')
        ip_port = prep_ip(device, '${yarn.nodemanager.hostname}:8042', data)
        self.assertEquals(ip_port, '100.10.0.0:8042')


def test_suite():
    from unittest import TestSuite, makeSuite
    suite = TestSuite()
    suite.addTest(makeSuite(HadoopComponentsTestCase))
    suite.addTest(makeSuite(HadoopModelerHelpersTestCase))
    return suite

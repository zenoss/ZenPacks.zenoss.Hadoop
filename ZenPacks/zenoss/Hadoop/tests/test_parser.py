##############################################################################
#
# Copyright (C) Zenoss, Inc. 2014, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

from mock import Mock

from Products.ZenTestCase.BaseTestCase import BaseTestCase
from ZenPacks.zenoss.Hadoop.dsplugins import HadoopPlugin
from ZenPacks.zenoss.Hadoop.tests.utils import load_data


class TestParser(BaseTestCase):

    def afterSetUp(self):
        self.plugin = HadoopPlugin()
        self.ds = Mock()
        self.ds.id = 'LiveNodes'
        self.ds.points = [self.ds]
        self.ds.datasource = "NameNodeMonitor"
        self.ds.component = 'Component'

        self.result = load_data('test_data_for_parser.txt')

    def test_form_values(self):
        self.assertEquals(
            self.plugin.form_values(
                self.result, self.ds
            ), {'LiveNodes': (1, 'N')}
        )
        self.ds.id = 'heap_memory_used_bytes'
        self.assertEquals(
            self.plugin.form_values(
                self.result, self.ds
            ), {'heap_memory_used_bytes': (2, 'N')}
        )
        self.ds.datasource = "JobTrackerMonitor"
        self.ds.id = 'jobs_running'
        self.assertEquals(
            self.plugin.form_values(
                self.result, self.ds
            ), {'jobs_running': (3, 'N')}
        )

    def test_service_nodes_remodel(self):
        self.assertEquals(
            self.plugin.service_nodes_remodel(
                self.ds, 'Normal'
            ), []
        )
        self.ds.datasource = 'DataNodeMonitor'
        self.assertEquals(
            str(self.plugin.service_nodes_remodel(
                self.ds, 'Normal')
                ),
            ("[<ObjectMap {'compname': 'hadoop_data_nodes/Component',\n "
             "'health_state': 'Normal',\n 'modname': 'HadoopDataNode'}>]")
        )


def test_suite():
    from unittest import TestSuite, makeSuite
    suite = TestSuite()
    suite.addTest(makeSuite(TestParser))
    return suite

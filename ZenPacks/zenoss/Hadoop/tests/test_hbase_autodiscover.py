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
from ZenPacks.zenoss.Hadoop.parsers.hadoop_parser import hadoop_parser
from ZenPacks.zenoss.Hadoop.tests.utils import load_data


class TestHBaseAutodiscover(BaseTestCase):

    def afterSetUp(self):
        self.result = Mock()
        self.result.values = []
        self.result.maps = []
        self.result.events = []

        self.cmd = Mock()
        self.cmd.result.exitCode = 0
        self.cmd.result.output = load_data('test_data_for_hbase_autodiscover.txt')
        self.cmd.ds = "HBaseDiscoverMonitor"

        # Patch apply_maps method.
        hadoop_parser.apply_maps = lambda *args, **kwargs: None

    def test_parser_events(self):
        assert 'HBase was discovered' in  hadoop_parser().processResults(
            self.cmd, self.result).events[0]['summary']

        self.assertEquals(
            hadoop_parser().processResults(
                self.cmd, self.result
            ).events[1]['summary'],
            'Successfully parsed collected data.'
        )


def test_suite():
    from unittest import TestSuite, makeSuite
    suite = TestSuite()
    suite.addTest(makeSuite(TestHBaseAutodiscover))
    return suite

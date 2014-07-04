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


class TestParser(BaseTestCase):

    def afterSetUp(self):
        self.result = Mock()
        self.result.values = []
        self.result.events = []

        self.cmd = Mock()
        self.cmd.result.exitCode = 0
        self.cmd.result.output = load_data('test_data_for_parser.txt')
        self.cmd.id = 'LiveNodes'
        self.cmd.points = [self.cmd]
        self.cmd.ds = "NameNodeMonitor"

        # Patch apply_maps method.
        hadoop_parser.apply_maps = lambda *args, **kwargs: None

    def test_parser_values(self):
        self.assertEquals(
            hadoop_parser().processResults(
                self.cmd, self.result
            ).values[0][1], 1
        )
        self.cmd.id = 'heap_memory_used_bytes'
        self.assertEquals(
            hadoop_parser().processResults(
                self.cmd, self.result
            ).values[1][1], 2
        )
        self.cmd.ds = "JobTrackerMonitor"
        self.cmd.id = 'jobs_running'
        self.assertEquals(
            hadoop_parser().processResults(
                self.cmd, self.result
            ).values[2][1], 3
        )

    def test_parser_events(self):
        self.assertEquals(
            hadoop_parser().processResults(
                self.cmd, self.result
            ).events[0]['summary'],
            'Successfully parsed collected data.'
        )
        self.cmd.result.output = 'fake'
        self.assertEquals(
            hadoop_parser().processResults(
                self.cmd, self.result
            ).events[1]['summary'],
            'Error parsing collected data.'
        )
        self.cmd.result.exitCode = 7
        self.assertEquals(
            hadoop_parser().processResults(
                self.cmd, self.result
            ).events[2]['summary'],
            'Error parsing collected data: No monitoring data received for {}.'
            .format(self.cmd.name)
        )


def test_suite():
    from unittest import TestSuite, makeSuite
    suite = TestSuite()
    suite.addTest(makeSuite(TestParser))
    return suite

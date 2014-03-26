##############################################################################
#
# Copyright (C) Zenoss, Inc. 2014, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################


from Products.ZenRRD.CommandParser import CommandParser
from Products.ZenUtils.Utils import getExitMessage
from Products.ZenEvents import ZenEventClasses
import json
import logging

log = logging.getLogger("zen.HadoopParser")


class hadoop_parser(CommandParser):
    def processResults(self, cmd, result):
        """
        Parse the results of the hadoop service node command.
        """
        if cmd.result.exitCode != 0:
            result.events.append(dict(
                severity=ZenEventClasses.Error,
                summary='Parsing collected data: {}'.format(
                    getExitMessage(cmd.result.exitCode)
                ),
                eventKey='json_parse',
                eventClassKey='hadoop_json_parse',
            ))

            return result

        try:
            data = json.loads(cmd.result.output)
        except Exception, ex:
            result.events.append(dict(
                severity=ZenEventClasses.Error,
                summary='Error parsing collected data',
                message='Error parsing collected data: %s' % ex,
                eventKey='hadoop_json_parse',
                eventClassKey='json_parse_error',
            ))

        return result

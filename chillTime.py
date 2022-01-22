"""
REQUIRES WeeWX V4.2 OR LATER!

To use:
    1. Stop weewxd
    2. Put this file in your user subdirectory.
    3. In weewx.conf, subsection [Engine][[Services]], add ChillTimeService to the list
    "xtype_services". For example, this means changing this

        [Engine]
            [[Services]]
                xtype_services = weewx.wxxtypes.StdWXXTypes, weewx.wxxtypes.StdPressureCooker, weewx.wxxtypes.StdRainRater

    to this:

        [Engine]
            [[Services]]
                xtype_services = weewx.wxxtypes.StdWXXTypes, weewx.wxxtypes.StdPressureCooker, weewx.wxxtypes.StdRainRater, user.chillTime.ChillTimeService

    4. Optionally, add the following section to weewx.conf:
        [ChillTime]
            algorithm = simple   # Or utah, or modified

    5. Restart weewxd

"""

import logging

import weewx
import weewx.units
import weewx.xtypes
from weewx.engine import StdService
from weewx.units import ValueTuple

# Create a new unit group, 'group_duration', and assign chillTime to it.
weewx.units.obs_group_dict['chillTime'] = "group_duration"
weewx.units.USUnits['group_duration'] = 'hour'
weewx.units.MetricUnits['group_duration'] = 'hour'
weewx.units.MetricWXUnits['group_duration'] = 'hour'

log = logging.getLogger(__name__)


class ChillTime(weewx.xtypes.XType):

    def __init__(self, algorithm='simple'):
        # Save the algorithm to be used.
        self.algorithm = algorithm.lower()

    def get_scalar(self, obs_type, record, db_manager):
        """Calculate 'chillTime'. For everything else, raise an exception UnknownType"""

        if obs_type != 'chillTime':
            raise weewx.UnknownType(obs_type)

        # We need outTemp and interval in order to do the calculation.
        if record.get('outTemp') is None or record.get('interval') is None:

        # We have everything we need. Start by forming a ValueTuple for the outside temperature and archive interval.
        # To do this, figure out what unit and group the outTemp record is in
        unit_and_group = weewx.units.getStandardUnitType(record['usUnits'], 'outTemp')
        # ... then form the ValueTuple.
        outTemp_vt = ValueTuple(record['outTemp'], *unit_and_group)
        # Now do it for the interval
        unit_and_group = weewx.units.getStandardUnitType(record['usUnits'], 'interval')
        interval_vt = ValueTuple(record['interval'], *unit_and_group)

        # Both algorithms need temperature in Farenheit, so let's make sure our incoming temperature
        # is in that unit. Use function convert(). The results will be in the form of a ValueTuple
        outTemp_F_vt = weewx.units.convert(outTemp_vt, 'degree_F')
        interval_H_vt = weewx.units.convert(interval_vt, 'hour')
        # Get the first element of the ValueTuple. This will be in Celsius:
        outTemp_F = outTemp_F_vt[0]
        interval_H = interval_H_vt[0]

        if self.algorithm == 'simple':
            # Use the "Simple" algorithm. Results will be in hours. Create a ValueTuple out of it:
            if outTemp_F < 45:
                chill_time = interval_H
            else:
                chill_time = 0
            log.debug("Found Simple chill hours of %s hours", chill_time)
        elif self.algorithm == 'utah':
            # Use the "Utah" algorithm, weighting chill temps between 32 and 45F
            if outTemp_F <= 34:
                chill_time = 0
            elif 34 < outTemp_F <= 36:
                chill_time = interval_H * .5
            elif 36 < outTemp_F <= 48:
                chill_time = interval_H
            elif 48 < outTemp_F <= 54:
                chill_time = interval_H * .5
            elif 54 < outTemp_F <= 60:
                chill_time = 0
            elif 60 < outTemp_F <= 65:
                chill_time = interval_H * -.5
            elif 65 < outTemp_F:
                chill_time = interval_H * -1
            else:
                chill_time = 0
            log.debug("Found Utah chill hours of %s hours", chill_time)
        elif self.algorithm == 'modified':
            # Use the "Modified" algorithm, counting only chill temps between 32 and 45F
            if 32 < outTemp_F < 45:
                chill_time = interval_H
            else:
                chill_time = 0
            log.debug("Found Modified chill hours of %s hours", chill_time)
        else:
            # Don't recognize the algorithm type. Fail hard:
            raise ValueError("Unrecognized chill time algorithm '%s'" % self.algorithm)

        # Form a ValueTuple out of our results and return it
        chill_vt = ValueTuple(chill_time, 'hour', 'group_duration')

        return chill_vt

    def get_aggregate(self, obs_type, timespan, aggregate_type, db_manager, **option_dict):

        if obs_type != 'chillTime':
            raise weewx.UnknownType(obs_type)
        if aggregate_type != 'sum':
            raise weewx.UnknownAggregation(aggregate_type)

        ###*** Needs to be moved to after the sql call, not sure how to make it exit the 'for' loop
        '''
        if 'outTemp' not in record or record['outTemp'] is None:
            raise weewx.CannotCalculate(obs_type)
        if 'interval' not in record or record['interval'] is None:
            raise weewx.CannotCalculate(obs_type)
        '''

        interp_dict = {
            'table': db_manager.table_name,
            'start': timespan.start,
            'stop': timespan.stop
        }

        chill_total = 0

        for record in db_manager.getBatchRecords(*timespan):
            chill_delta = self.get_scalar('chillTime', record, db_manager)
            chill_total += chill_delta

        chill_vt = ValueTuple(chill_total, 'hour', 'group_elapsed')

        return chill_vt


class ChillTimeService(StdService):
    """ WeeWX service whose job is to register the XTypes extension ChillTime with the
    XType system.
    """

    def __init__(self, engine, config_dict):
        super(ChillTimeService, self).__init__(engine, config_dict)

        # Get the desired algorithm. Default to "simple".
        try:
            algorithm = config_dict['ChillTime']['algorithm']
        except KeyError:
            algorithm = 'simple'

        # Instantiate an instance of ChillTime:
        self.ch = ChillTime(algorithm)
        # Register it:
        weewx.xtypes.xtypes.append(self.ch)

    def shutDown(self):
        # Remove the registered instance:
        weewx.xtypes.xtypes.remove(self.ch)

"""
REQUIRES WeeWX V4.2 OR LATER!

To use:
    1. Stop weewxd
    2. Put this file in your user subdirectory.
    3. In weewx.conf, subsection [Engine][[Services]], add ChillHoursService to the list
    "xtype_services". For example, this means changing this

        [Engine]
            [[Services]]
                xtype_services = weewx.wxxtypes.StdWXXTypes, weewx.wxxtypes.StdPressureCooker, weewx.wxxtypes.StdRainRater

    to this:

        [Engine]
            [[Services]]
                xtype_services = weewx.wxxtypes.StdWXXTypes, weewx.wxxtypes.StdPressureCooker, weewx.wxxtypes.StdRainRater, user.chillHours.ChillHoursService

    4. Optionally, add the following section to weewx.conf:
        [ChillHours]
            algorithm = simple   # Or tetens

    5. Restart weewxd

"""
import math

import weewx
import weewx.units
import weewx.xtypes
from weewx.engine import StdService
from weewx.units import ValueTuple
import logging

# Tell the unit system what group our new observation type, 'chillHours', belongs to:
weewx.units.obs_group_dict['chillHours'] = "group_elapsed"

log = logging.getLogger(__name__)


class ChillHours(weewx.xtypes.XType):

    def __init__(self, algorithm='simple'):
        # Save the algorithm to be used.
        self.algorithm = algorithm.lower()

    def get_scalar(self, obs_type, record, db_manager):
        # Calculate 'chillHours'. For everything else, raise an exception UnknownType
        if obs_type != 'chillHours':
            raise weewx.UnknownType(obs_type)

        # We need outTemp and interval in order to do the calculation.
        if 'outTemp' not in record or record['outTemp'] is None:
            raise weewx.CannotCalculate(obs_type)
        if 'interval' not in record or record['interval'] is None:
            raise weewx.CannotCalculate(obs_type)

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
            chill_vt = ValueTuple(chill_time, 'hour', 'group_elapsed')
            log.debug("Found chill hours of %s hours", chill_time)
        elif self.algorithm == 'utah':
            # Use the "Modified" algorithm, weighting chill temps between 32 and 45F
            if outTemp_F <= 34:
                chill_time = 0
            elif 34 < outTemp_F and outTemp_F <= 36:
                chill_time = interval_H * .5
            elif 36 < outTemp_F and outTemp_F <= 48:
                chill_time = interval_H
            elif 48 < outTemp_F and outTemp_F <= 54:
                chill_time = interval_H * .5
            elif 54 < outTemp_F and outTemp_F <= 60:
                chill_time = 0
            elif 60 < outTemp_F and outTemp_F <= 65:
                chill_time = interval_H * -.5
            elif 65 < outTemp_F:
                chill_time = interval_H * -1
            chill_vt = ValueTuple(chill_time, 'hour', 'group_elapsed')
            log.debug("Found chill hours of %s hours", chill_time)
        elif self.algorithm == 'modified':
            # Use the "Modified" algorithm, counting only chill temps between 32 and 45F
            if outTemp_F < 45 and 32 < outTemp_F:
                chill_time = interval_H
            else:
                chill_time = 0
            chill_vt = ValueTuple(chill_time, 'hour', 'group_elapsed')
            log.debug("Found chill hours of %s hours", chill_time)
        else:
            # Don't recognize the exception. Fail hard:
            raise ValueError(self.algorithm)

        # We have the chill hours as a ValueTuple. Convert it back to the units used by
        # the incoming record and return it
        log.debug("Returning chillHours tuple: ", print(chill_vt))
        return weewx.units.convertStd(chill_vt, record['usUnits'])
        


class ChillHoursService(StdService):
    """ WeeWX service whose job is to register the XTypes extension ChillHours with the
    XType system.
    """

    def __init__(self, engine, config_dict):
        super(ChillHoursService, self).__init__(engine, config_dict)

        # Get the desired algorithm. Default to "simple".
        try:
            algorithm = config_dict['ChillHours']['algorithm']
        except KeyError:
            algorithm = 'simple'

        # Instantiate an instance of VaporPressure:
        self.ch = ChillHours(algorithm)
        # Register it:
        weewx.xtypes.xtypes.append(self.ch)

    def shutDown(self):
        # Remove the registered instance:
        weewx.xtypes.xtypes.remove(self.ch)

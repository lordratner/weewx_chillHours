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
import math

import weewx
import weewx.units
import weewx.xtypes
from weewx.engine import StdService
from weewx.units import ValueTuple

import logging

# Tell the unit system what group our new observation type, 'chillTime', belongs to:
weewx.units.obs_group_dict['chillTime'] = "group_elapsed"

log = logging.getLogger(__name__)


class ChillTime(weewx.xtypes.XType):

    def __init__(self, algorithm='simple'):
        # Save the algorithm to be used.
        self.algorithm = algorithm.lower()

    sql_stmts = {
###*** Need to make sure the sqlLite statement is correct, since I don't use it
        'sqlite': "SELECT outTemp FROM {table} WHERE dateTime BETWEEN {start} AND {stop}",
        'mysql': "SELECT outTemp FROM {table} WHERE dateTime BETWEEN {start} AND {stop}"
    }

    def get_scalar(self, obs_type, record, db_manager):
        # Calculate 'chillTime'. For everything else, raise an exception UnknownType
        if obs_type != 'chillTime':
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
            log.debug("Found Simple chill hours of %s hours", chill_time)
        elif self.algorithm == 'utah':
            # Use the "Utah" algorithm, weighting chill temps between 32 and 45F
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
            log.debug("Found Utah chill hours of %s hours", chill_time)
        elif self.algorithm == 'modified':
            # Use the "Modified" algorithm, counting only chill temps between 32 and 45F
            if outTemp_F < 45 and 32 < outTemp_F:
                chill_time = interval_H
            else:
                chill_time = 0
            chill_vt = ValueTuple(chill_time, 'hour', 'group_elapsed')
            log.debug("Found Modified chill hours of %s hours", chill_time)
        else:
            # Don't recognize the exception. Fail hard:
            raise ValueError(self.algorithm)

        # We have the chill hours as a ValueTuple. Convert it back to the units used by the incoming record and return it
###*** Converting back to the schema units turns it into seconds. For now this conflicts with the image generators that can't convert the units to hours. When fixed, this can be uncommented and the temp solution removed
        #return weewx.units.convertStd(chill_vt, record['usUnits'])
        return chill_vt

    def get_aggregate(self, obs_type, timespan, aggregate_type, db_manager, **option_dict):

        dbtype = db_manager.connection.dbtype
        
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

        sql_stmt = ChillTime.sql_stmts[dbtype].format(**interp_dict)

        try:
            row_gen = db_manager.genBatchRecords(timespan.start, timespan.stop)
        except:
            log.error("weedb No Column Error rasied")

###*** Need the correct error hanlding for genBatchRecords here. No clue what it should be   
        """
        except weedb.NoColumnError:
            log.error("weedb No Column Error rasied")
            raise weewx.UnknownType(aggregate_type)

        if not row or None in row:
            value = None"""

        chill_total = 0
        
###*** Need better way to get usUnits. Pass from ChillTimeService from config_dict?
        vt_sql = 'SELECT usUnits FROM archive ORDER BY dateTime DESC LIMIT 1'
        vt_units = db_manager.getSql(vt_sql)[0]
        
        
        for record in row_gen:
            vt_units = record['usUnits']
            # We have everything we need. Start by forming a ValueTuple for the outside temperature and archive interval.
            # To do this, figure out what unit and group the outTemp record is in
            unit_and_group = weewx.units.getStandardUnitType(vt_units, 'outTemp')
            # ... then form the ValueTuple.
            outTemp_vt = ValueTuple(record['outTemp'], *unit_and_group)
            # Now do it for the interval
            unit_and_group = weewx.units.getStandardUnitType(vt_units, 'interval')
            interval_vt = ValueTuple(record['interval'], *unit_and_group)

            # Both algorithms need temperature in Farenheit, so let's make sure our incoming temperature
            # is in that unit. Use function convert(). The results will be in the form of a ValueTuple
            outTemp_F_vt = weewx.units.convert(outTemp_vt, 'degree_F')
            interval_H_vt = weewx.units.convert(interval_vt, 'hour')
            # Get the first element of the ValueTuple. This will be in Celsius:
            outTemp_F = outTemp_F_vt[0]
            interval_H = interval_H_vt[0]
            
            if self.algorithm == 'simple' and outTemp_F != None:
                # Use the "Simple" algorithm. Results will be in hours. Create a ValueTuple out of it:
                if outTemp_F < 45:
                    chill_time = interval_H
                else:
                    chill_time = 0
            elif self.algorithm == 'utah' and outTemp_F != None:
                # Use the "Utah" algorithm, weighting chill temps between 32 and 45F
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
            elif self.algorithm == 'modified' and outTemp_F != None:
                # Use the "Modified" algorithm, counting only chill temps between 32 and 45F
                if outTemp_F < 45 and 32 < outTemp_F:
                    chill_time = interval_H
                else:
                    chill_time = 0
            # Not sure the 'right' way to catch a None that sneaks in here, but it's popping up
            elif outTemp_F == None:
                chill_time = 0
            else:
                # Don't recognize the exception. Fail hard:
                raise ValueError(self.algorithm)
            chill_total += chill_time
        chill_vt = ValueTuple(chill_total, 'hour', 'group_elapsed')

        # We have the chill hours as a ValueTuple. Convert it back to the units used by the incoming record and return it        
###*** Converting back to the schema units turns it into seconds. For now this conflicts with the image generators that can't convert the units to hours. When fixed, this can be uncommented and the temp solution removed
        #return weewx.units.convertStd(chill_vt, record['usUnits'])
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

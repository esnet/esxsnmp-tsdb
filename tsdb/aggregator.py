import itertools
from math import floor, ceil
from fpconst import isNaN
import time

from tsdb.error import *
from tsdb.row import Aggregate, ROW_VALID, ROW_TYPE_MAP, ROW_INVALID

class Aggregator(object):
    """Calculate Aggregates.
    
    XXX ultimately there should be an aggregator for each class of TSDBVars.
    This one is really targeted at Counters and should become
    CounterAggregator.  It should be possible to generalize some of this
    functionality into a base Aggregator class though."""

    def __init__(self, agg, ancestor):
        self.agg = agg
        self.ancestor = ancestor

    def _empty_row(self, var, timestamp):
        aggs = {}
        for a in var.metadata['AGGREGATES']:
            aggs[a] = 0

        return var.type(timestamp, 0, **aggs)

    def _invalidate_row(self, var, timestamp):
        if var.type != Aggregate:
            raise TSDBVarIsNotAggregate("not an Aggregate")

        try:
            row = var.get(timestamp)
        except (TSDBVarEmpty, TSDBVarRangeError):
            row = self._empty_row(var, timestamp)

        row.flags |= ROW_INVALID
        if row.flags & ROW_VALID:
            row.flags ^= ROW_VALID

        var.insert(row)

    def _increase_delta(self, var, timestamp, value):
        if var.type != Aggregate:
            raise TSDBVarIsNotAggregate("not an Aggregate")

        try:
            row = var.get(timestamp)
        except (TSDBVarEmpty, TSDBVarRangeError):
            row = self._empty_row(var, timestamp)

        if not row.flags & ROW_INVALID:
            row.delta += value
            row.flags |= ROW_VALID
            var.insert(row)

    def update(self, uptime_var=None, min_last_update=None, max_rate=None,
            error_callback=None):
        """Update an aggregate.

        ``uptime_var``
            A monotonically increasing variable that shows how long the system
            was up at a given time.  Usually it is of type TimeTicks.
        ``min_last_update``
            When updating this aggregate go back no further than
            `min_last_update`.
        ``max_rate``
            if rate > max_rate then the row in the computed aggregate is set
            to invalid.  if ``error_callback`` is not None then
            ``error_callback`` to notify the upper level application of
            potentially bad data. 
        ``error_callback``
            a function that takes single argument:
                ``message``
                    a string describing what went wrong
        """

        try:
            if self.ancestor.type == Aggregate:
                self.update_from_aggregate(min_last_update=min_last_update,
                        max_rate=max_rate, error_callback=error_callback)
            else:
                self.update_from_raw_data(uptime_var=uptime_var,
                        min_last_update=min_last_update, max_rate=max_rate,
                        error_callback=error_callback)
        except TSDBVarEmpty:
            # not enough data to build aggregate
            pass

    def update_from_raw_data(self, uptime_var=None, min_last_update=None,
            max_rate=None, error_callback=None):
        """Update this aggregate from raw data.

        The first aggregate MUST have the same step as the raw data.  (This
        the only aggregate with a raw data ancestor.)

        Scan all of the new data and bin it in the appropriate place.  At
        either end a bin may have only partial data.  Detect and handle
        rollovers.  We process all data with a timestamp >= begin and
        with ROW_VALID set.

        Diagram of the relationship between the prev and curr elements in the
        main loop::

            prev_slot          curr_slot
            |                  |
            v                  v
            +----------+       +----------+
            |   prev   | . . . |   curr   |
            +----------+       +----------+
                ^                   ^
                |                   |
                prev.timestamp      curr.timestamp
                |                   |
                |<---- delta_t ---->|
        """
        step = self.agg.metadata['STEP']
        assert self.ancestor.metadata['STEP'] == step

        last_update = self.agg.metadata['LAST_UPDATE']
        if min_last_update and min_last_update > last_update:
            last_update = min_last_update

        min_ts = self.ancestor.min_timestamp()
        if min_ts > last_update:
            last_update = min_ts
            self.agg.metadata['LAST_UPDATE'] = last_update

        prev = self.ancestor.get(last_update)
        now = int(time.time())

        # XXX this only works for Counter types right now
        for curr in self.ancestor.select(begin=last_update+step,
                end=now, # limit unnecessary IO, there's no data in the future
                flags=ROW_VALID):

            # if our previous value is not valid, then only set LAST_UPDATE
            # and return. this avoids big spikes after periods of missing data
            # this way we'll generate an accurate aggregate at the next timestep
            if prev.flags != ROW_VALID:
                self.agg.metadata['LAST_UPDATE'] = curr.timestamp
                self.agg.flush()
                return

            delta_t = curr.timestamp - prev.timestamp
            delta_v = curr.value - prev.value
            prev_slot = (prev.timestamp / step) * step
            curr_slot = (curr.timestamp / step) * step

            # trying to detect rollovers was buggy in practice, so instead
            # we just invalidate rows where the counter decreases. We also
            # invalidate rows on each side of the decreasing measurement as
            # the partial updates are misleading
            if delta_v < 0:
                # the partial updates to the aggregate can be misleading/confusing
                self._invalidate_row(self.agg, prev_slot)
                self._invalidate_row(self.agg, curr_slot)
                if error_callback:
                    error_callback("counter decreased: %s between %d %d: %d > %d" % (
                        self.ancestor.path,
                        prev.timestamp,
                        curr.timestamp,
                        prev.value,
                        curr.value))
                prev = curr # so LAST_UPDATE is updated
                continue

            rate = float(delta_v) / float(delta_t)

            if max_rate and rate > max_rate:
                if error_callback:
                    error_callback("rate too high for %s at %d: %f" % (self.ancestor.path,
                                                                       curr.timestamp, rate))

                prev = curr
                continue

            assert delta_v >= 0

            # allocate a portion of this data to a given bin
            prev_frac = int( floor(
                        delta_v * (prev_slot+step - prev.timestamp)
                        / float(delta_t)
                    ))

            curr_frac = int( ceil(
                        delta_v * (curr.timestamp - curr_slot)
                        / float(delta_t)
                    ))

            if delta_t > self.agg.metadata['HEARTBEAT']:
                for slot in range(prev_slot, curr_slot, step):
                    try:
                        row = self.agg.get(slot)
                    except TSDBVarRangeError:
                        row = self._empty_row(self.agg, slot)

                    row.invalidate()
                    self.agg.insert(row)

                self._increase_delta(self.agg, curr_slot, curr_frac)
                prev = curr
                continue

            self._increase_delta(self.agg, curr_slot, curr_frac)
            self._increase_delta(self.agg, prev_slot, prev_frac)

            # if we have some left, try to backfill
            if curr_frac + prev_frac != delta_v:
                missed_slots = range(prev_slot+step, curr_slot, step)
                if not missed_slots:
                    missed_slots = [curr_slot]
                missed = delta_v - (curr_frac + prev_frac)
                if missed > len(missed_slots):
                    missed_frac = missed / len(missed_slots)
                    missed_rem = missed % (missed_frac * len(missed_slots))
                    for slot in missed_slots:
                        self._increase_delta(self.agg, slot, missed_frac)

                    # distribute the remainder
                    for i in range(missed_rem):
                        self._increase_delta(self.agg, missed_slots[i], 1)

            prev = curr


        for row in self.agg.select(begin=last_update, flags=ROW_VALID):
            if row.delta != 0:
                row.average = float(row.delta) / step
            else:
                row.average = 0.0
            self.agg.insert(row)

        self.agg.metadata['LAST_UPDATE'] = prev.timestamp
        self.agg.flush()

    def update_from_aggregate(self, min_last_update=None, max_rate=None,
            error_callback=None):
        """Update this aggregate from another aggregate."""
        # LAST_UPDATE points to the last step updated

        step = self.agg.metadata['STEP']
        steps_needed = step // self.ancestor.metadata['STEP']
        # XXX what to do if our step isn't divisible by ancestor steps?

        last_update = self.agg.metadata['LAST_UPDATE'] + \
                        self.ancestor.metadata['STEP']

        if min_last_update and min_last_update > last_update:
            last_update = min_last_update

        data = self.ancestor.select(
                begin=last_update,
                end=self.ancestor.max_valid_timestamp())

        # get all timestamps since the last update
        # fill as many bins as possible
        work = list(itertools.islice(data, 0, steps_needed))

        slot = None
        while len(work) == steps_needed:
            slot = ((work[0].timestamp / step) * step) #+ step

#            assert work[-1].timestamp == slot

            valid = 0
            row = Aggregate(slot, ROW_VALID, delta=0, average=None,
                    min=None, max=None)

            for datum in work:
                if datum.flags & ROW_VALID:
                    valid += 1
                    row.delta += datum.delta

                    if isNaN(row.min) or datum.delta < row.min:
                        row.min = datum.delta

                    if isNaN(row.max) or datum.delta > row.max:
                        row.max = datum.delta
            row.average = row.delta / float(step)
            valid_ratio = float(valid)/float(len(work))

            if valid_ratio < self.agg.metadata['VALID_RATIO']:
                row.invalidate()

            self.agg.insert(row)

            work = list(itertools.islice(data, 0, steps_needed))

        if slot is not None:
            self.agg.metadata['LAST_UPDATE'] = slot
            self.agg.flush()

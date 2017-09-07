#!/usr/bin/env python

import os

from nose import with_setup

from tsdb import *
from tsdb.row import Counter64, TimeTicks, ROW_INVALID
from tsdb.chunk_mapper import YYYYMMDDChunkMapper

TESTDB = os.path.join(os.environ.get('TMPDIR', 'tmp'), 'actual_testdb')

def db_reset():
    os.system("rm -rf %s" % TESTDB)

@with_setup(db_reset, db_reset)
def test_rounding1():
    """This caused a rounding error in one version of the code.
    From observed data."""

    db = TSDB.create(TESTDB)
    var = db.add_var("test1", Counter64, 30, YYYYMMDDChunkMapper)
    agg = var.add_aggregate("30s", YYYYMMDDChunkMapper, ['average', 'delta'],
            {'HEARTBEAT': 90})
    var.insert(Counter64(1204329701, ROW_VALID, 54652476))
    var.insert(Counter64(1204329731, ROW_VALID, 54652612))
    var.update_all_aggregates()

@with_setup(db_reset, db_reset)
def test_erroneous_data1():
    """value went backwards but was not a rollover.
    From observed data."""

    db = TSDB.create(TESTDB)
    var = db.add_var("test1", Counter64, 30, YYYYMMDDChunkMapper)
    up = db.add_var("uptime", TimeTicks, 30, YYYYMMDDChunkMapper)
    agg = var.add_aggregate("30s", YYYYMMDDChunkMapper, ['average', 'delta'],
            {'HEARTBEAT': 90})

    var.insert(Counter64(1204345906, ROW_VALID, 54697031))
    var.insert(Counter64(1204345937, ROW_VALID, 54696971))
    var.insert(Counter64(1204345967, ROW_VALID, 54696981))

    up.insert(TimeTicks(1204345906, ROW_VALID, 677744266))
    up.insert(TimeTicks(1204345937, ROW_VALID, 677747340))
    var.update_all_aggregates(uptime_var=up)

@with_setup(db_reset, db_reset)
def test_erroneous_data2():
    """value went backwards but was not a rollover.
    From observed data.  test max_rate and max_rate_callback as well"""

    db = TSDB.create(TESTDB)
    var = db.add_var("test1", Counter64, 30, YYYYMMDDChunkMapper)
    up = db.add_var("uptime", TimeTicks, 30, YYYYMMDDChunkMapper)
    agg = var.add_aggregate("30s", YYYYMMDDChunkMapper, ['average', 'delta'],
            {'HEARTBEAT': 90})

    var.insert(Counter64(1204345906, ROW_VALID, 54697031))
    var.insert(Counter64(1204345937, ROW_VALID, 54696971))
    var.insert(Counter64(1204345967, ROW_VALID, 54696981))

    up.insert(TimeTicks(1204345906, ROW_VALID, 677744266))
    up.insert(TimeTicks(1204345937, ROW_VALID, 677747340))

    bad_data = []
    def callback(message):
        bad_data.append(message)

    var.update_all_aggregates(uptime_var=up, max_rate=int(11*1e9),
            error_callback=callback)

    print bad_data
    assert len(bad_data) == 1

@with_setup(db_reset, db_reset)
def test_gaps1():
    """there are one or more missing chunks in the middle of the range"""

    db = TSDB.create(TESTDB)
    var = db.add_var("test1", Counter64, 30, YYYYMMDDChunkMapper)
    up = db.add_var("uptime", TimeTicks, 30, YYYYMMDDChunkMapper)

    var.insert(Counter64(0, ROW_VALID, 1))
    var.insert(Counter64(1 + 2*24*3600, ROW_VALID, 1))
    var.flush()

    #os.system("ls -l %s/test1" % TESTDB)
    var.get(1 + 24*3600)


@with_setup(db_reset, db_reset)
def test_select_bounds():
    """if select gets called with a begin time that isn't on a slot boundary
    data may not be found in the last slot."""

    db = TSDB.create(TESTDB)
    var = db.add_var("test1", Counter64, 30, YYYYMMDDChunkMapper)
    var.insert(Counter64(0, ROW_VALID, 1))
    var.insert(Counter64(33, ROW_VALID, 2))
    var.flush()

    l = [x for x in  var.select(begin=5)]
    print l
    assert len(l) == 2

@with_setup(db_reset, db_reset)
def test_select_bounds2():
    """select returns one row too many"""

    db = TSDB.create(TESTDB)
    var = db.add_var("test1", Counter64, 30, YYYYMMDDChunkMapper)
    var.insert(Counter64(0, ROW_VALID, 1))
    var.insert(Counter64(33, ROW_VALID, 2))
    var.flush()

    l = [x for x in var.select(begin=5, end=30)]
    print l
    assert len(l) == 1

def create_inclusive_test_data_set():
    db = TSDB.create(TESTDB)
    var = db.add_var("test1", Counter64, 30, YYYYMMDDChunkMapper)
    var.insert(Counter64(0, ROW_VALID, 1))
    var.insert(Counter64(33, ROW_VALID, 2))
    var.flush()
    return var

@with_setup(db_reset, db_reset)
def test_select_inclusive_end():
    """check for inclusive results at end of time range"""

    var = create_inclusive_test_data_set()

    l = [x for x in var.select(begin=33, end=33)]
    print l
    assert len(l) == 1
    assert l[0].timestamp == 33

@with_setup(db_reset, db_reset)
def test_select_inclusive_begin():
    """check for inclusive results at end of time range"""

    var = create_inclusive_test_data_set()

    l = [x for x in var.select(begin=0, end=0)]
    print l
    assert len(l) == 1
    assert l[0].timestamp == 0

@with_setup(db_reset, db_reset)
def test_select_inclusive_begin():
    """check for inclusive results at end of time range"""

    var = create_inclusive_test_data_set()

    l = [x for x in var.select(begin=0, end=0)]
    print l
    assert len(l) == 1
    assert l[0].timestamp == 0


@with_setup(db_reset, db_reset)
def test_bad_aggregation1():
    """Test for overly large values in aggregates after a period of missing data.

    There was a period of missing values in the raw counter variable,
    once data resumed there was large spike.  this is actual counter data
    from:

    star-cr5/ALUSAPPoll/sapBaseStatsIngressQchipForwardedInProfOctets/6012-2_2_1-3506

    for the timerange:

    1454514846i (2016-02-03 09:54:06)
    1454516436 (2016-02-03 10:20:36)

    the SAP collection doesn't include sysUpTime and so we don't use uptime in
    this test.

    The solution is to just invalidate the first row after a gap.
    """

    db = TSDB.create(TESTDB)
    var = db.add_var("test1", Counter64, 30, YYYYMMDDChunkMapper)
    agg = var.add_aggregate("30s", YYYYMMDDChunkMapper, ['average', 'delta'])

    bad_data = []
    def callback(ancestor, agg, rate, prev, curr):
        bad_data.append(rate)

    def update(var, ts, flags, value, update_agg=True):
        counter = Counter64(ts, flags, value)
        var.insert(counter)
        var.flush()

        if update_agg:
            # if ts > 1454516405:
            #     import pdb
            #     pdb.set_trace()

            min_last_update = ts - (30*40)
            var.update_aggregate("30",
                                 uptime_var=None,
                                 min_last_update=min_last_update,
                                 max_rate=int(110e9),
                                 error_callback=callback)

    update(var, 1454514785, ROW_VALID, 63615634280445, update_agg=False)
    update(var, 1454514815, ROW_VALID, 63615634282858)
    update(var, 1454514846, ROW_VALID, 63615634284139)
    update(var, 1454514877, ROW_VALID, 63615634286704)
    update(var, 1454516405, ROW_VALID, 63615634387100)
    update(var, 1454516436, ROW_VALID, 63615634388440)

    assert len(bad_data) == 0

    for r in agg.select(begin=1454514815, end=1454516436):
        if r.timestamp == 1454516400:
            if r.average > 8.0e+09:
                print "got incorrect value after gap", r
                assert r.average > 8.0e+09
        if r.timestamp > 1454516400:
            assert (r.average - 8.66667) < 0.1


@with_setup(db_reset, db_reset)
def test_bad_aggregation2():
    """Aggregations are incorrect with decreasing counters with an invalid data point in the middle.

    https://github.com/esnet/esxsnmp-tsdb/issues/2"""
    db = TSDB.create(TESTDB)
    var = db.add_var("test1", Counter64, 30, YYYYMMDDChunkMapper)
    agg = var.add_aggregate("30s", YYYYMMDDChunkMapper, ['average', 'delta'])

    bad_data = []
    def callback(message):
        bad_data.append(message)

    def update(var, ts, flags, value, update_agg=True):
        counter = Counter64(ts, flags, value)
        var.insert(counter)
        var.flush()

        if update_agg:
            # if ts > 1454516405:
            #     import pdb
            #     pdb.set_trace()

            min_last_update = ts - (30*40)
            #if ts == 1501635268:
            #    import pdb; pdb.set_trace()

            var.update_aggregate("30",
                                 uptime_var=None,
                                 min_last_update=min_last_update,
                                 max_rate=int(110e9),
                                 error_callback=callback)

    update(var, 1501635150, ROW_VALID, 300290824551, update_agg=False)
    update(var, 1501635180, ROW_VALID, 300290824919)
    update(var, 1501635210, ROW_VALID, 300290825047)
    update(var, 1501635268, 0x0, 0)
    update(var, 1501635270, ROW_VALID, 300290814799)
    update(var, 1501635305, ROW_VALID, 300290815063)

    assert len(bad_data) == 1
    print bad_data
    for row in agg.select(begin=1501635150, end=1501635305):
        print row

    bad1 = agg.get(1501635210)
    assert not bad1.flags & ROW_VALID
    assert bad1.flags & ROW_INVALID

    bad2 = agg.get(1501635270)
    assert not bad2.flags & ROW_VALID
    assert bad2.flags & ROW_INVALID



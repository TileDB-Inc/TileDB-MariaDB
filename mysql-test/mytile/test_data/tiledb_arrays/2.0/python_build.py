import tiledb
import numpy

array_name = "all_datetimes"

# The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
dom = tiledb.Domain(tiledb.Dim(name="id", domain=(1, 4), tile=4, dtype=numpy.int32))

# The array will be sparse with a single attribute "a" so each (i,j) cell can store an integer.
schema = tiledb.ArraySchema(domain=dom, sparse=True,
                            attrs=[tiledb.Attr(name="datetime_year", dtype="datetime64[Y]"),
                                   tiledb.Attr(name="datetime_month", dtype="datetime64[M]"),
                                   tiledb.Attr(name="datetime_week", dtype="datetime64[W]"),
                                   tiledb.Attr(name="datetime_day", dtype="datetime64[D]"),
                                   tiledb.Attr(name="datetime_hour", dtype="datetime64[h]"),
                                   tiledb.Attr(name="datetime_minute", dtype="datetime64[m]"),
                                   tiledb.Attr(name="datetime_second", dtype="datetime64[s]"),
                                   tiledb.Attr(name="datetime_millisecond", dtype="datetime64[ms]"),
                                   tiledb.Attr(name="datetime_microsecond", dtype="datetime64[us]"),
                                   tiledb.Attr(name="datetime_nanosecond", dtype="datetime64[ns]"),
                                   tiledb.Attr(name="datetime_picosecond", dtype="datetime64[ps]"),
                                   tiledb.Attr(name="datetime_femtosecond", dtype="datetime64[fs]"),
                                   tiledb.Attr(name="datetime_attosecond", dtype="datetime64[as]"),
                                  ])

# Create the (empty) array on disk.
tiledb.SparseArray.create(array_name, schema)

with tiledb.open(array_name, mode='w') as A:
    datetime_Y = numpy.datetime64("2020")
    datetime_M = numpy.datetime64("2020-07")
    datetime_W = numpy.datetime64("2020-07-26 13:45:55").astype('datetime64[W]')
    datetime_D = numpy.datetime64("2020-07-26")
    datetime_h = numpy.datetime64("2020-07-26 13")
    datetime_m = numpy.datetime64("2020-07-26 13:45")
    datetime_s = numpy.datetime64("2020-07-26 13:45:55")
    datetime_ms = numpy.datetime64("2020-07-26 13:45:55.123")
    datetime_us = numpy.datetime64("2020-07-26 13:45:55.123456")
    datetime_ns = numpy.datetime64("2020-07-26 13:45:55.123456789")
    datetime_ps = numpy.datetime64("2020-07-26 13:45:55.123456789123")
    datetime_fs = numpy.datetime64("2020-07-26 13:45:55.123456789123456")
    datetime_as = numpy.datetime64("2020-07-26 13:45:55.123456789123456789")
    #A.meta["datetime_year"] = datetime_Y
    #A.meta["datetime_month"] = datetime_M
    #A.meta["datetime_week"] = datetime_W
    #A.meta["datetime_day"] = datetime_D
    #A.meta["datetime_hour"] = datetime_h
    #A.meta["datetime_minute"] = datetime_m
    #A.meta["datetime_second"] = datetime_s
    #A.meta["datetime_millisecond"] = datetime_ms
    #A.meta["datetime_microsecond"] = datetime_us
    #A.meta["datetime_nanosecond"] = datetime_ns
    #A.meta["datetime_picosecond"] = datetime_ps
    #A.meta["datetime_femtosecond"] = datetime_fs
    #A.meta["datetime_attosecond"] = datetime_as

    A[1] = {
    "datetime_year": datetime_Y,
    "datetime_month": datetime_M,
    "datetime_week": datetime_W,
    "datetime_day": datetime_D,
    "datetime_hour": datetime_h,
    "datetime_minute": datetime_m,
    "datetime_second": datetime_s,
    "datetime_millisecond": datetime_ms,
    "datetime_microsecond": datetime_us,
    "datetime_nanosecond": datetime_ns,
    "datetime_picosecond": datetime_ps,
    "datetime_femtosecond": datetime_fs,
    "datetime_attosecond": datetime_as,
    }

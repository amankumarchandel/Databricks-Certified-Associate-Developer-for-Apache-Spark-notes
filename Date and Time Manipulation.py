#!/usr/bin/env python
# coding: utf-8

# ## Date and Time Manipulation Functions

# * We can use `current_date` to get today’s server date. 
#   * Date will be returned using **yyyy-MM-dd** format.
# * We can use `current_timestamp` to get current server time. 
#   * Timestamp will be returned using **yyyy-MM-dd HH:mm:ss:SSS** format.
#   * Hours will be by default in 24 hour format.

# In[ ]:


l = [('X',)]


# In[ ]:


df = spark.createDataFrame(l).toDF('dummy')


# In[ ]:


from pyspark.sql.functions import current_date, current_timestamp


# In[ ]:


df.select(current_date()).show()


# In[ ]:


df.select(current_timestamp()).show(truncate=False)


# * We can convert a string which contain date or timestamp in non-standard format to standard date or time using `to_date` or `to_timestamp` function respectively.

# In[ ]:


from pyspark.sql.functions import lit,to_date,to_timestamp


# In[ ]:


df.select(to_date(lit('20210228'),'yyyyMMdd').alias('to_date')).show()


# In[ ]:


df.select(to_timestamp(lit('20210228 1725'),'yyyyMMdd HHmm').alias('to_timestamp')).show()


# Date and Time Arithmetic

# * Adding days to a date or timestamp - `date_add`
# * Subtracting days from a date or timestamp - `date_sub`
# * Getting difference between 2 dates or timestamps - `datediff`
# * Getting the number of months between 2 dates or timestamps - `months_between`
# * Adding months to a date or timestamp - `add_months`
# * Getting next day from a given date - `next_day`
# * All the functions are self explanatory. We can apply these on standard date or timestamp. All the functions return date even when applied on timestamp field.

# In[ ]:


datetimes = [("2014-02-28", "2014-02-28 10:00:00.123"),
                     ("2016-02-29", "2016-02-29 08:08:08.999"),
                     ("2017-10-31", "2017-12-31 11:59:59.123"),
                     ("2019-11-30", "2019-08-31 00:00:00.000")
                ]


# In[ ]:


datetimesDF = spark.createDataFrame(datetimes, schema="date STRING, time STRING")


# In[ ]:


datetimesDF.show(truncate=False)


# * Add 10 days to both date and time values.
# * Subtract 10 days from both date and time values.

# In[ ]:


from pyspark.sql.functions import date_add, date_sub


# In[ ]:


help(date_add)


# In[ ]:


datetimesDF.  withColumn('date_add_date', date_add('date',10)).  withColumn('time_add_date',date_add('time',10)).  withColumn('date_sub_date',date_sub('date',10)).  withColumn('time_sub_date',date_sub('time',10)).show()


# * Get the difference between current_date and date values as well as current_timestamp and time values.

# In[ ]:


from pyspark.sql.functions import current_date, current_timestamp, datediff


# In[ ]:


datetimesDF.  withColumn('datediff_date',datediff(current_date(),'date')).  withColumn('datediff_time',datediff(current_timestamp(),'time')).show()


# * Get the number of months between current_date and date values as well as current_timestamp and time values.
# * Add 3 months to both date values as well as time values.

# In[ ]:


from pyspark.sql.functions import months_between, add_months , round


# In[ ]:


datetimesDF.  withColumn('months_between_date',round(months_between(current_date(),'date'),2)). withColumn('months_between_time',round(months_between(current_timestamp(),'time'),2)). withColumn('add_months_date',add_months('date',3)). withColumn('add_months_time',add_months('time',3)).show(truncate=False)


# ## Using Date and Time Trunc Functions
# In Data Warehousing we quite often run to date reports such as week to date, month to date, year to date etc.

# * We can use `trunc` or `date_trunc` for the same to get the beginning date of the week, month, current year etc by passing date or timestamp to it.
# * We can use `trunc` to get beginning date of the month or year by passing date or timestamp to it - for example `trunc(current_date(), "MM")` will give the first of the current month.
# * We can use `date_trunc` to get beginning date of the month or year as well as beginning time of the day or hour by passing timestamp to it.
#   * Get beginning date based on month - `date_trunc("MM", current_timestamp())`
#   * Get beginning time based on day - `date_trunc("DAY", current_timestamp())`

# In[ ]:


from pyspark.sql.functions import trunc, date_trunc


# In[ ]:


help(trunc)


# In[ ]:


help(date_trunc)


# In[ ]:


datetimesDF.show(truncate=False)


# In[ ]:


from pyspark.sql.functions import trunc


# In[ ]:


datetimesDF.  withColumn('date_trunc',trunc('date','MM')).  withColumn('time_trunc',trunc('time','yy')).show(truncate=False)


# Get begginning hour time using date and time field.

# In[ ]:


from pyspark.sql.functions import date_trunc


# In[ ]:


datetimesDF. withColumn('date_trunc',date_trunc('MM','date')). withColumn('time_trunc',date_trunc('yy','time')).show(truncate=False)


# In[ ]:


datetimesDF.  withColumn('date_dt',date_trunc('HOUR','date')).  withColumn('time_dt',date_trunc('HOUR','time')).  withColumn('time_dt1', date_trunc('dd','time')).show()


# * `year`
# * `month`
# * `weekofyear`
# * `dayofyear`
# * `dayofmonth`
# * `dayofweek`
# * `hour`
# * `minute`
# * `second`
# 
# There might be few more functions. You can review based up on your requirements.

# In[ ]:


df = spark.createDataFrame([('X',)],['dummy'])


# In[ ]:


df.show()


# In[ ]:


from pyspark.sql.functions import year,month,weekofyear,dayofmonth,dayofyear,dayofweek,current_date


# In[ ]:


df.select(
 current_date(),\
  year(current_date()).alias('year'),\
  month(current_date()).alias('month'),\
  weekofyear(current_date()).alias('weekofyear'),\
  dayofmonth(current_date()).alias('dayofmonth'),\
  dayofyear(current_date()).alias('dayofyear'),\
  dayofweek(current_date()).alias('dayofweek')
).show()


# In[ ]:


help(weekofyear)


# In[ ]:


from pyspark.sql.functions import current_timestamp,hour,minute,second


# In[ ]:


df.select(
    current_timestamp().alias('current_timestamp'), 
    year(current_timestamp()).alias('year'),
    month(current_timestamp()).alias('month'),
    dayofmonth(current_timestamp()).alias('dayofmonth'),
    hour(current_timestamp()).alias('hour'),
    minute(current_timestamp()).alias('minute'),
    second(current_timestamp()).alias('second')
).show(truncate=False) #yyyy-MM-dd HH:mm:ss.SSS


# Using to_date and to_timestamp

# * `yyyy-MM-dd` is the standard date format
# * `yyyy-MM-dd HH:mm:ss.SSS` is the standard timestamp format
# * Most of the date manipulation functions expect date and time using standard format. However, we might not have data in the expected standard format.
# * In those scenarios we can use `to_date` and `to_timestamp` to convert non standard dates and timestamps to standard ones respectively.

# In[ ]:


datetimes = [(20140228, "28-Feb-2014 10:00:00.123"),
                     (20160229, "20-Feb-2016 08:08:08.999"),
                     (20171031, "31-Dec-2017 11:59:59.123"),
                     (20191130, "31-Aug-2019 00:00:00.000")
                ]


# In[ ]:


datetimesDF = spark.createDataFrame(datetimes, schema="date BIGINT, time STRING")


# In[ ]:


datetimesDF.show(truncate=False)


# In[ ]:


from pyspark.sql.functions import lit, to_date


# In[ ]:


df.show()


# In[ ]:


df.select(to_date(lit('20210809'),'yyyyMMdd').alias('to_date')).show()


# In[ ]:


# year and day of year to standard date
df.select(to_date(lit('2021061'),'yyyyDDD').alias('to_date')).show()


# In[ ]:


df.select(to_date(lit('02/03/2022'),'dd/MM/yyyy').alias('to_date')).show()


# In[ ]:


df.select(to_date(lit('02-03-2021'),'dd-MM-yyyy').alias('to_date')).show()


# In[ ]:


df.select(to_date(lit('02-Mar-2021'),'dd-MMM-yyyy').alias('to_date')).show()


# In[ ]:


df.select(to_date(lit('02-March-2021'),'dd-MMMM-yyyy').alias('to_date')).show()


# In[ ]:


df.select(to_date(lit('March 2,2022'),'MMMM d,yyyy').alias('to_date')).show()


# In[ ]:


from pyspark.sql.functions import to_timestamp


# In[ ]:


df.select(to_timestamp(lit('02-Apr-2022'),'dd-MMM-yyyy').alias('to_date')).show()


# In[ ]:


df.select(to_timestamp(lit('02-Mar-2021 17:30:15'),'dd-MMM-yyyy HH:mm:ss').alias('to_date')).show()


# In[ ]:


from pyspark.sql.functions import col


# In[ ]:


datetimesDF.  withColumn('to_date',to_date(col('date').cast('string'),'yyyyMMdd')).  withColumn('to_timestamp',to_timestamp(col('time'),'dd-MMM-yyyy HH:mm:ss.SSS')).show(truncate=False)


# * We can use `date_format` to extract the required information in a desired format from standard date or timestamp. Earlier we have explored `to_date` and `to_timestamp` to convert non standard date or timestamp to standard ones respectively.
# * There are also specific functions to extract year, month, day with in a week, a day with in a month, day with in a year etc. These are covered as part of earlier topics in this section or module.

# In[ ]:


datetimes = [("2014-02-28", "2014-02-28 10:00:00.123"),
                     ("2016-02-29", "2016-02-29 08:08:08.999"),
                     ("2017-10-31", "2017-12-31 11:59:59.123"),
                     ("2019-11-30", "2019-08-31 00:00:00.000")
                ]


# In[ ]:


datetimesDF = spark.createDataFrame(datetimes, schema="date STRING, time STRING")


# In[ ]:


datetimesDF.show(truncate=False)


# In[ ]:


from pyspark.sql.functions import date_format


# In[ ]:


datetimesDF.  withColumn('date_ym',date_format('date','yyyyMM')).  withColumn('time_ym',date_format('time','yyyyMM')).show(truncate=False)

# yyyy
# MM
# dd
# DD
# HH
# hh
# mm
# ss
# SSS


# In[ ]:


datetimesDF.  withColumn('date_dt',date_format('date','yyyyMMddHHmmss')).  withColumn('date_ts',date_format('time','yyyyMMddHHmmss')).show(truncate=False)


# In[ ]:


datetimesDF.     withColumn("date_dt", date_format("date", "yyyyMMddHHmmss").cast('long')).     withColumn("date_ts", date_format("time", "yyyyMMddHHmmss").cast('long')).     show(truncate=False)


# In[ ]:





# In[ ]:


datetimesDF.  withColumn('date_yd',date_format('date','yyyyDDD').cast('int')).  withColumn('time_yd',date_format('time','yyyyDDD').cast('int')).show(truncate=False)


# get complete description of the date

# In[ ]:


datetimesDF.  withColumn('date_desc',date_format('date','MMMM dd, yyyy')).show(truncate=False)


# In[ ]:


# name of the week day

datetimesDF.  withColumn('day_name_abbr',date_format('date','EE')).show()


# In[ ]:


datetimesDF.  withColumn('day_full_name',date_format('date','EEEE')).show()


# Dealing with Unix Timestamp

# * It is an integer and started from January 1st 1970 Midnight UTC.
# * Beginning time is also known as epoch and is incremented by 1 every second.
# * We can convert Unix Timestamp to regular date or timestamp and vice versa.
# * We can use `unix_timestamp` to convert regular date or timestamp to a unix timestamp value. For example `unix_timestamp(lit("2019-11-19 00:00:00"))`
# * We can use `from_unixtime` to convert unix timestamp to regular date or timestamp. For example `from_unixtime(lit(1574101800))`
# * We can also pass format to both the functions.

# In[ ]:


datetimes = [(20140228, "2014-02-28", "2014-02-28 10:00:00.123"),
                     (20160229, "2016-02-29", "2016-02-29 08:08:08.999"),
                     (20171031, "2017-10-31", "2017-12-31 11:59:59.123"),
                     (20191130, "2019-11-30", "2019-08-31 00:00:00.000")
                ]


# In[ ]:


datetimesDF = spark.createDataFrame(datetimes).toDF("dateid", "date", "time")


# In[ ]:


datetimesDF.show(truncate=False)


# In[ ]:


from pyspark.sql.functions import unix_timestamp, col


# In[ ]:


datetimesDF.  withColumn('unix_date_id',unix_timestamp(col('dateid').cast('string'),'yyyyMMdd')).  withColumn('unix_date',unix_timestamp('date','yyyy-MM-dd')).  withColumn('unix_time',unix_timestamp('time','yyyy-MM-dd HH:mm:ss.SSS')).show()


# In[ ]:


unixtimes = [(1393561800, ),
             (1456713488, ),
             (1514701799, ),
             (1567189800, )
            ]


# In[ ]:


unixtimesDF = spark.createDataFrame(unixtimes).toDF('unixtime')


# In[ ]:


unixtimesDF.show()


# In[ ]:


from pyspark.sql.functions import from_unixtime


# In[ ]:


unixtimesDF.  withColumn('date',from_unixtime('unixtime','yyyyMMdd')).  withColumn('time',from_unixtime('unixtime')).show()


# In[ ]:





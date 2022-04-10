#!/usr/bin/env python
# coding: utf-8

# In[ ]:


ages_list = [22,25,18,19,28]


# In[ ]:


type(ages_list)


# In[ ]:


spark.createDataFrame(ages_list,'int')


# In[ ]:


from pyspark.sql.types import IntegerType
spark.createDataFrame(ages_list,IntegerType())


# In[ ]:


names_list = ['Scott','Donald','Mickey']


# In[ ]:


spark.createDataFrame(names_list,'string')


# In[ ]:


from pyspark.sql.types import StringType
spark.createDataFrame(ages_list,StringType())


# In[ ]:


ages_list =[(21,),(23,),(41,),(32,)]


# In[ ]:


type(ages_list)


# In[ ]:


type(ages_list[2])


# In[ ]:


spark.createDataFrame(ages_list)


# In[ ]:


spark.createDataFrame(ages_list,'age int')


# In[ ]:


user_list = [(1,'Scott'),(2,'Donald'),(3,'Mickey'),(4,'Elvis')]


# In[ ]:


spark.createDataFrame(user_list)


# In[ ]:


df = spark.createDataFrame(user_list,'user_id int,user_first_name string')


# In[ ]:


df.show()


# In[ ]:


df.collect()


# In[ ]:


type(df.collect())


# In[ ]:


from pyspark.sql import Row


# In[ ]:


r = Row("Alice",11)


# In[ ]:


r


# In[ ]:


row2 = Row(name='Alice',age = 11)


# In[ ]:


row2


# In[ ]:


row2.name


# In[ ]:


row2['name']


# In[ ]:


#dataFrame using list of list
user_list = [[1,'Scott'],[2,'Donald'],[3,'Mickey'],[4,'Elvis']]


# In[ ]:


type(user_list[1])


# In[ ]:


df = spark.createDataFrame(user_list,'user_id int, user_first_name string')


# In[ ]:


df.show()


# In[ ]:


users_rows = [ Row(*user) for user in user_list]


# In[ ]:


users_rows


# In[ ]:


df = spark.createDataFrame(users_rows,'user_id int, user_first_name string')


# In[ ]:


df.show()


# In[ ]:


def dummy(*args):
  print(args)


# In[ ]:


dummy(1)


# In[ ]:


dummy(1,'Hello')


# In[ ]:


#take list as 1 argument
user_details = [1,'Scott']
dummy(user_details)


# In[ ]:


#take list as different arguments
dummy(*user_details)


# In[ ]:


user_list = [(1,'Scott'),(2,'Donald'),(3,'Mickey'),(4,'Elvis')]


# In[ ]:


type(user_list[1])


# In[ ]:


df = spark.createDataFrame(user_list,'user_id int,user_first_name string')


# In[ ]:


df.show()


# In[ ]:


users_list = [Row(*user) for user in user_list]


# In[ ]:


df = spark.createDataFrame(user_list,'user_id int,user_first_name string')


# In[ ]:


df.show()


# In[ ]:


#Convert List of Dicts into Spark DataFrame using Row
user_list = [
  {'user_id':1 , 'user_first_name':'Scott'},
  {'user_id':2 , 'user_first_name':'Donald'},
  {'user_id':3 , 'user_first_name':'Mickey'},
  {'user_id':4 , 'user_first_name':'Elvis'}
]


# In[ ]:


df = spark.createDataFrame(user_list)


# In[ ]:


df.show()


# In[ ]:


users_list = [Row(**user) for user in user_list]


# In[ ]:


df = spark.createDataFrame(users_list)


# In[ ]:


df.show()


# In[ ]:


#Alternative approch
users_rows = [ Row(*user.values()) for user in user_list]


# In[ ]:


df = spark.createDataFrame(users_rows,'user_id int,user_first_name string')


# In[ ]:


df.show()


# In[ ]:


def dummy(**kwargs):
  print(kwargs)


# In[ ]:


dummy(user_id =1, user_first_name='Scott')


# In[ ]:


my_list = {'user_id':1,'user_name':'Scott'}
dummy(**my_list)


# In[ ]:


import datetime
users = [
  {
    "id":1,
    "first_name":"Corrie",
    "last_name":"Van den Oord",
    "email":"cvandenoord0@etsy.com",
    "is_customer":True,
    "amount_paid":100.55,
    "customer_from":datetime.date(2021,1,15),
    "last_updated_ts":datetime.datetime(2021,2,10,1,15,0)
  },
  {
    "id":2,
    "first_name":"Nikolaus",
    "last_name":"BBrewitt",
    "email":"nbrewitti@dailymail.co.uk",
    "is_customer":True,
    "amount_paid":900.0,
    "customer_from":datetime.date(2021,2,14),
    "last_updated_ts": datetime.datetime(2021,2,18,3,33,0)
  }
]


# In[ ]:


from pyspark.sql import Row


# In[ ]:


users_df = spark.createDataFrame([Row(**user) for user in users])


# In[ ]:


users_df.printSchema()


# In[ ]:


users_df.columns


# In[ ]:


users_df.dtypes


# In[ ]:


user = [
  (1,
   "Aman",
   "abcd1234aman@gmail.com",
   True,
   1000.55,
   datetime.date(2021,1,15)
   ),
  (2,
   "Tushar",
   "tushar@gamil.com",
   False,
   1000.55,
   datetime.date(2022,3,4)
  )
]


# In[ ]:


users_schema = '''
    id INT,
    name STRING,
    email STRING,
    is_customer BOOLEAN,
    salary FLOAT,
    joined_date DATE
'''


# In[ ]:


df = spark.createDataFrame(user,schema=users_schema)


# In[ ]:


df.printSchema()


# In[ ]:


df.show()


# In[ ]:


onlyColumnName_Schema = [
  'id',
  'name',
  'email',
  'is_customer',
  'salary',
  'joined_date'
]


# In[ ]:


df = spark.createDataFrame(user,onlyColumnName_Schema)


# In[ ]:


df.printSchema()


# In[ ]:


df.show()


# In[ ]:


from pyspark.sql.types import *


# In[ ]:


fields = StructType([
  StructField('id',IntegerType()),
  StructField('name',StringType()),
  StructField('email',StringType()),
  StructField('is_customer',StringType()),
  StructField('salary',FloatType()),
  StructField('joined_date',DateType())
])


# In[ ]:


spark.createDataFrame(user,fields).printSchema()


# In[ ]:


type(fields)


# In[ ]:


# We can use panda's dataframe in case some filed are missing in dict
# import pandas as pd
# spark.createDataFrame(pd.DataFrame(user)).show()


# * Here are the special types that are supported by Spark
#   * Array
#   * Struct
#   * Map
# * Python structures such as list and dict can implicitly conveted to Spark Array and Map respectively.
# * We need to use few Spark related API's to convert Python data Structures to Struct type.

# In[ ]:


users = [
  {
    "id":1,
    "first_name":"Corrie",
    "last_name":"Van den Oord",
    "email":"cvandenoord0@etsy.com",
    "phone_numbers":["4234234234","42342343243"],
    "is_customer":True,
    "amount_paid":100.55,
    "customer_from":datetime.date(2021,1,15),
    "last_updated_ts":datetime.datetime(2021,2,10,1,15,0)
  },
  {
    "id":2,
    "first_name":"Nikolaus",
    "last_name":"BBrewitt",
    "email":"nbrewitti@dailymail.co.uk",
    "phone_numbers":["4234212124","4234212123"],
    "is_customer":True,
    "amount_paid":900.0,
    "customer_from":datetime.date(2021,2,14),
    "last_updated_ts": datetime.datetime(2021,2,18,3,33,0)
  }
]


# In[ ]:


user_df = spark.createDataFrame([Row(**user) for user in users])


# In[ ]:


user_df.printSchema()


# In[ ]:


user_df.select('id','phone_numbers').show(truncate=False)


# In[ ]:


user_df.columns


# In[ ]:


user_df.dtypes


# In[ ]:


from pyspark.sql.functions import explode


# In[ ]:


user_df.  withColumn('phone_number',explode('phone_numbers')).  drop('phone_numbers').show()


# In[ ]:


from pyspark.sql.functions import col


# In[ ]:


user_df.  select('id',col('phone_numbers')[0].alias('mobile'),col('phone_numbers')[1].alias('home')).show()
# it will print null if data is missing


# * explode_outer can be used if you want to keep record if it has null value in array column

# In[ ]:


users = [
  {
    "id":1,
    "first_name":"Corrie",
    "last_name":"Van den Oord",
    "email":"cvandenoord0@etsy.com",
    "phone_numbers":{"mobile":"4234234234","home":"42342343243"},
    "is_customer":True,
    "amount_paid":100.55,
    "customer_from":datetime.date(2021,1,15),
    "last_updated_ts":datetime.datetime(2021,2,10,1,15,0)
  },
  {
    "id":2,
    "first_name":"Nikolaus",
    "last_name":"BBrewitt",
    "email":"nbrewitti@dailymail.co.uk",
    "phone_numbers":{"mobile":"4234212124","home":"4234212123"},
    "is_customer":True,
    "amount_paid":900.0,
    "customer_from":datetime.date(2021,2,14),
    "last_updated_ts": datetime.datetime(2021,2,18,3,33,0)
  }
]


# In[ ]:


user_df = spark.createDataFrame([Row(**user) for user in users])


# In[ ]:


user_df.dtypes


# In[ ]:


user_df.printSchema()


# In[ ]:


user_df.show()


# In[ ]:


user_df.select('id','phone_numbers').show(truncate=False)


# In[ ]:


user_df.select('*',explode('phone_numbers')).  withColumnRenamed('key','phone_type').  withColumnRenamed('value','phone_number').  drop('phone_numbers').show()


# In[ ]:


user_df.  select('id',col('phone_numbers')['mobile'].alias('mobile'),col('phone_numbers')['home'].alias('home')).show()


# In[ ]:


user_df.select('id',explode('phone_numbers')).show()


# In[ ]:


users = [
  {
    "id":1,
    "first_name":"Corrie",
    "last_name":"Van den Oord",
    "email":"cvandenoord0@etsy.com",
    "phone_numbers":Row(mobile="4234234234",home="42342343243"),
    "is_customer":True,
    "amount_paid":100.55,
    "customer_from":datetime.date(2021,1,15),
    "last_updated_ts":datetime.datetime(2021,2,10,1,15,0)
  },
  {
    "id":2,
    "first_name":"Nikolaus",
    "last_name":"BBrewitt",
    "email":"nbrewitti@dailymail.co.uk",
    "phone_numbers":Row(mobile="4234212124",home="4234212123"),
    "is_customer":True,
    "amount_paid":900.0,
    "customer_from":datetime.date(2021,2,14),
    "last_updated_ts": datetime.datetime(2021,2,18,3,33,0)
  },
  {
    "id":3,
    "first_name":"Niko",
    "last_name":"BBrewitt",
    "email":"nbrewitti@dailymail.co.uk",
    "phone_numbers":Row(mobile=None,home=None),
    "is_customer":True,
    "amount_paid":900.0,
    "customer_from":datetime.date(2021,2,14),
    "last_updated_ts": datetime.datetime(2021,2,18,3,33,0)
  },
  {
    "id":4,
    "first_name":"Mike",
    "last_name":"BBrewitt",
    "email":"nbrewitti@dailymail.co.uk",
    "phone_numbers":Row(mobile="7897876656",home=None),
    "is_customer":True,
    "amount_paid":900.0,
    "customer_from":datetime.date(2021,2,14),
    "last_updated_ts": datetime.datetime(2021,2,18,3,33,0)
  }
]


# In[ ]:


user_df = spark.createDataFrame([Row(**user) for user in users])


# In[ ]:


user_df.dtypes


# In[ ]:


user_df.printSchema()


# In[ ]:


user_df.select('id','phone_numbers').show(truncate=False)


# In[ ]:


user_df.  select('id','phone_numbers.mobile','phone_numbers.home').show()


# In[ ]:


user_df.  select('id',col('phone_numbers')['mobile'],col('phone_numbers')['home']).show()


# In[ ]:


user_df.  select('id','phone_numbers.*').show()


# In[ ]:


import datetime
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "phone_numbers": Row(mobile="+1 234 567 8901", home="+1 234 567 8911"),
        "courses": [1, 2],
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "phone_numbers":  Row(mobile="+1 234 567 8923", home="1 234 567 8934"),
        "courses": [3],
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "phone_numbers": Row(mobile="+1 714 512 9752", home="+1 714 512 6601"),
        "courses": [2, 4],
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "phone_numbers": Row(mobile=None, home=None),
        "courses": [],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "phone_numbers": Row(mobile="+1 817 934 7142", home=None),
        "courses": [],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]


# In[ ]:


import pandas as pd


# In[ ]:


spark.conf.set('spark.sql.execution.arrow.pyspark.enabled',False)


# In[ ]:


users_df = spark.createDataFrame(pd.DataFrame(users))


# In[ ]:


users_df.printSchema()


# In[ ]:


users_df.show()


# ##Overview of Narrow and Wide Transformations
# 
# * Here are the functions realated to narrow transformations. Narrow transformations doesn't reault in shuffling. These are also known as row level transformations.
#   * df.select
#   * df.filter
#   * df.withColumn
#   * df.withColumnRenamed
#   * df.drop
# 
# * Here are the functions related to wide transformations.
#   * df.distinct
#   * df.union or any set operation
#   * df.join or any join operation
#   * df.groupBy
#   * df.sort or df.orderBy
# 
# * Any function that result in shuffling is wide transformation. For all the wide transformation, we have to deal with group of records based on a key.

# In[ ]:


help(users_df.select)


# In[ ]:


users_df.select('*').show()


# In[ ]:


users_df.columns


# In[ ]:


users_df.select('id','first_name','last_name').show()


# In[ ]:


users_df.select(['id','first_name','last_name']).show()


# In[ ]:


# Defining alias to the dataframe
users_df.alias('u').select('u.*').show()


# In[ ]:


users_df.alias('u').select('u.id','u.first_name','u.last_name').show()


# In[ ]:


from pyspark.sql.functions import col,concat,lit


# In[ ]:


users_df.select(
  col('id'),
  'first_name',
  'last_name',
  concat(col('first_name'),lit(', '),col('last_name')).alias('full_name')
).show()


# In[ ]:


help(user_df.selectExpr)


# In[ ]:


users_df.selectExpr('*').show()


# In[ ]:


users_df.selectExpr('id','first_name','last_name').show()


# In[ ]:


users_df.selectExpr(['id','first_name','last_name']).show()


# In[ ]:


# Using selectExpr to use Spark SQL Functions
users_df.selectExpr('id','first_name','last_name',"concat(first_name, ', ',last_name) AS full_name").show()


# In[ ]:


users_df.createOrReplaceTempView('users')


# In[ ]:


spark.sql("""
  select id,first_name,last_name,
    concat(first_name,', ',last_name) as full_name 
    from users
""").show()


# ###Referring Column using Spark Data Frame Names

# In[ ]:


users_df['id']


# In[ ]:


col('id')


# In[ ]:


type(user_df['id'])


# In[ ]:


from pyspark.sql.functions import col


# In[ ]:


users_df.select('id',col('first_name'),'last_name').show()


# In[ ]:


users_df.select(users_df['id'],col('first_name'),'last_name').show()


# In[ ]:


# This does not work as there is no object by name u in this session
users_df.alias('u').select(u['id'],col('first_name'),'last_name').show()


# In[ ]:


# This shall work
users_df.alias('u').select('u.id',col('first_name'),'last_name').show()


# In[ ]:


# This does not work as selectExpr can only take column name or SQL style expression in column names
users_df.selectExpr(col('id'),'first_name','last_name').show()


# In[ ]:


user_df.  select(
    'id','first_name','last_name',
    concat(user_df['first_name'],lit(', '),col('last_name')).alias('full_name')
).show()


# In[ ]:


# Using selectExpr to use Spark SQL Functions
users_df.alias('u').  selectExpr('id','first_name','last_name',"concat(u.first_name,', ',u.last_name) as full_name").show()


# In[ ]:


spark.sql("""
  select id,first_name,last_name,
  concat(u.first_name,', ',u.last_name) as full_name
  from users as u
  """).show()


# ####Understanding col function in Spark

# In[ ]:





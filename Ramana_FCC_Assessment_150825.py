#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pyspark


# In[5]:


from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructField,StringType,IntegerType
from pyspark.sql.types import ArrayType,DoubleType,BooleanType
from pyspark.sql.functions import *


# In[6]:


spark=SparkSession.builder.appName("FCC Assessment").getOrCreate()


# In[114]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.types import ArrayType,DoubleType,BooleanType
from pyspark.sql.functions  import column
from pyspark.sql import functions as F


# In[8]:


#####STEP 1 --LOAD USER TABLE


# In[9]:


user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("social_media_handle", StringType(), True),
    StructField("email", StringType(), True)
])


# In[10]:


user_df = spark.read.csv(
    "C:/Users/lenovo/Downloads/user.csv", schema=user_schema, header=True, sep=",")


# In[12]:


#load user_registration


# In[13]:


user_registration_df=spark.read.csv(
    "C:/Users/lenovo/Downloads/user_registration.csv", inferSchema=True, header=True, sep=",")


# In[ ]:


#load user_play_session


# In[14]:


user_play_session_df=spark.read.csv(
    "C:/Users/lenovo/Downloads/user_play_session.csv", inferSchema=True, header=True, sep=",")


# In[15]:


#user_play_session_df.show()


# In[ ]:


# load user_plan


# In[100]:


user_plan_df=spark.read.csv("C:/Users/lenovo/Downloads/user_plan.csv", inferSchema=True, header=True, sep=",")


# In[101]:


user_plan_df.show(2)


# In[ ]:


#Load user_payment_detail


# In[17]:


user_payment_detail_df=spark.read.csv("C:/Users/lenovo/Downloads/user_payment_detail.csv", inferSchema=True, header=True, sep=",")


# In[ ]:


#load status_code


# In[18]:


user_status_code_df=spark.read.csv("C:/Users/lenovo/Downloads/status_code.csv", inferSchema=True, header=True, sep=",")


# In[ ]:


#load plan_payment_frequency


# In[19]:


user_payment_frequency_df=spark.read.csv(
    "C:/Users/lenovo/Downloads/plan_payment_frequency.csv",inferSchema=True, header=True, sep=",")


# In[ ]:


# load channel_code


# In[20]:


user_channel_code_df=spark.read.csv("C:/Users/lenovo/Downloads/channel_code.csv", inferSchema=True, header=True, sep=",")


# In[ ]:


# load plan.csv


# In[21]:


user_plan_df=spark.read.csv("C:/Users/lenovo/Downloads/plan.csv", inferSchema=True, header=True, sep=",")


# In[22]:


#####STEP 2:  CREATE DIM AND FACT TABLE 


# In[23]:


dim_user=user_df.dropDuplicates()


# In[2]:


"""dim_user = user_df.join(user_registration_df, "user_id", "left")     .select(
        col("user_id"),
        col("ip_address"),
        col("social_media_handle"),
        col("username"),
        col("first_name"),
        col("last_name")
    ).dropDuplicates()"""


# In[26]:


dim_plan = user_plan_df.select("plan_id", "payment_frequency_code", "cost_amount").dropDuplicates()


# In[27]:


dim_plan.show(1)


# In[28]:


dim_payment_frequency = user_payment_frequency_df.dropDuplicates()


# In[29]:


dim_payment_frequency.show(1)


# In[30]:


dim_status = user_status_code_df.dropDuplicates()


# In[31]:


dim_status.show(1)


# In[32]:


dim_channel = user_channel_code_df.dropDuplicates()


# In[33]:


dim_channel.show(1)


# In[34]:


dim_payment_method = user_payment_detail_df.dropDuplicates()


# In[35]:


dim_payment_method.show(1)


# In[36]:


user_play_session_df.show(2)


# In[37]:


dim_user_play_session=user_play_session_df.dropDuplicates()


# In[38]:


dim_payment_detail=user_payment_detail_df.dropDuplicates()


# In[39]:


dim_payment_detail.show(2)


# In[40]:


dim_registration=user_registration_df.dropDuplicates()


# In[41]:


dim_registration.show(2)


# In[102]:


dim_user_plan=user_plan_df.dropDuplicates()


# In[103]:


dim_user_plan.show(2)


# In[ ]:


# Create Fact table based on user session 


# In[55]:


fact_play_sessions_df = dim_user_play_session.alias("ups")     .join(dim_user.alias("u"), on="user_id", how="inner")     .join(dim_channel.alias("c"), dim_user_play_session.channel_code == dim_channel.play_session_channel_code, "inner")     .join(dim_status.alias("s"), dim_user_play_session.status_code == dim_status.play_session_status_code, "inner")     .select(
        col("ups.play_session_id"),
        col("ups.user_id"),
        col("u.ip_address"),
        col("u.social_media_handle"),
        col("u.email"),
        col("c.play_session_channel_code").alias("channel_code"),
        col("c.english_description").alias("channel_name"),
        col("s.play_session_status_code").alias("status_code"),
        col("s.english_description").alias("status_name"),
        col("ups.start_datetime"),
        col("ups.end_datetime"),
        col("ups.total_score")
    )


# In[162]:


fact_play_sessions_df.show(1)


# In[ ]:


#To know which channel are using more


# In[62]:


fact_play_sessions_df.groupby("channel_code").count().show()


# In[ ]:


# Find the play count 


# In[163]:


fact_play_sessions_df     .groupBy("channel_code")     .agg(
        F.count("*").alias("play_count"),
        F.sum("total_score").alias("total_score_sum")
    ) \
    .orderBy("channel_code") \
    .show()


# In[ ]:





# In[ ]:


#To know the playing status details


# In[89]:


fact_play_sessions_df.groupby("status_code").count().orderBy(col("count").desc()).show(2)


# In[ ]:


#To find the duration of play -- 


# In[ ]:


from pyspark.sql.functions import col, unix_timestamp, round


# In[81]:


fact_play_sessions_df.withColumn(
    "session_duration_minutes",
    round((unix_timestamp("end_datetime") - unix_timestamp("start_datetime")) / 60, 2)
).select("play_session_id","user_id","channel_code","total_score","session_duration_minutes").show(2)
#fact_play_sessions_df.
#.withColumn("session_duration_minutes", round((unix_timestamp("end_datetime") - unix_timestamp("start_datetime")) / 60, 2))


# In[90]:


#fact_play_sessions_df.show(2)


# In[151]:


#fact_play_sessions_df.write.mode("overwrite").parquet("C:/Users/lenovo/Downloads/fact_play_sessions")


# In[ ]:


# Fact based on user plan and payment


# In[98]:


dim_user.show(2)


# In[104]:


dim_user_plan.show(2)


# In[107]:


fact_user_plan_df = (
    dim_user_plan
    # Join with user registration
    .join(dim_registration, "user_registration_id", "left")
    # Join with user dimension
    .join(dim_user, "user_id", "left")
    # Join with plan dimension
    .join(dim_plan, "plan_id", "left")
    # Join with payment frequency dimension
    .join(dim_payment_frequency, "payment_frequency_code", "left")
    # Join with payment details
    .join(dim_payment_detail, "payment_detail_id", "left"))


# In[161]:


fact_user_plan_df.describe()


# In[77]:


fact_user_plan_df = (
    dim_user_plan
    # Join with user registration
    .join(dim_registration, "user_registration_id", "left")
    # Join with user dimension
    .join(dim_user, "user_id", "left")
    # Join with plan dimension
    .join(dim_plan, "plan_id", "left")
    # Join with payment frequency dimension
    .join(dim_payment_frequency, "payment_frequency_code", "left")
    # Join with payment details
    .join(dim_payment_detail, "payment_detail_id", "left")
    # Select fact table columns
    .select(
        col("user_registration_id"),
        col("user_id"),
        col("username"),
        col("first_name"),
        col("last_name"),
        col("user_df.email").alias("user_email"),
        col("ip_address"),
        col("social_media_handle"),
        col("plan_id"),
        col("payment_frequency_code"),
        col("english_description").alias("payment_frequency_english"),
        col("cost_amount"),
        col("payment_method_code"),
        col("payment_method_value"),
        col("payment_method_expiry"),
        col("start_date"),
        col("end_date")
    )
)


# In[ ]:





# In[ ]:


# To find the highest paid user


# In[115]:


fact_user_plan_df     .groupBy("user_id")     .agg(F.sum("cost_amount").alias("total_cost"))     .orderBy(F.desc("total_cost"))     .show(10)


# In[ ]:


# Find most expensive plan


# In[116]:


fact_user_plan_df.select("user_id", "plan_id", "cost_amount")     .orderBy(F.desc("cost_amount")).show(5)


# In[ ]:


# Find the users based on total cost-- rank 


# In[154]:


from pyspark.sql import functions as F
from pyspark.sql.window import Window


# In[155]:


f1=fact_user_plan_df     .groupBy("user_id")     .agg(F.sum("cost_amount").alias("total_spend"))


# In[119]:


f1.


# In[136]:


window_spec = Window.orderBy(F.desc("total_spend"))


# In[140]:


ranked_users_df = f1.withColumn("rank", F.rank().over(window_spec))                     .orderBy(F.asc("rank"))


# In[141]:


ranked_users_df.show(5)


# In[143]:


ranked_users_df1 = f1.withColumn("rank", F.dense_rank().over(window_spec))                     .orderBy(F.asc("rank"))


# In[145]:


ranked_users_df1.show(10)


# In[152]:


# Save fact table


# In[153]:


#fact_user_plan_df.write.mode("overwrite").parquet("C:/Users/lenovo/Downloads/fact_play_sessions")


# In[ ]:


#subscription details


# In[157]:


fact_user_plan_df.groupby("payment_frequency_code").count().show()


# In[ ]:


Revenue generated through app vs mobile


# In[ ]:


fact_user_plan_df


# In[167]:


fact_final_df = (
    fact_user_plan_df
    # Join with user registration
    .join(fact_play_sessions_df, "user_id", "inner"))


# In[170]:


fact_final_df     .groupBy("channel_code")     .agg(
        F.count("*").alias("play_count"),
        F.sum("cost_amount").alias("total_revenue")
    ) \
    .orderBy("channel_code") \
    .show()


# In[168]:


fact_final_df.show(1)


# In[164]:


#fact_user_plan_df.show(1)


# In[165]:


#fact_play_sessions_df.show(1)


# In[ ]:


fact_play_sessions_df     .groupBy("channel_code")     .agg(
        F.count("*").alias("play_count"),
        F.sum("total_score").alias("total_score_sum")
    ) \
    .orderBy("channel_code") \
    .show()


# In[ ]:





# In[ ]:





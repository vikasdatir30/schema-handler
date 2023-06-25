# -*- coding: utf-8 -*-
"""
Created on Mon Jun 19 14:22:46 2023

@author: Vikas Datir
"""

from pyspark.sql import SparkSession as ss

from schema_handler import Schema_Handler

sh = Schema_Handler('employees_v2.json', 'D:/Hdp_Server/schema-handler/input_schema', 'D:/Hdp_Server/schema-handler/archive_schema')

spark = ss.builder.master('local[*]').appName('Test_Schema_Handler').getOrCreate()

df = spark.read.json('D:/Hdp_Server/schema-handler/sample_data/data.json')

shm = sh.schema_validate(df)

print(shm)












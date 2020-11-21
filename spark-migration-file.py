from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import multiprocessing as mp
import logging 

logging.basicConfig(filename='spark.log', level=logging.DEBUG)

conf = SparkConf().setAll( [('spark.executor.cores', '5') , 
                            ('spark.cores.max', '8') , 
                            ('spark.dynamicAllocation.enabled', 'true') , 
                            ("spark.shuffle.service.enabled", "true") , 
                            ("spark.dynamicAllocation.maxExecutors", 20) ,
                            ("spark.dynamicAllocation.minExecutors", 10)]
                             )

# "spark.jars", "/home/mehdi/Downloads/spark-3.0.1-bin-hadoop2.7"]

sc = SparkContext(conf=conf)


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()


def oracle_to_mysql(table_name):
    try:
        # bound = "( select MIN(id) as min, MAX(id) as max from {} ) bound".format(table_name)
        bound = f"( select MIN(id) as min, MAX(id) as max from {table_name} ) bound"
        db_driver = 'oracle.jdbc.driver.OracleDriver'
        db_url = 'jdbc:oracle:thin:username/password@//host:1521/SID'
        partitions = mp.cpu_count() * 10
        conn_properties = {  'user': 'username',
                             'password': 'password',
                             'driver': 'oracle.jdbc.driver.OracleDriver'   }
        
        bound = spark.read.jdbc(
                url=db_url,
                table=bound,
                properties=conn_properties
            ).collect()[0]
        
        # df_table = "{}_DF".format(table_name)m
        df_table = f"{table_name}_DF"
        
        df_table =  spark.read.jdbc(
        url=db_url,
        # table="(select * from  {}) table_name".format(table_name),
        table=f"(select * from  {table_name}) table_name",
        numPartitions=partitions,
        column='id',
        lowerBound=bound.MIN,
        upperBound=bound.MAX + 1,  # upperBound is exclusive
        properties=conn_properties
        )
        

        df_table.write.format('jdbc').options(
        url="jdbc:mysql://host/db_name?useEncoding=true&characterEncoding=utf8&useLegacyDatetimeCode=false&serverTimezone=UTC&sessionVariables=sql_mode=''",
        driver='com.mysql.jdbc.Driver',
        dbtable=table_name,
        user='username',
        password='password').mode('append').save()
    except Exception as exc:
        logging.exception('Exception ===> ')



table_list = ["ITEM1","ITEM2",....,"ITEMN"]

for table in table_list:
    oracle_to_mysql(table)

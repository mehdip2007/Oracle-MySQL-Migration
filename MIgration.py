import cx_Oracle
import csv
import datetime as dt
import os
import re
import pymysql
import pandas as pd
import time


def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()

def OutConverter(value):
    if value is None:
        return ''
    return value

def OutputTypeHandler(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.DB_TYPE_CLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG, arraysize=cursor.arraysize, outconverter=OutConverter)
    if defaultType == cx_Oracle.DB_TYPE_BLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize , outconverter=OutConverter)
    if defaultType in (cx_Oracle.DB_TYPE_VARCHAR, cx_Oracle.DB_TYPE_CHAR , cx_Oracle.STRING):
        return cursor.var(str, size, cursor.arraysize, outconverter=OutConverter)
    
def initSession(connection, requestedTag):
    cursor = connection.cursor()
    cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD hh:mi:ss'")    


def parse_sql(filename):
    data = open(filename, 'r').readlines()
    stmts = []
    DELIMITER = ';'
    stmt = ''

    for lineno, line in enumerate(data):
        if not line.strip():
            continue

        if line.startswith('--'):
            continue

        if 'DELIMITER' in line:
            DELIMITER = line.split()[1]
            continue

        if (DELIMITER not in line):
            stmt += line.replace(DELIMITER, ';')
            continue

        if stmt:
            stmt += line
            stmts.append(stmt.strip())
            stmt = ''
        else:
            stmts.append(line.strip())
    return stmts





# Global DB Variables
# ORACLE Database
ORACLE_USER="user"
ORACLE_PASSWORD="pass"
ORACLE_DB_NAME="sid"
ORACLE_HOST="host"
ORACLE_STR_HOST_DB=ORACLE_HOST+"/"+ORACLE_DB_NAME

# MySQL Database
MySQL_USER="user"
MySQL_PASSWORD="pass"
MySQL_DB_NAME="db"
MySQL_HOST="host"


table_list=["ITEM1","ITEM2", ...... ,"ITEMN"]

# Get Current directory 
dir_path = os.path.dirname(os.path.realpath('__file__'))
# print(dir_path)


def get_oracle_tables():
    dsn_tns = cx_Oracle.makedsn(ORACLE_HOST, '1521', ORACLE_DB_NAME)
    connection = cx_Oracle.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=dsn_tns,encoding="UTF-8")
    cursor = connection.cursor()
    cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD hh:mi:ss'")
    cursor.execute("""
        BEGIN
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',false);
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'PRETTY',true);
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SQLTERMINATOR',true);
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SEGMENT_ATTRIBUTES',false);
        END;
        """)

    for each in table_list:
        try:
            clob = cursor.callfunc("dbms_metadata.get_ddl", cx_Oracle.CLOB,("TABLE", each))
            clob_lower = str(clob.read())
            clob_lower = clob_lower.lower()
            with open(each + "_table_structure.sql", 'w') as sqlfile:
                sqlfile.write(clob_lower)
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if error.code == 31603:
                print("Table " + each + " Doesn't Exists in the Schema")
            continue



def mysql_formatter():
    count = 0
    for each in table_list:
        try:
            with open(each + "_table_structure.sql", 'r+') as read_sql:
                modified_body =''
                for line in read_sql:
                    count += 1
                    MySQL_formatter = line.strip()
                    MySQL_formatter = MySQL_formatter.replace(str(ORACLE_USER), str(MySQL_DB_NAME)) \
                          .replace('"', "`").replace(' char)',')') \
                         .replace('number(','numeric(').replace('varchar2','varchar') \
                    .replace('clob','longtext').replace('using index  enable','').replace('date,','datetime,')
                    if "`id`" in MySQL_formatter  :
                        MySQL_formatter = MySQL_formatter.replace('not null enable','not null auto_increment') \
                            .replace('numeric','bigint')
                    MySQL_formatter = re.sub(r'float\(.*\)', 'FLOAT', MySQL_formatter)
                    MySQL_formatter = re.sub(r'\,.*\)', ')', MySQL_formatter)
                    modified_body += MySQL_formatter + '\n'
                read_sql.seek(0)
                read_sql.write(modified_body)
                read_sql.truncate()
        except FileNotFoundError:
            print("File " + each + " is not Available.")
            continue



def import_mysql_table():
    sqlmode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' 

    # Open database connection
    MySQLdb = pymysql.connect(MySQL_HOST,MySQL_USER,MySQL_PASSWORD,MySQL_DB_NAME ,sql_mode=sqlmode, local_infile=True)  

    # prepare a cursor object using cursor() method
    cursor = MySQLdb.cursor()  

    # execute SQL query using execute() method.
    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
    
    for each in table_list:
        try:
            stmts = parse_sql(each + '_table_structure.sql')
            with MySQLdb.cursor() as cursor:
                for stmt in stmts:
                    cursor.execute(stmt)
                MySQLdb.commit()
        except FileNotFoundError:
            print("File " + each + " is not Available.")
            continue
        except Exception as e: 
            print(e)
            continue

    # disconnect from server
    MySQLdb.close()


"""
##################################################################################################
########### i use dataframe just to be sure the data is in the correct format ####################
## if you wanted to have better perfromance use may use csv writer part in the below section #####
##################################################################################################

    for lowercase_table in lowercase_tables:
        with open(lowercase_table + ".csv", 'w', newline='') as csvfile:
            ora_query = 'select * from ' + lowercase_table.upper()
            try:
                for row in cursor.execute(ora_query):
                    try:
                        csvwriter = csv.writer(csvfile, delimiter='~', quotechar='^' , quoting=csv.QUOTE_NONNUMERIC, lineterminator='\n')
                        csvwriter.writerow(row)
                    except cx_Oracle.DatabaseError as exc:
                        error, = exc.args
                        # print("Oracle-Error-Message: ", error.code , "Oracle-Error-Message:", error.message)
                        if error.code == 31603 or error.code == 942:
                            print("table " + table + " doesn't exists in the schema")
                            continue
            except:
                pass
""""




def fetch_data_oracle():
    
    Start_Date=dt.datetime.now()
    print("Start Fetching Data: ",Start_Date)
    
    # Create the session pool
    pool = cx_Oracle.SessionPool(ORACLE_USER, ORACLE_PASSWORD, ORACLE_STR_HOST_DB,min=2, max=5, increment=1,sessionCallback=initSession,  encoding="UTF-8")
    # Acquire a connection from the pool
    connection = pool.acquire()
    # Create query for each item in table list
    lowercase_tables = [lowercase_table.lower() for lowercase_table in table_list]
    cursor = connection.cursor()
    cursor.outputtypehandler = OutputTypeHandler
    for lowercase_table in lowercase_tables:
    
        ora_query = """ select * from """ + lowercase_table.upper()
        count_stmt = """ select count(*) from """ + lowercase_table.upper()
        numRows = 400000
        df_res = pd.DataFrame() 
        try:
            rowCounts = cursor.execute(count_stmt)
            for rowCnt in rowCounts:
                print("the count for table {} {}".format(lowercase_table,rowCnt))
            cursor.execute(ora_query)
            headers = [i[0] for i in cursor.description]
            while True:
                rows = cursor.fetchmany(numRows)
                if not rows:
                    break
                for i, row in enumerate(rows):
                    printProgressBar(i, len(rows), prefix='Progress', suffix='Completed', length=50)
                    list_rows = list( row )
                    df = pd.DataFrame([list_rows])
                    df_res = df_res.append(df)
            print('********************')
            try:
                df_res.to_csv( lowercase_table + ".csv" , index=False, header=headers , sep="~", quotechar='#', quoting=csv.QUOTE_NONNUMERIC, escapechar="\\" ,line_terminator='\n')
            except Exception as e: 
                print(lowercase_table,e)
                pass
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if error.code == 31603 or error.code == 942:
                print("table " + lowercase_table + " doesn't exists in the schema")
                pass

    # Release the connection to the pool
    pool.release(connection)
    # Close the pool
    pool.close()


    End_Date=dt.datetime.now()
    print("Finished Fetching Data:      ",End_Date)


def import_to_mysql():
   # sqlmode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' 
    sqlmode = ''
    MySQLdb = pymysql.connect(MySQL_HOST,MySQL_USER,MySQL_PASSWORD,MySQL_DB_NAME ,sql_mode=sqlmode, local_infile=True, autocommit=True, charset='utf8mb4') 
    MySQL_cursor = MySQLdb.cursor()
    MySQL_cursor.execute("SET FOREIGN_KEY_CHECKS=0;")

    dsn_tns = cx_Oracle.makedsn(ORACLE_HOST, '1521', ORACLE_DB_NAME)
    connection = cx_Oracle.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=dsn_tns,encoding="UTF-8")
    ora_cursor = connection.cursor()
    for table in table_list:
        print("Importing Data for Table {} ".format(table))
        try: 
            SQL = 'select * from ' + table + ' where rownum = 1' 
            ora_cursor.execute(SQL) 
            desc = ora_cursor.description
            headers = [i[0] for i in desc] 
            headers = tuple(headers) 
            MySQL_query = "LOAD DATA LOCAL INFILE '" + dir_path + '/' + table.lower() + ".csv'"  + \
                    " INTO TABLE " + table.lower() + " FIELDS TERMINATED BY '~' ENCLOSED BY '#' ESCAPED BY '\\\\'   IGNORE 1 ROWS " + str(headers).replace("'","")  
            MySQL_cursor.execute(MySQL_query)
        except Exception as exec: 
            print(exec, table)


print("Getting tables from Oracle..")
get_oracle_tables()
print("Formatting for MySQL..")
mysql_formatter()
print("Importing Data to MySQL..")
import_mysql_table()
print("Getting Data from Oracle..")
fetch_data_oracle()
print("Import Data to MySQL..")
import_to_mysql()




# Oracle-MySQL-Migration
Script for migrating from Oracle Database to MySQL database


<p>With the loss of the Oracle Standard Edition One license, and the free Oracle Express Edition database stuck at the old 11g Release 2 from 2014, many smaller projects are considering whether MySQL might be an alternative.</p>

<p>If you are building your application from scratch, most projects can start with any relational database. But if you have an existing database, you need to investigate carefully if it makes sense to move.</p>

<p>Databases, schemas, and users also work differently. In Oracle, you have one database with many schemas that are also users. In MySQL, the word DATABASE is synonymous with SCHEMA and is decoupled from users. So users need to be granted access to schemas/databases.</p>

**Data Types**

The data types are different between Oracle and MySQL, but can generally be mapped easily.

Oracle   < ----- >   MySQL

VARCHAR2 < ----- > VARCHAR

NUMBER  < ----- >  NUMERIC

DATE < ----- > DATETIME

**Tables**

To create these tables in MySQL, you need to remove the double quotes and change the datatypes. *SYSDATE* is an Oracle-specific function that you need to replace with the equivalent *CURRENT_TIMESTAMP*


**Data**
SQL*Plus syntax *(REM and SET)* that you need to remove. MySQL doesn’t have a *TO_DATE* function, but accepts *DATETIME* values directly as quoted strings in ISO standard format

so you can use the attached script shell scripts to make your life easy.

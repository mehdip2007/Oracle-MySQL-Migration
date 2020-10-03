#!/bin/bash
#!/usr/bin/awk -f

export ORACLE_HOME=/usr/lib/oracle/19.8/client64
export LD_LIBRARY_PATH=$ORACLE_HOME/lib:/lib:/usr/lib
export PATH=/home/mehdi/Downloads/jdk1.8.0_261/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin
# export NLS_LANG=american_america.AR8MSWIN1256
export NLS_LANG=AMERICAN_AMERICA.UTF8

export gawk=/usr/bin/gawk
export sed=/bin/sed
export mysql=/usr/bin/mysql



# Declare an array of string with type


StringArray=("ITEM_1" "ITEM_2" "ITEM_N")
 
# Iterate the string array using for loop
for val in "${StringArray[@]}"; do
      # echo  "select dbms_metadata.get_ddl('TABLE','"$val"','ICARE_BI') FROM  dual;"
      sqlplus -s  username/password@DBNAME << Prak

			SET LONG 20000 LONGCHUNKSIZE 20000 PAGESIZE 0 LINESIZE 1000 FEEDBACK OFF VERIFY OFF TRIMSPOOL ON

			BEGIN
			   DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'SQLTERMINATOR', true);
			   DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'PRETTY', true);
			END;
			/

			spool $val.sql

			SELECT DBMS_METADATA.get_ddl ('TABLE', table_name, owner)
			FROM   all_tables
			WHERE  owner      = UPPER('schema_name')
			AND    table_name = DECODE(UPPER('$val'), 'ALL', table_name, UPPER('$val'));

			SET PAGESIZE 14 LINESIZE 100 FEEDBACK ON VERIFY ON

			spool off;

			exit;

Prak
done

###### since MySQL has a diffrent engine(technology) for managing RDMBS, first need to make format readable according to MySQL.

for each in "${StringArray[@]}"; do

	# to remove all the double quotes
	 sed  -i 's/"//g' $each".sql"

	# change all variables :
	 sed -i  's/VARCHAR2/VARCHAR/' $each".sql"
	 sed -i  's/255 CHAR/255/' $each".sql"
	 sed -i  's/,0)/)/g' $each".sql"
	 sed -i  's/NOT NULL ENABLE/NOT NULL AUTO_INCREMENT/' $each".sql"
	 sed -i  's/NUMBER/NUMERIC/' $each".sql"
	 sed -i  's/ID NUMERIC/ID INT/' $each".sql"
	 	 
	# clean the last line format for importing to mysql
	 # sed -i '/PRIMARY KEY/,$d'  $each".sql"
	 sed -i '/PRIMARY KEY/q'  $each".sql"
	 sed -i '$s/(ID)/(id)\n);/g' $each".sql"


	 ##################################################################################
	 ######		 			awk is not running on my shell script				#######
	 ##################################################################################
	 # sed -i '/PRIMARY KEY /q' $each".sql"
	 # gawk -f /home/mehdi/migration/oracle-tables/1/program.awk $each".sql"
	 # awk -i inplace 'BEGIN {RS=""}{ gsub(/,\n[ \t]+PRIMARY KEY \("ID"\)/, "\n);");}1' $each".sql"
	 # replacer=$(awk -i inplace 'BEGIN {RS=""}{ gsub(/,\n[ \t]+PRIMARY KEY \("ID"\)/, "\n);");}1') $each".sql"
	 # awk  $each".sql" -i inplace -v patt='BEGIN {RS=""}{ gsub(/,\n[ \t]+PRIMARY KEY \("ID"\)/, "\n);");}1' 
	  
	 
  	 # Converts upper to lower case
	 sed -i 's/\(.*\)/\L\1/' $each".sql"

	 # change schema  name
	 sed -i 's/OLD_SCHEMA/NEW schema_name/' $each".sql"   ##### you may change your schema name if it is different

done

for i in "${StringArray[@]}"; do

	sudo mysql --local-infile=1 -h HOSTNAME -u USERNAME -pPASSWORD < $i".sql"
	
done
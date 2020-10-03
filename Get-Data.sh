#!/bin/bash
#!/usr/bin/awk -f

####### Below is my configuration for ORACLE make sure to chage it according to yout environment.
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
      		set echo off;

			spool ${val}_data.sql

			desc $val

			spool off;

			exit;

Prak
done



for each in "${StringArray[@]}"; do

 	tmpfile=$(mktemp)
 	cat ${each}_data.sql | awk '{print $1}' &> ${tmpfile}
 	cat ${tmpfile} > ${each}_data.sql
 	rm -f ${tmpfile}

	# Converts upper to lower case
 	# sed -i 's/\(.*\)/\L\1/' ${each}_data.sql

 	sed -i  '1,2d' ${each}_data.sql 

 	# remove empty lines 
	sed -i '/^$/d' ${each}_data.sql 
	
	# Starighten the columns
	sed -z -i  's/\n/,/g;s/,$/\n/'  ${each}_data.sql 

	column_string=`sed "s/\,/||','||/g" ${each}_data.sql`
	# echo $column_string

	# creating the query
	sed -i  "1s/^/SELECT '/" ${each}_data.sql 

	sed -i  "s/$/' FROM DUAL UNION ALL SELECT /" ${each}_data.sql
	echo ${column_string} from $each\; >> ${each}_data.sql

	# connect to oralce to fetch data to file
	sqlplus -s  username/password@DBNAME >> ${each}.csv << Prak
      		
      		set pagesize 0
			set linesize 1000
			set echo off
			set heading off
			set timing off
			set feedback off
			set serveroutput on

			@${each}_data.sql

			exit;


		
Prak
done




########################################################################################
####################   for block comment put your text between two END  ################
# : <<'END'
# END
#######################################################################################

	# mysql_tablename=`sed 's/\(.*\)/\L\1/' ${j}`

for j in "${StringArray[@]}"; do
	mysql_tablename=`echo ${j} |awk '{print tolower($0)}'`
	sudo mysql --local-infile=1 -h Host -u username -ppassword << Prak
		
	use DBANAME;

	SELECT DATABASE();

	LOAD DATA LOCAL INFILE '${j}.csv' INTO TABLE DBNAME(schema_name).${mysql_tablename} FIELDS TERMINATED BY ',' IGNORE 1 ROWS;

	exit

Prak
done



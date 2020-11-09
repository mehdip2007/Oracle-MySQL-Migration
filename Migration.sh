#!/bin/bash
#!/usr/bin/awk -f
export ORACLE_HOME=/oracle/product/11.2.0/client_1/
export PATH=$PATH:$ORACLE_HOME/bin
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME/lib:/oracle/product/11.2.0/client_1/lib32
PATH=$PATH:$HOME/.local/bin:$HOME/bin
export PATH="$HOME/.rbenv/plugins/ruby-build/bin:/data/.rbenv/bin:/data/.rbenv/versions/2.4.0/bin:$PATH"
export NLS_LANG=AMERICAN_AMERICA.UTF8
export awk=/usr/bin/awk
export sed=/usr/bin/sed
export mysql=/usr/bin/mysql



Start=`date +'%m-%d-%Y %T'`
echo  " Start Time :  >" $Start " <------------ "
dir=$(pwd)

##### since mysql have table case sensivity i have separated the tables. and you can use capital the the transform will be done.

# LOWER CASE TABLES
declare -a lowerCase_TablesArray=("ITEM1", "ITEM2", ....  , "ITEMN")


# UPPER CASE TABLES
# declare -a UpperCase_TablesArray=("ITEM1", "ITEM2", ....  , "ITEMN")

#ALL TABLES
declare -a All_Tables=( "${UpperCase_TablesArray[@]}" "${lowerCase_TablesArray[@]}" )


###### Databases Variables
ORACLE_USER=user
ORACLE_PASSWORD=pass
ORACLE_DB_NAME=sid

MySQL_USER=user 
MySQL_PASSWORD=pass
MySQL_DB_NAME=db
MySQL_HOST=host



Prepare_Lower(){
	for val in "${lowerCase_TablesArray[@]}"; do
		l_myArr=`echo "$val" | sed 's/\(.*\)/\L\1/'`
		echo "------------------ Make the format ready for MySQL $l_myArr" 
		echo "------------------ fetching table structure ${l_myArr}  "

	      sqlplus -s  ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_DB_NAME} << Prak

	      alter session set nls_date_format = 'YYYY-MM-DD hh:mi:ss';
	      
	      		SET TERMOUT OFF
				SET LONG 20000 LONGCHUNKSIZE 20000 PAGESIZE 0 LINESIZE 1000 FEEDBACK OFF VERIFY OFF TRIMSPOOL ON
				BEGIN
					DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',false);
					DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'PRETTY',true);
					DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SQLTERMINATOR',true);
					DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SEGMENT_ATTRIBUTES',false);
				END;
				/
				spool $l_myArr.sql

				SELECT DBMS_METADATA.get_ddl ('TABLE', table_name, owner)
				FROM   all_tables
				WHERE  owner      = UPPER('icare_bi')
				AND    table_name = DECODE(UPPER('$val'), 'ALL', table_name, UPPER('$val'));

				SET PAGESIZE 14 LINESIZE 100 FEEDBACK ON VERIFY ON

				spool off;

				exit;

Prak
	
	echo "------------------ Getting columns finished for Lower Case tables.  "
	echo "------------------ Make the format ready for MySQL $l_myArr" 
	##################################################################
	###########	Make the format ready for MySQL ######################
	################################################################## 
	tmpfile=$(mktemp)
	sed 's/"/`/g' ${l_myArr}".sql" |sed  's/VARCHAR2/VARCHAR/' |sed 's/255 CHAR/255/' |sed 's/NOT NULL ENABLE/NOT NULL AUTO_INCREMENT/' | sed 's/,0)/)/g' |sed 's/NUMBER/NUMERIC/g' | sed '/ID/ s/NUMERIC/BIGINT/' | sed "s/FLOAT[(][^)]*[)]/FLOAT/g" |sed 's/CLOB/LONGTEXT/g' | sed 's/LONG RAW/LONGBLOB/g' | sed 's/BLOB/LONGBLOB/g' | sed 's/TIMESTAMP[(][^)]*[)]/DATETIME/g' | sed 's/CLOB/LONGTEXT/g' | sed 's/LONG RAW/LONGBLOB/g'| sed 's/BLOB/LONGBLOB/g' |sed '/TIMESTAMP/ s/AUTO_INCREMENT//' | sed 's/TIMESTAMP[(][^)]*[)]/DATETIME/g'| sed 's/ CHAR)/)/g'|sed '/VARCHAR/ s/AUTO_INCREMENT//' | sed '/USING INDEX  ENABLE/ s/USING INDEX  ENABLE//'|sed '/LONGTEXT/ s/AUTO_INCREMENT//'| sed '/SEQ/ s/NOT NULL AUTO_INCREMENT//'|sed '/PORT/ s/NOT NULL AUTO_INCREMENT//'|sed '/ID/! s/AUTO_INCREMENT//' |sed "/_ID/ s/AUTO_INCREMENT//" | sed 's/\(.*\)/\L\1/' | sed 's/icare_bi/icaredciat/g'| sed 's/longlongblob/longtext/g' |sed 's/varchar(4000)/text/g'|sed 's/enable//'| sed '/primary key/ s/)/),/' | sed  's/primary key(`id`)/primary key(`id`),/'| sed 's/primary key (`id`),/primary key (`id`)/'| sed 's/longlongtext/longtext/g' &> ${tmpfile}
	echo > ${l_myArr}".sql" 
	cat ${tmpfile} > ${l_myArr}".sql"
	rm -f ${tmpfile}


	if [[ ${l_myArr} == 'boardproject' ]]; then
		sed -i 's/`project_id`),/`project_id`)/'  ${l_myArr}".sql" 
	fi

	if [[ ${l_myArr} == 'clusternode' ]]; then
		sed -i 's/(`node_id`),/(`node_id`)/' ${l_myArr}".sql" 
	fi

	if [[ ${l_myArr} == 'clusternodeheartbeat' ]]; then
		sed -i  's/(`node_id`),/(`node_id`)/' ${l_myArr}".sql"
	fi

	if [[ ${l_myArr} == 'cwd_application_address' ]]; then
		sed -i  's/`remote_address`),/`remote_address`)/' ${l_myArr}".sql"
	fi

	if [[ ${l_myArr} == 'cwd_directory_attribute' ]]; then
		sed -i 's/`attribute_name`),/`attribute_name`)/' ${l_myArr}".sql"
	fi

	if [[ ${l_myArr} == 'cwd_directory_operation' ]]; then
		sed -i  's/`operation_type`),/`operation_type`)/'  ${l_myArr}".sql"
	fi

	if [[ ${l_myArr} == 'nodeassociation' ]]; then
		sed -i 's/`association_type`),/`association_type`)/' ${l_myArr}".sql"
	fi

	if [[ ${l_myArr} == 'pluginstate' ]]; then
		sed -i 's/(`pluginkey`),/(`pluginkey`)/' ${l_myArr}".sql"
	fi

	if [[ ${l_myArr} == 'projectchangedtime' ]]; then
		sed -i 's/(`project_id`),/(`project_id`)/' ${l_myArr}".sql"
	fi

	if [[ ${l_myArr} == 'qrtz_calendars' ]]; then
		sed -i 's/(`calendar_name`),/(`calendar_name`)/' ${l_myArr}".sql"
	fi

	if [[ ${l_myArr} == 'qrtz_fired_triggers' ]]; then
		sed -i 's/(`entry_id`),/(`entry_id`)/' ${l_myArr}".sql"
	fi

	if [[ ${l_myArr} == 'tempattachmentsmonitor' ]]; then
		sed -i 's/(`temporary_attachment_id`),/(`temporary_attachment_id`)/' ${l_myArr}".sql"
	fi

	if [[ ${l_myArr} == 'upgradehistory' ]]; then
		sed -i  's/(`upgradeclass`),/(`upgradeclass`)/' ${l_myArr}".sql"
	fi

	if [[ ${l_myArr} == 'upgradeversionhistory' ]]; then
		sed -i 's/(`targetbuild`),/(`targetbuild`)/' ${l_myArr}".sql"
	fi

	if [[ ${l_myArr} == 'userassociation' ]]; then
		sed -i 's/association_type`),/association_type`)/' ${l_myArr}".sql"
	fi

	echo "------------------ Formatting For Lower Case Tables is Done.  "
done
}



Prepare_Upper(){
	for val in "${UpperCase_TablesArray[@]}"; do

		echo "------------------ fetching table structure ${val}  "

      	sqlplus -s  ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_DB_NAME} << Prak

      		alter session set nls_date_format = 'YYYY-MM-DD hh:mi:ss';
      
      				SET TERMOUT OFF
					SET LONG 20000 LONGCHUNKSIZE 20000 PAGESIZE 0 LINESIZE 1000 FEEDBACK OFF VERIFY OFF TRIMSPOOL ON
					BEGIN
						DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',false);
						DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'PRETTY',true);
						DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SQLTERMINATOR',true);
						DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SEGMENT_ATTRIBUTES',false);
					END;
					/
					spool $val.sql

					SELECT DBMS_METADATA.get_ddl ('TABLE', table_name, owner)
					FROM   all_tables
					WHERE  owner      = UPPER('icare_bi')
					AND    table_name = DECODE(UPPER('$val'), 'ALL', table_name, UPPER('$val'));

					SET PAGESIZE 14 LINESIZE 100 FEEDBACK ON VERIFY ON

					spool off;

					exit;

Prak
	echo "------------------ Getting columns finished for all Upper Case tables.  "
	echo "------------------ Make the format ready for MySQL  ${val}"
	##################################################################
	###########	Make the format ready for MySQL ######################
	##################################################################
	tmpfile=$(mktemp)
	sed 's/"/`/g' ${val}".sql" |sed  's/VARCHAR2/VARCHAR/' |sed 's/255 CHAR/255/' |sed 's/NOT NULL ENABLE/NOT NULL AUTO_INCREMENT/' | sed 's/,0)/)/g' |sed 's/NUMBER/NUMERIC/g' | sed '/ID/ s/NUMERIC/BIGINT/' | sed "s/FLOAT[(][^)]*[)]/FLOAT/g" |sed 's/CLOB/LONGTEXT/g' | sed 's/LONG RAW/LONGBLOB/g' | sed 's/BLOB/LONGBLOB/g' | sed 's/TIMESTAMP[(][^)]*[)]/DATETIME/g' | sed 's/CLOB/LONGTEXT/g' | sed 's/LONG RAW/LONGBLOB/g'| sed 's/BLOB/LONGBLOB/g'| sed '/TIMESTAMP/ s/AUTO_INCREMENT//'|sed '/LONGTEXT/ s/AUTO_INCREMENT//' | sed 's/TIMESTAMP[(][^)]*[)]/DATETIME/g'| sed 's/ CHAR)/)/g'|sed '/VARCHAR/ s/AUTO_INCREMENT//' | sed '/USING INDEX  ENABLE/ s/USING INDEX  ENABLE//'| sed '/SEQ/ s/NOT NULL AUTO_INCREMENT//'|sed '/PORT/ s/NOT NULL AUTO_INCREMENT//' | sed 's/ICARE_BI/icaredciat/g'| sed 's/VARCHAR(4000)/TEXT/g'|sed 's/ENABLE//'| sed '/PRIMARY KEY/ s/)/),/'| sed  's/PRIMARY KEY(`ID`)/PRIMARY KEY(`ID`),/' |sed '/CONSTRAINT/ s/),/)/' |sed "/_ID/ s/AUTO_INCREMENT//" |sed 's/PRIMARY KEY (`ID`),/PRIMARY KEY (`ID`)/' |sed '/ID/! s/AUTO_INCREMENT//' |sed 's/LONGLONGBLOB/LONGTEXT/g' &> ${tmpfile}
	echo > ${val}".sql"
	cat ${tmpfile} > ${val}".sql"
	rm -f ${tmpfile}


	if [[ ${val} == 'AO_563AEE_ACTIVITY_ENTITY' ]]; then
		sed -i 's/PRIMARY KEY (`ACTIVITY_ID`),/PRIMARY KEY (`ACTIVITY_ID`)/' ${val}".sql"
	fi

	if [[ ${val} == 'AO_587B34_PROJECT_CONFIG' ]]; then
		sed -i '/`NAME_UNIQUE_CONSTRAINT`/ s/VARCHAR(255)/VARCHAR(255),/' ${val}".sql"
	fi

	if [[ ${val} == 'AO_B9C0BA_NAVIGATION_HIDE' ]]; then
		sed -i '/HIDE_ENTRY_POSITION/ s/AUTO_INCREMENT//' ${val}".sql"
	fi

	if [[ ${val} == 'AO_E2B9A5_TABLE_CONFIG' ]]; then
		sed -i 's/PRIMARY KEY (`TABLE_CONFIG_ID`),/PRIMARY KEY (`TABLE_CONFIG_ID`)/' ${val}".sql"
	fi

	if [[ ${val} == 'AO_60DB71_WORKINGDAYS' ]]; then
		sed -i '/FRIDAY/ s/AUTO_INCREMENT//' ${val}".sql"
		sed -i 's/`ID` BIGINT(20)/`ID` BIGINT(20) AUTO_INCREMENT/'  ${val}".sql"
	fi

	echo "------------------ Formatting Done.  "
		
done
}


MySQL_Import_Table_lower(){

	for i in "${lowerCase_TablesArray[@]}"; do
	l_myArr=`echo "$i" | sed 's/\(.*\)/\L\1/'`
	echo "------------------ importing Data to MySQL ${l_myArr} " 
	mysql --local-infile=1 -h ${MySQL_HOST} -u ${MySQL_USER} -p${MySQL_PASSWORD} -D ${MySQL_DB_NAME} << Prak
	use ${MySQL_DB_NAME}; 
	set sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';
	SET FOREIGN_KEY_CHECKS=0;

	source $dir/$l_myArr.sql 

Prak
done	

}


MySQL_Import_Table_Upper(){

	for i in "${UpperCase_TablesArray[@]}"; do
	echo "------------------ importing Data to MySQL ${i} " 
	mysql --local-infile=1 -h ${MySQL_HOST} -u ${MySQL_USER} -p${MySQL_PASSWORD} -D ${MySQL_DB_NAME} << Prak
	use ${MySQL_DB_NAME}; 
	set sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';
	SET FOREIGN_KEY_CHECKS=0;

	source $dir/$i.sql 

Prak
done

}


Get_Import_Data_Upper(){
	for val in "${UpperCase_TablesArray[@]}"; do
      echo "------------------ fetching table columns ${val}  "
      
      sqlplus -s  ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_DB_NAME} << Prak
      alter session set nls_date_format = 'YYYY-MM-DD hh:mi:ss';
      		SET TERMOUT OFF
      		set echo off;

			spool ${val}_data.sql

			desc $val

			spool off;

			exit;

Prak
	echo "------------------  getting columns finished for tables ${val}.  "

	echo "------------------  making data ready for ${val}  "

 	tmpfile=$(mktemp)
 	cat ${val}_data.sql | awk '{print $1}' &> ${tmpfile}
 	cat ${tmpfile} > ${val}_data.sql
 	rm -f ${tmpfile}

 	sed -i  '1,2d' ${val}_data.sql 
 	# remove empty lines 
	sed -i '/^$/d' ${val}_data.sql 
	# Starighten the columns
	sed -z -i  's/\n/,/g;s/,$/\n/'  ${val}_data.sql 
	column_string=`sed "s/\,/||'~'||/g" ${val}_data.sql`
	# echo $column_string
	echo "----------------------  creating the query ${val}  "
	# creating the query
	sed -i  "1s/^/SELECT '/" ${val}_data.sql 
	sed -i  "s/$/' FROM DUAL UNION ALL SELECT /" ${val}_data.sql
	echo ${column_string} from $val\; >> ${val}_data.sql

	sqlplus -s  ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_DB_NAME} >> ${val}.csv << Prak
	alter session set nls_date_format = 'YYYY-MM-DD hh:mi:ss';
      		
			set pagesize 0
			set linesize 1000
			set echo off
			set heading off
			set timing off
			set feedback off
			set serveroutput on

			@${val}_data.sql

			exit;

	
Prak
	chmod 666 ${val}.csv
	sed -i '1,3d' ${val}.csv

	echo "------------------  Importing data to table ${j} ....  "

	# mysql_tablename=`echo ${val} |awk '{print tolower($0)}'`
	mysql --local-infile=1 -h ${MySQL_HOST} -u ${MySQL_USER} -p${MySQL_PASSWORD} -D ${MySQL_DB_NAME} << Prak
    
    use icaredciat; 
	set sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';
	SET FOREIGN_KEY_CHECKS=0;

	LOAD DATA LOCAL INFILE '${val}.csv' INTO TABLE icaredciat.${val} FIELDS TERMINATED BY '~' IGNORE 1 ROWS;

	exit

Prak

done

}

Get_Import_Data_Lower(){
	for val in "${lowerCase_TablesArray[@]}"; do
	l_myArr=`echo "$val" | sed 's/\(.*\)/\L\1/'`
      echo "------------------ fetching table columns ${l_myArr}  "
      
      sqlplus -s  ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_DB_NAME} << Prak
      alter session set nls_date_format = 'YYYY-MM-DD hh:mi:ss';
      		SET TERMOUT OFF
      		set echo off;

			spool ${l_myArr}_data.sql

			desc $val

			spool off;

			exit;

Prak
	
	echo "------------------ Make the format ready for MySQL $l_myArr" 
	echo "------------------  getting columns finished for tables ${l_myArr}.  "

	echo "------------------  making data ready for ${l_myArr}  "

 	tmpfile=$(mktemp)
 	cat ${l_myArr}_data.sql | awk '{print $1}' &> ${tmpfile}
 	cat ${tmpfile} > ${l_myArr}_data.sql
 	rm -f ${tmpfile}

 	sed -i  '1,2d' ${l_myArr}_data.sql 
 	# remove empty lines 
	sed -i '/^$/d' ${l_myArr}_data.sql 
	# Starighten the columns
	# sed -z -i  's/\n/,/g;s/,$/\n/'  ${l_myArr}_data.sql 
	column_string=`sed "s/\,/||'"~"'||/g" ${l_myArr}_data.sql`
	# echo $column_string
	echo "----------------------  creating the query ${l_myArr}  "
	# creating the query
	# sed -i  "1s/^/SELECT '/" ${l_myArr}_data.sql 
	# sed -i  "s/$/' FROM DUAL UNION ALL SELECT /" ${l_myArr}_data.sql
	echo ${column_string} from $val\; >> ${l_myArr}_data.sql

	sqlplus -s  ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_DB_NAME} >> ${l_myArr}.csv << Prak
	alter session set nls_date_format = 'YYYY-MM-DD hh:mi:ss';
      		
		SET FEEDBACK OFF 
		SET WRAP ON
		SET TERMOUT OFF
		SET VERIFY OFF
		SET PAGES 0
		set numwidth 20
		SET LONG 200000
		set pagesize 0
		set linesize 500
		set echo off
		set heading off
		set timing off
		set feedback off
		set serveroutput on
		set trimspool on
		set trim on
		SET LONGCHUNKSIZE 999999;
		set arraysize 5000
		set linesize 32767 long 2000000000 longchunksize 32767 PAGESIZE 0 FEEDBACK OFF ECHO OFF TERMOUT OFF

			@${l_myArr}_data.sql

			exit;

	
Prak
	chmod 666 ${l_myArr}.csv
	sed -i '1,3d' ${l_myArr}.csv

	echo "------------------  Importing data to table ${l_myArr} ....  "

	mysql_tablename=`echo ${val} |awk '{print tolower($0)}'`
	mysql --local-infile=1 -h ${MySQL_HOST} -u ${MySQL_USER} -p${MySQL_PASSWORD} -D ${MySQL_DB_NAME} << Prak
    
    use ${MySQL_DB_NAME}; 
	set sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';
	SET FOREIGN_KEY_CHECKS=0;

	LOAD DATA LOCAL INFILE '${l_myArr}.csv' INTO TABLE icaredciat.${mysql_tablename} FIELDS TERMINATED BY '~' IGNORE 1 ROWS;

	exit

Prak

done

}




echo "Preparing data for Lower Case Tables..."
Prepare_Lower
sleep 5
echo "Preparing data for Upper Case Tables..."
Prepare_Upper
sleep 5
echo "Importing Tables to MySQL Database...."
MySQL_Import_Table
sleep 5
echo "Get & Import Data for Upper Tables...."
Get_Import_Data_Upper
sleep 5
echo "Get & Import Data for Lower Tables...."
Get_Import_Data_Lower

sed -i 's/`//g' $dir/update-to-null.sql 

End=`date +'%m-%d-%Y %T'`
echo  " End Time :  >" $End " <------------ "


# ps -ef | grep -i "migration.sh"| grep -v grep | awk '{print $2}' |xargs kill -9

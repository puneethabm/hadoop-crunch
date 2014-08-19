#!/bin/bash

#######################################################
#################Iterate through HDFS Direcctory#################
#######################################################

function main(){

CURRENT_DIR="hadoop fs -ls /user/puneetha/inputdir"
DIR_COUNT=`$CURRENT_DIR | wc -l`

DB_NAME="default"
TABLE_NAME="sample_table1"

#Iterate through the HDFS directory
i=0
while read line
do
 array[ $i ]="$line"
 (( i++ ))
done < <($CURRENT_DIR | awk '{print $8}')

for (( i=$DIR_COUNT-1; i>0; i-- ))
 do
 BASE_NAME=`basename ${array[$i]}`
 DIR_NAME=`dirname ${array[$i]}`

#Ignore directory/filename which are unwanted 
 if [ "$BASE_NAME" != ".Trash" ] && [ "$BASE_NAME" != ".staging" ] ; then
  #FILE_NAME contains the filename inside the directory we specify Ex:/user/puneetha/inputdir
  FILE_NAME=$BASE_NAME
  
  #Perform any operation here
  #Ex: Below command creates hive partitions taking the name of the directory
  hive --database $DB_NAME -e "ALTER TABLE $TABLE_NAME ADD PARTITION(PDATE='$FILE_NAME') LOCATION '/user/puneetha/inputdir/$FILE_NAME/'";
 
  #Add other functionalities here


 fi
done

}

main $*

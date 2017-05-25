#!/bin/bash
while read line
do
	link_name=`echo ${line}|sed 's/\//_/g'`
	echo "ln -s  "${line}" "${link_name}  
	ln -s ${line} ${link_name}
done < ./nemo-cppfile.lst


#!/bin/bash
year=$1
if ! [ -d $year ]; then
    mkdir $year
fi
curr=$(pwd)
cd $year
months=$(ls | awk -F "-" {'print $2'} | sort | uniq)
echo $months
for month in ${months}; do
    echo "processing $month";
    if ! [ -d $month ]; then
        mkdir $month
    fi
    echo "moving all days for the month in the month folder $year/$month"
    mv $year-$month-* $month/
    cd $month;
    find . -type d -iname "$year*" | while read l; do mv $l ./$(echo $l | awk -F "-" {'print $3'}); done
    cd -;
done
cd $curr

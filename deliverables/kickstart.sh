#!/usr/bin/env bash
sh ./killall.sh
echo "sleeping"
sleep 7
echo " Start Coordinator"
totalservers=70
count=0
joincount=0
a=''
while [ $count -lt $totalservers ] && read line; do
let count++
TEST=$(pwd)
if [ $count -le 10 ];
then
JAR="Coordinator.jar"
stringarray=($line)
ssh -T ${stringarray[0]} <<ENDSSH0 &
cd $TEST
java -jar $JAR $line
ENDSSH0
fi
done < configs.txt &
echo "sleeping"
sleep 5
b=0
echo "Joining servers"
while [ $b -lt $totalservers ]; do
let b++
if [ $b -le 10 ];
then
: nop
else
let joincount++
a+=' '$b
fi
if [[ $joincount == 5 ]]; then
java -jar CoordinatorClient.jar JOIN $a
echo $a
a=''
let joincount=0
else
: nop
fi
done
echo "sleeping"
sleep 15
echo "Starting DbServer"
while [ $count -lt $totalservers ] && read line; do
let count++
TEST=$(pwd)
if [ $count -le 10 ];
then
: nop
else
JAR="DbServer.jar"
stringarray=($line)
ssh -T ${stringarray[0]} <<ENDSSH0 &
cd $TEST
java -jar $JAR $line two
ENDSSH0
fi
done < configs.txt &

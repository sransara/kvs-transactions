sh ./killall.sh
sleep 7
echo " Start Coordinator and Db Servers"
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
else
JAR="DbServer.jar"
fi
stringarray=($line)
ssh -T ${stringarray[0]} <<ENDSSH0 &
cd $TEST
java -jar $JAR $line
ENDSSH0
done < configs.txt &
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
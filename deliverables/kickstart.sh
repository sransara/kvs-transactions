sh ./killall.sh
sleep 5
echo " Start Db Servers"
count=1
while [ $count -lt 6 ] && read line; do
let count++
TEST=$(pwd)
echo $TEST
stringarray=($line)
ssh -T ${stringarray[0]} <<ENDSSH0 &
cd $TEST
java -jar DbServer.jar $line
ENDSSH0
done < configs.txt &

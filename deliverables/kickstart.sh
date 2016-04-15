sh ./killall.sh
sleep 5
echo " Start Db Servers"
count=1
while [ $count -lt 6 ] && read line; do
let count++
stringarray=($line)
ssh -T ${stringarray[0]} <<ENDSSH0 &
cd /u/antor/u7/ravi18/KVSTransactions/deliverables
java -jar DbServer.jar $line
ENDSSH0
done < configs.txt

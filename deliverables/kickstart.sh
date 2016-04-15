sh ./killall.sh
echo " Start Db Servers"
while read line; do
stringarray=($line)
ssh -T ${stringarray[0]} <<'ENDSSH0' &
cd /u/antor/u7/ravi18/KVSTransactions/deliverables
java -jar DbServer.jar
ENDSSH0
done < configs.txt
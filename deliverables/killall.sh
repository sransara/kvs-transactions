while read line; do
stringarray=($line)
echo " Killing DbServer, Coordinator, Client on host " $line
ssh -T ${stringarray[0]} <<'ENDSSH0' &
jps -l | grep -E 'DbServer.jar|Coordinator.jar|TransactionClient.jar' | awk '{print $1}' | xargs kill -9
ENDSSH0
done < configs.txt
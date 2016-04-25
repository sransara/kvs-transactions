while read line; do
	stringarray=($line)
	ssh -T ${stringarray[0]} <<'ENDSSH0' &
	jps -l | grep -E 'DbServer.jar|Coordinator.jar|Client.jar|TransactionClient.jar' | awk '{print $1}' | xargs kill -9
ENDSSH0
done < configs.txt

while read line; do
	stringarray=($line) 
	echo ' Killing DbServer, Coordinator, Client on host ${stringarray[0]}'
	ssh -T ${stringarray[0]} <<'ENDSSH0' &
	jps -l | egrep (DbServer.jar | Coordinator.jar | Client.jar) | awk '{print $1}' | xargs kill -9
ENDSSH0
done < configs.txt
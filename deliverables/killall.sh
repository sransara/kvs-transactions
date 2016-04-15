while read line; do   
	stringarray=($line) 
	echo ' Killing Process on host with index # 1 '
	ssh -T ${stringarray[0]} <<'ENDSSH0' &
	jps -l | grep Process.jar | awk '{print $1}' | xargs kill -9
ENDSSH0
done < configs.txt
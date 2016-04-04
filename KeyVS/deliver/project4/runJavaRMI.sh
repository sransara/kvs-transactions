echo "Setting up 5 Replicas..."
FILE=configs.txt
k=1
while read line;do
		(cd "RMIServer$k" && java -jar RMIServer.jar $line &);
		((k++))
done < $FILE
for i in {1..5}; do 
  printf '\r%2d' $i
  sleep 1
done
(cd "RMIClient" && sh runJavaRMIClient.sh);
jps | grep jar | awk '{print $1}' | xargs kill -9
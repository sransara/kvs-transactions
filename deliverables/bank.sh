rm -rf out/*
java -jar TransactionClient.jar two < KvsScripts/bank1a.kvs > out/bank1a.txt

java -jar TransactionClient.jar two < KvsScripts/bank1b.kvs > out/bank1b-1.txt &
java -jar TransactionClient.jar two < KvsScripts/bank1b.kvs > out/bank1b-2.txt &
java -jar TransactionClient.jar two < KvsScripts/bank1b.kvs > out/bank1b-3.txt &
java -jar TransactionClient.jar two < KvsScripts/bank1b.kvs > out/bank1b-4.txt &
java -jar TransactionClient.jar two < KvsScripts/bank1b.kvs > out/bank1b-5.txt &
java -jar TransactionClient.jar two < KvsScripts/bank1b.kvs > out/bank1b-6.txt &
java -jar TransactionClient.jar two < KvsScripts/bank1b.kvs > out/bank1b-7.txt &
java -jar TransactionClient.jar two < KvsScripts/bank1b.kvs > out/bank1b-8.txt &
java -jar TransactionClient.jar two < KvsScripts/bank1b.kvs > out/bank1b-9.txt &

wait

java -jar TransactionClient.jar two < KvsScripts/bank1c.kvs > out/bank1c.txt

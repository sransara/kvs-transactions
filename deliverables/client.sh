rm -rf out/*
java -jar TransactionClient.jar two < KvsScripts/test1a.kvs > out/out1a.txt &
java -jar TransactionClient.jar two < KvsScripts/test1b.kvs > out/out1b.txt &

java -jar TransactionClient.jar two < KvsScripts/test2a.kvs > out/out2b.txt &


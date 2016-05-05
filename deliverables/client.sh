rm -rf out/*
java -jar TransactionClient.jar two < KvsScripts/test1a.kvs > out/out1a-1.txt &
java -jar TransactionClient.jar two < KvsScripts/test1b.kvs > out/out1b-2.txt &
#java -jar TransactionClient.jar two < KvsScripts/test3a.kvs > out/out3a-3.txt &
#java -jar TransactionClient.jar two < KvsScripts/test3a.kvs > out/out3a-4.txt &
#java -jar TransactionClient.jar two < KvsScripts/test3a.kvs > out/out3a-5.txt &
java -jar TransactionClient.jar two < KvsScripts/test1a.kvs > out/out1a-4.txt &
java -jar TransactionClient.jar two < KvsScripts/test2a.kvs > out/out2a-5.txt &
java -jar TransactionClient.jar two < KvsScripts/test2a.kvs > out/out2a-6.txt &
java -jar TransactionClient.jar two < KvsScripts/test1b.kvs > out/out1b-7.txt &
java -jar TransactionClient.jar two < KvsScripts/test2a.kvs > out/out2a-8.txt &
java -jar TransactionClient.jar two < KvsScripts/test1b.kvs > out/out1b-4.txt &
java -jar TransactionClient.jar two < KvsScripts/test2a.kvs > out/out2a-11.txt &
java -jar TransactionClient.jar two < KvsScripts/test2a.kvs > out/out2a-10.txt &
java -jar TransactionClient.jar two < KvsScripts/test1b.kvs > out/out1b-7.txt &
java -jar TransactionClient.jar two < KvsScripts/test2a.kvs > out/out2a-9.txt &

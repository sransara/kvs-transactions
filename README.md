# kvs-transaction
### Exploring transaction protocols for distributed key value store

In this project we implemented:
- Concurrency control: Optimistic
- Isolation level: Repeatable read
- Two phase optimistic commit protocol

## Original repo
https://github.com/prashant-r/KVSTransactions

## Usage
To generate the jar files
:Run "ant jar" from project root folder (that contains build.xml)

Next to do test transaction correctness:
cd deliverables

open three terminal tabs
one ssh into xinu machine for running: sh kickstart.sh
(This step starts coordinators and DbServers on servers as specified on configs.txt)
(use sh killall.sh at anytime to kill all these servers)

... wait till you see messages saying Paxos RMI started successfully ...

Then open
one ssh into mc13.cs.purdue.edu for running client: sh bank.sh
(mc13 is important, because it's not included as a server in configs.txt)
(and in case when transaction client was started in one of them java complains about thread limit reached)

run "cat out/*"
(This where the outputs are redicted to in bank.sh)

bank.sh basically starts one transaction script to initialize server bank values to 0
Then starts bunch of (9) concurrent transactions
Once they are all done: another bash script runs with a KVSScript to print the final bank values


DIY:
KVSScripts are the scripts that the TransactionClient.jar will take in as STDIN input
The syntax of these scripts is simple and can be adopted by looking at the code in KVSScripts folder
Or by looing at the implementation in TransactionClient.java
(Note depending on whether transactions are turned on some instructions are not recognized)

TransactionClient.java twopc < KVSScripts/bank1a.kvs
(with transactions enabled)

java -jar TransactionClient.java none < KVSScripts/sanstranscations-10.kvs
java -jar TransactionClient.java twopc < KVSScripts/transcations-10.kvs
(transactions disabled: used with plotting)

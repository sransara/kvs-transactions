README


------------------------------------------------------------
IMPORTANT NOTES: 
a) Please use ANT 1.9 and JavaC 1.8



b) Exclude the quotation marks when instructed to type in something.

c) Deliverables(PDFs, runfiles, Log files) are included in the deliver/projectx subdirectory. Where x represents the project#
------------------------------------------------------------
How TO TEST:

To clean, build and create JARs for all projects' code please type in the KeyVs folder:



"ant clean"

"ant jar" 



Following which change directory to point to project you're testing, for example:

"cd deliver/project2"

After which, based on what you're testing 

Project1) if you're testing UDP or TCP type 

"sh runTCPProject1.sh", 
"sh runUDPProject1.sh"
 respectively.
Project2) if you're testing Java RMI type "sh runJavaRMI.sh" 
Project3 and Project 4) Just type:  sh runJavaRMI.sh in the project3 or project4 folder
	  Explanation of what this does:
	  This script starts 5 servers at the addresses in configs.txt, and the client makes requests from the runJavaRMIClient.sh script in
	  the RMIClient folder. The script in the client folder consists of java -jar -DclientId=1 -DserverChoice=1 RMIClient.jar PUT Apple Color is Red like
	  code. What this means is that we are choosing server 1 with an optional identifier for clientId =1 and making the PUT request to 1st address in 
	  configs.txt  . If all goes well then we should be able to do a GET on another server say -DserverChoice = 2 and get the same value for key Apple.
	  
	  
IMPORTANT NOTE: The data in the hash is persisted using MapDB. So, if you shutdown the server and startup don't be surprised to see the key-values persist in disk.

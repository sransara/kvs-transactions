
echo "[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[["
echo "]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]"
echo "---------------------------BASIC TEST----------------------------"
echo "[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[["
echo "]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]"

#Basic TESTS
echo "---------------------------------------------------------------------------------------------------------------------------"
echo "5 PUTS"
echo "---------------------------------------------------------------------------------------------------------------------------"
echo "*******************  1.PUT APPLE ***********************"
java -jar -DclientId=1 -DserverChoice=1 RMIClient.jar PUT Apple Color is Red
echo "*******************  2.PUT BANANA *************************"
java -jar -DclientId=1 -DserverChoice=2 RMIClient.jar PUT Banana Color is Yellow
echo "*******************  3.PUT Orange ***********************"
java -jar -DclientId=1 -DserverChoice=3 RMIClient.jar PUT Orange Color is Orange
echo "******************   4.PUT KIWI  *************************"
java -jar -DclientId=1 -DserverChoice=4 RMIClient.jar PUT Kiwi Color is Green
echo "*******************  5.PUT PINEAPPLE *************************"
java -jar -DclientId=1 -DserverChoice=5 RMIClient.jar PUT Pineapple Color is Yellowish-Green
echo "---------------------------------------------------------------------------------------------------------------------------"
echo "5 GETS"
echo "-------------------------------------------------------------------------------------------------------------------------"
echo "*******************  1.GET APPLE ***********************"
java -jar -DclientId=1 -DserverChoice=5 RMIClient.jar GET Apple
echo "*******************  2.GET BANANA *************************"
java -jar -DclientId=1 -DserverChoice=2 RMIClient.jar GET Banana
echo "*******************  3.GET Orange ***********************"
java -jar -DclientId=1 -DserverChoice=3 RMIClient.jar GET Orange
echo "******************   4.GET KIWI  *************************"
java -jar -DclientId=1 -DserverChoice=1 RMIClient.jar GET Kiwi
echo "*******************  5.GET PINEAPPLE *************************"
java -jar -DclientId=1 -DserverChoice=4 RMIClient.jar GET Pineapple
echo "---------------------------------------------------------------------------------------------------------------------------"
echo "5 DELETES"
echo "-------------------------------------------------------------------------------------------------------------------------"
echo "*******************  1.DELETE APPLE ***********************"
java -jar -DclientId=1 -DserverChoice=1 RMIClient.jar DELETE Apple
echo "*******************  2.DELETE BANANA *************************"
java -jar -DclientId=1 -DserverChoice=3 RMIClient.jar DELETE Banana
echo "*******************  3.DELETE Orange ***********************"
java -jar -DclientId=1 -DserverChoice=4 RMIClient.jar DELETE Orange
echo "******************   4.DELETE KIWI  *************************"
java -jar -DclientId=1 -DserverChoice=2 RMIClient.jar DELETE Kiwi
echo "*******************  5.DELETE PINEAPPLE *************************"
java -jar -DclientId=1 -DserverChoice=5 RMIClient.jar DELETE Pineapple

echo "---------------------------------------------------------------------------------------------------------------------------"
echo "5 GETS TO TEST DELETE"
echo "-------------------------------------------------------------------------------------------------------------------------"
echo "*******************  1.GET APPLE ***********************"
java -jar -DclientId=1 -DserverChoice=5 RMIClient.jar GET Apple
echo "*******************  2.GET BANANA *************************"
java -jar -DclientId=1 -DserverChoice=1 RMIClient.jar GET Banana
echo "*******************  3.GET Orange ***********************"
java -jar -DclientId=1 -DserverChoice=2 RMIClient.jar GET Orange
echo "******************   4.GET KIWI  *************************"
java -jar -DclientId=1 -DserverChoice=3 RMIClient.jar GET Kiwi
echo "*******************  5.GET PINEAPPLE *************************"
java -jar -DclientId=1 -DserverChoice=4 RMIClient.jar GET Pineapple







echo "[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[["
echo "]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]"
echo "---------------------------MANUAL KILL SERVER 1 TEST-------------"
echo "[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[["
echo "]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]"






# TEST MANUAL FAIL

echo "*******************  1.PUT APPLE AND KILL server 1 ***********************"
java -jar -DclientId=1 -DserverChoice=1 RMIClient.jar KILL Server 1 &
java -jar -DclientId=1 -DserverChoice=1 RMIClient.jar PUT Apple iPhone6
echo "*******************  2.PUT MICROSOFT ***********************"
java -jar -DclientId=1 -DserverChoice=4 RMIClient.jar PUT Microsoft Band
echo "*******************  3.PUT AMAZON *************************"
java -jar -DclientId=1 -DserverChoice=2 RMIClient.jar PUT Amazon Fire
echo "*******************  4.PUT GOOGLE ***********************"
java -jar -DclientId=1 -DserverChoice=3 RMIClient.jar PUT Google Nexus
echo "******************   5.PUT ACER  *************************"
java -jar -DclientId=1 -DserverChoice=4 RMIClient.jar PUT Acer Aspire-V5 Notebook PC




echo ""
echo ""
echo "-------------------------------------------------------------------"

echo " HELLO THERE!,  "



echo "Server 1 is expected to be back in 12 seconds, if not already. So lets's wait  "

echo "Please read the following until then"
echo "Server 1 was dead for the duration of the above PUTS "
echo "Now, we send GETS to Server 1 to see if it catches up when its comes alive"
echo ""
echo ""

for i in {1..12}; do 
  printf '\r%2d' $i
  sleep 1
done

echo "*******************  1.GET APPLE ***********************"
java -jar -DclientId=1 -DserverChoice=1 RMIClient.jar GET Apple
echo "*******************  2.GET MICROSOFT *************************"
java -jar -DclientId=1 -DserverChoice=1 RMIClient.jar GET Microsoft
echo "*******************  3.GET AMAZON ***********************"
java -jar -DclientId=1 -DserverChoice=1 RMIClient.jar GET Amazon
echo "******************   4.GET GOOGLE  *************************"
java -jar -DclientId=1 -DserverChoice=1 RMIClient.jar GET Google
echo "*******************  5.GET ACER *************************"
java -jar -DclientId=1 -DserverChoice=1 RMIClient.jar GET Acer
echo "---------------------------------------------------------------------------------------------------------------------------"
echo ""
echo "//////// end of SIMULATION /////////////////////"


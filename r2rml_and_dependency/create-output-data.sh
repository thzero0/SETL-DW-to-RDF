# Creates a script to generate the output data files with the command: java -jar r2rml.jar ./files/licences/config.properties

JAR_FILE="r2rml.jar"

echo "Executando R2RML..."
echo -e "#------------------------------#" 

echo -e "\nGerando TTL de configuration ..."
java -jar "$JAR_FILE" ./files/configuration/config.properties

echo -e "\nGerando TTL de dataObject ..."
java -jar "$JAR_FILE" ./files/dataObjects/config.properties

echo -e "\nGerando TTL de date ..."
java -jar "$JAR_FILE" ./files/date/config.properties

echo -e "\nGerando TTL de licences ..."
java -jar "$JAR_FILE" ./files/licences/config.properties

echo -e "\nGerando TTL de permissions ..."
java -jar "$JAR_FILE" ./files/permissions/config.properties

echo -e "\nGerando TTL de providers ..."
java -jar "$JAR_FILE" ./files/providers/config.properties

echo -e "\nGerando TTL de status ..."
java -jar "$JAR_FILE" ./files/status/config.properties

echo -e "\nGerando TTL de time ..."
java -jar "$JAR_FILE" ./files/time/config.properties

echo -e "\n#------------------------------#"
echo -e "\nGerando TTL da fato ..."
java -jar "$JAR_FILE" ./files/fact/config.properties


echo -e "\n#------------------------------#"
echo "R2RML finalizado."
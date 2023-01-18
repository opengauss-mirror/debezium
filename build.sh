mkdir temp
cd ./temp
git clone https://github.com/osheroff/mysql-binlog-connector-java.git -b 0.25.4
cd mysql-binlog-connector-java
cp ../../debezium-connector-mysql/patch/mysql-binlog-connector-java-0.25.4.patch .
git apply --stat mysql-binlog-connector-java-0.25.4.patch
git apply --check mysql-binlog-connector-java-0.25.4.patch
git am -s < mysql-binlog-connector-java-0.25.4.patch
mvn clean package -Dmaven.test.skip=true
mvn -U install:install-file -DgroupId=com.zendesk -DartifactId=mysql-binlog-connector-java -Dversion=0.25.4-modified -Dpackaging=jar -Dfile=./target/mysql-binlog-connector-java-0.25.4-modified.jar
cd ../../
rm -rf temp
mvn clean package -P quick,skip-integration-tests,oracle,jdk11,assembly,xstream,xstream-dependency,skip-tests -Dgpg.skip -Dmaven.test.skip=true
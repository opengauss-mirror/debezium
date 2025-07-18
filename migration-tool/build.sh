#!/bin/bash
mvn clean package -Dmaven.test.skip=true
mkdir full-migration-tool
cp target/full-migration-jar-with-dependencies.jar full-migration-tool/full-migration-tool-7.0.0rc2.jar
cp -r config full-migration-tool/
tar -czf full-migration-tool-7.0.0rc2.tar.gz full-migration-tool
rm -rf full-migration-tool
mv full-migration-tool-7.0.0rc2.tar.gz target/
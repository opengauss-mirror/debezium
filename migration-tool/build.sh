#!/bin/bash
mvn clean package -Dmaven.test.skip=true
mkdir full-migration-tool
cp target/full-magration-jar-with-dependencies.jar full-migration-tool/full-migration-tool-7.0.0-RC2.jar
cp -r config full-migration-tool/
tar -czf full-migration-tool-7.0.0-RC2.tar.gz full-migration-tool
rm -rf full-migration-tool
mv full-migration-tool-7.0.0-RC2.tar.gz target/
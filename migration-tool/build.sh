#!/bin/bash
mvn clean package -Dmaven.test.skip=true
mkdir openGauss-FullReplicate
cp target/full-migration-jar-with-dependencies.jar openGauss-FullReplicate/openGauss-FullReplicate-7.0.0rc3.jar
cp -r config openGauss-FullReplicate/
tar -czf openGauss-FullReplicate-7.0.0rc3.tar.gz openGauss-FullReplicate
rm -rf openGauss-FullReplicate
mv openGauss-FullReplicate-7.0.0rc3.tar.gz target/
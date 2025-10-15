#!/bin/bash
mvn clean package -Dmaven.test.skip=true
mkdir oG_datasync_full_migration
cp target/full-migration-jar-with-dependencies.jar oG_datasync_full_migration/oG_datasync_full_migration-7.0.0rc3.jar
cp -r config oG_datasync_full_migration/
tar -czf oG_datasync_full_migration-7.0.0rc3.tar.gz oG_datasync_full_migration
rm -rf oG_datasync_full_migration
mv oG_datasync_full_migration-7.0.0rc3.tar.gz target/
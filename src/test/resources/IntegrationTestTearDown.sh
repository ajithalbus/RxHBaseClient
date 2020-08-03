#!/bin/sh
# Must have hbase/bin in path.

# Disable table
echo "disable 'test'" | hbase shell

# Drop table
echo "drop 'test'" | hbase shell
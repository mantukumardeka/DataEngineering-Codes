#!/bin/bash

##############################################
# HDFS Stats Report
##############################################

# Configuration
NAMENODE="enchbcclb2adm01.srv.bmogc.net"
PORT="50070"

# Get stats from NameNode
URL="http://${NAMENODE}:${PORT}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState"

echo "========================================="
echo "HDFS Statistics Report - $(date)"
echo "========================================="

curl -s "$URL" | python3 -c "
import sys, json
data = json.load(sys.stdin)
b = data['beans'][0]

# Get values
files_total = b.get('FilesTotal', 0)
total_objects = b.get('TotalFiles', 0)
directories = total_objects - files_total
blocks = b.get('BlocksTotal', 0)
dfs_used = b.get('CapacityUsed', 0)

# Format DFS Used
def fmt(bytes_val):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024:
            return f'{bytes_val:.2f} {unit}'
        bytes_val /= 1024
    return f'{bytes_val:.2f} TB'

print(f'1) DFS Used:          {fmt(dfs_used)}')
print(f'2) Files:             {files_total:,}')
print(f'   Directories:       {directories:,}')
print(f'3) Total Blocks:      {blocks:,}')
"

echo "========================================="

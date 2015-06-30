#!/usr/bin/env bash

# Use 'http://hbase.apache.org/book.html#_running_the_shell_in_non_interactive_mode' for reference

CONFIG_DIR='../../kafka-examples/src/main/resources'
CONFIG_FILE=${CONFIG_DIR}/'hbase.properties'

TBL_NAME=$(grep "table.name" $CONFIG_FILE | cut -d'=' -f 2 | tr -d '[[:space:]]')
COL_FAMILY=$(grep "column.family" $CONFIG_FILE | cut -d'=' -f 2 | tr -d '[[:space:]]')

# Alternative way, but it's not permitted to use dots in bash vars
# CONF=${CONF:=$CONFIG_FILE}
# if [ -f "${CONF}" ]; then . ${CONF}; fi
# echo ${table_name}

echo "Creating table [$TBL_NAME], family [$COL_FAMILY]"

set +o errexit
hbase shell -n <<EOF
disable '$TBL_NAME'
drop '$TBL_NAME'
create '$TBL_NAME', '$COL_FAMILY'
list '$TBL_NAME'
EOF
set -o errexit

echo "Exit"
exit 0
#!/bin/bash
ENC_TYPES="aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96 des3-cbc-sha1 arcfour-hmac des-hmac-sha1 des-cbc-md5"
USER=cloudera-scm/admin
KVNO=1
PASSWD=cloudera
KEYTAB_OUT=cloudera-scm.keytab

# Export password to keytab
IFS=' ' read -a ENC_ARR <<< "$ENC_TYPES"
{
  for ENC in "${ENC_ARR[@]}"
  do
    echo "addent -password -p $USER -k $KVNO -e $ENC"
    sleep 0.1
    echo "$PASSWD"
  done
  echo "wkt $KEYTAB_OUT"
} | ktutil

chmod 600 $KEYTAB_OUT

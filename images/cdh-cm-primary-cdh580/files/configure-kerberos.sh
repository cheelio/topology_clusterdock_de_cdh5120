#! /usr/bin/env bash

KERBEROS_REALM=${KERBEROS_REALM:-CLOUDERA}
KERBEROS_DOMAIN=${KERBEROS_DOMAIN:-cluster}
KERBEROS_HOSTNAME=${KERBEROS_HOSTNAME:-node-1.${KERBEROS_DOMAIN}}
KERBEROS_PRINCIPAL=${KERBEROS_PRINCIPAL:-cloudera-scm/admin}
KERBEROS_PASSWORD=${KERBEROS_PASSWORD:-cloudera}
JAVA_HOME=${JAVA_HOME:-/usr/java/jdk1.7.0_*-cloudera}

cat > /etc/krb5.conf <<EOF
[logging]
 default = FILE:/var/log/krb5libs.log
 kdc = FILE:/var/log/krb5kdc.log
 admin_server = FILE:/var/log/kadmind.log

[libdefaults]
 default_realm = ${KERBEROS_REALM}
 dns_lookup_realm = false
 dns_lookup_kdc = false
 ticket_lifetime = 24h
 renew_lifetime = 7d
 forwardable = true

[realms]
 ${KERBEROS_REALM} = {
  kdc = ${KERBEROS_HOSTNAME}
  admin_server = ${KERBEROS_HOSTNAME}
  max_renewable_life = 7d 0h 0m 0s
  default_principal_flags = +renewable
 }

[domain_realm]
 .${KERBEROS_DOMAIN} = ${KERBEROS_REALM}
 ${KERBEROS_DOMAIN} = ${KERBEROS_REALM}
EOF

cat > /var/kerberos/krb5kdc/kdc.conf <<EOF
[kdcdefaults]
 kdc_ports = 88
 kdc_tcp_ports = 88

[realms]
 ${KERBEROS_REALM} = {
  #master_key_type = aes256-cts
  acl_file = /var/kerberos/krb5kdc/kadm5.acl
  dict_file = /usr/share/dict/words
  admin_keytab = /var/kerberos/krb5kdc/kadm5.keytab
  # Java Cryptography Extension (JCE) Unlimited Strength Jurisdiction Policy Files
  # for JDK/JRE 7 must be installed in order to use 256-bit AES encryption (aes256-cts:normal)
  supported_enctypes = aes128-cts:normal des3-hmac-sha1:normal arcfour-hmac:normal des-hmac-sha1:normal des-cbc-md5:normal des-cbc-crc:normal max_life = 30d
  max_renewable_life = 30d
 }
EOF

echo "*/admin@${KERBEROS_REALM}  *" > /var/kerberos/krb5kdc/kadm5.acl

echo 'Setting root password for Kerberos...'
expect - <<EOF
set timeout 60

spawn kdb5_util create -s
expect "Enter KDC database master key:"
send "${KERBEROS_PASSWORD}\r"
expect "Re-enter KDC database master key to verify:"
send "${KERBEROS_PASSWORD}\r"
expect eof
EOF

echo 'Creating Kerberos principal...'-
expect - <<EOF
set timeout 60

spawn kadmin.local -q "addprinc ${KERBEROS_PRINCIPAL}"
expect "Enter password for principal \"${KERBEROS_PRINCIPAL}@${KERBEROS_REALM}\":"
send "${KERBEROS_PASSWORD}\r"
expect "Re-enter password for principal \"${KERBEROS_PRINCIPAL}@${KERBEROS_REALM}\":"
send "${KERBEROS_PASSWORD}\r"
expect eof
EOF


echo 'Generating keytabs'
ENC_TYPES="aes256-cts-hmac-sha1-96"
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
    sleep 1
    echo "$PASSWD"
  done
  echo "wkt $KEYTAB_OUT"
} | ktutil

chmod 600 $KEYTAB_OUT

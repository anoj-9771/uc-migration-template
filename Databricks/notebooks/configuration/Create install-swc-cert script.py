# Databricks notebook source
dbutils.fs.put("/databricks/scripts/install-swc-cert.sh", """
#!/bin/bash

cat << 'EOF' > /usr/local/share/ca-certificates/myca.crt
-----BEGIN CERTIFICATE-----
<CA CHAIN 1 CERTIFICATE CONTENT>
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
<CA CHAIN 2 CERTIFICATE CONTENT>
-----END CERTIFICATE-----
EOF

update-ca-certificates

PEM_FILE="/databricks/certificates/SWCRootCA_chain.pem"
JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")
KEYSTORE="$JAVA_HOME/lib/security/cacerts"

CERTS=$(grep 'END CERTIFICATE' $PEM_FILE| wc -l)

# To process multiple certs with keytool, you need to extract
# each one from the PEM file and import it into the Java KeyStore.

for N in $(seq 0 $(($CERTS - 1))); do
  ALIAS="$(basename $PEM_FILE)-$N"
  echo "Adding to keystore with alias:$ALIAS"
  cat $PEM_FILE |
    awk "n==$N { print }; /END CERTIFICATE/ { n++ }" |
    keytool -noprompt -import -trustcacerts \
            -alias $ALIAS -keystore $KEYSTORE
done

echo "export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt" >> /databricks/spark/conf/spark-env.sh
""", overwrite=True)

# COMMAND ----------

# dbutils.fs.put("/databricks/scripts/install-swc-cert.sh", """
# #!/bin/bash

# cat << 'EOF' > /usr/local/share/ca-certificates/myca.crt
# -----BEGIN CERTIFICATE-----
# <CA CHAIN 1 CERTIFICATE CONTENT>
# -----END CERTIFICATE-----
# -----BEGIN CERTIFICATE-----
# <CA CHAIN 2 CERTIFICATE CONTENT>
# -----END CERTIFICATE-----
# EOF

# update-ca-certificates

# PEM_FILE="/databricks/certificates/SWCRootCA_chain.pem"
# JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")
# KEYSTORE="$JAVA_HOME/lib/security/cacerts"

# CERTS=$(grep 'END CERTIFICATE' $PEM_FILE| wc -l)

# # To process multiple certs with keytool, you need to extract
# # each one from the PEM file and import it into the Java KeyStore.

# for N in $(seq 0 $(($CERTS - 1))); do
#   ALIAS="$(basename $PEM_FILE)-$N"
#   echo "Adding to keystore with alias:$ALIAS"
#   cat $PEM_FILE |
#     awk "n==$N { print }; /END CERTIFICATE/ { n++ }" |
#     keytool -noprompt -import -trustcacerts \
#             -alias $ALIAS -keystore $KEYSTORE -storepass $PASSWORD
# done

# echo "export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt" >> /databricks/spark/conf/spark-env.sh
# """, overwrite=True)

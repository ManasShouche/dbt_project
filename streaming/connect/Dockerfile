# Confluent's Connect base image has no opinion about which broker it talks
# to, so pointing it at Redpanda is fine.
FROM confluentinc/cp-kafka-connect-base:7.6.1

# Pulls the connector plus its BouncyCastle dependencies, which are what
# parse the PKCS#8 private key at runtime.
RUN confluent-hub install --no-prompt \
      snowflakeinc/snowflake-kafka-connector:latest

USER appuser

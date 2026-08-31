FROM confluentinc/cp-kafka-connect-base:7.6.1

RUN confluent-hub install --no-prompt \
      snowflakeinc/snowflake-kafka-connector:latest

USER appuser

#!/usr/bin/env bash
# Generates a self-signed CA, Kafka broker keystore, and client truststore.
# Requires keytool (bundled with any JDK).
# Run once before starting Docker Compose:  bash certs/generate-certs.sh
set -euo pipefail

PASS="${KAFKA_SSL_STORE_PASSWORD:-changeit}"
VALIDITY=3650
DIR="$(cd "$(dirname "$0")" && pwd)"

echo "==> Generating CA key pair..."
keytool -genkeypair -alias ca -keyalg RSA -keysize 2048 \
  -dname "CN=KafkaCA,O=Dev" \
  -validity "$VALIDITY" \
  -keystore "$DIR/ca.jks" -storepass "$PASS" -keypass "$PASS" \
  -ext BasicConstraints:critical=ca:true

echo "==> Exporting CA certificate..."
keytool -export -alias ca \
  -keystore "$DIR/ca.jks" -storepass "$PASS" \
  -rfc -file "$DIR/ca.crt"

echo "==> Generating broker key pair..."
keytool -genkeypair -alias kafka -keyalg RSA -keysize 2048 \
  -dname "CN=localhost,O=Dev" \
  -validity "$VALIDITY" \
  -keystore "$DIR/kafka.keystore.jks" -storepass "$PASS" -keypass "$PASS"

echo "==> Creating broker CSR..."
keytool -certreq -alias kafka \
  -keystore "$DIR/kafka.keystore.jks" -storepass "$PASS" \
  -file "$DIR/kafka.csr" \
  -ext "SAN=DNS:localhost,IP:127.0.0.1"

echo "==> Signing broker certificate with CA..."
keytool -gencert -alias ca \
  -keystore "$DIR/ca.jks" -storepass "$PASS" \
  -infile "$DIR/kafka.csr" -outfile "$DIR/kafka.crt" \
  -validity "$VALIDITY" \
  -ext "SAN=DNS:localhost,IP:127.0.0.1" -rfc

echo "==> Importing CA + signed cert into broker keystore..."
keytool -import -trustcacerts -alias ca \
  -file "$DIR/ca.crt" \
  -keystore "$DIR/kafka.keystore.jks" -storepass "$PASS" -noprompt
keytool -import -alias kafka \
  -file "$DIR/kafka.crt" \
  -keystore "$DIR/kafka.keystore.jks" -storepass "$PASS" -noprompt

echo "==> Creating client truststore..."
keytool -import -trustcacerts -alias ca \
  -file "$DIR/ca.crt" \
  -keystore "$DIR/kafka.truststore.jks" -storepass "$PASS" -noprompt

echo "==> Done. Files written to $DIR:"
echo "      kafka.keystore.jks  — broker identity (mount into Docker)"
echo "      kafka.truststore.jks — client trust anchor (used by the Java app)"

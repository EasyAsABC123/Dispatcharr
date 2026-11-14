#!/bin/bash
set -euo pipefail
mkdir -p /tls
chmod 755 /tls
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=OwnedMigrationCA -keyout /tls/ca.key -out /tls/ca.pem
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=OwnedWrongCA -keyout /tls/wrong-ca.key -out /tls/wrong-ca.pem
openssl req -new -newkey rsa:2048 -nodes -subj /CN=127.0.0.1 -keyout /tls/server.key -out /tls/server.csr
printf 'subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' > /tls/server.ext
openssl x509 -req -in /tls/server.csr -CA /tls/ca.pem -CAkey /tls/ca.key -CAcreateserial -days 1 -extfile /tls/server.ext -out /tls/server.pem
openssl req -new -newkey rsa:2048 -nodes -subj /CN=dispatch -keyout /tls/client.key -out /tls/client.csr
printf 'extendedKeyUsage=clientAuth\n' > /tls/client.ext
openssl x509 -req -in /tls/client.csr -CA /tls/ca.pem -CAkey /tls/ca.key -CAcreateserial -days 1 -extfile /tls/client.ext -out /tls/client.pem
cp /tls/client.key /tls/client-root.key
chmod 600 /tls/*.key
chown 1000:1000 /tls/server.key
chown 65532:65532 /tls/client.key
cat >> /data/db/postgresql.conf <<'CONF'
ssl=on
ssl_cert_file='/tls/server.pem'
ssl_key_file='/tls/server.key'
ssl_ca_file='/tls/ca.pem'
listen_addresses='127.0.0.1,::1'
CONF
cat > /data/db/pg_hba.conf <<'CONF'
local all all trust
hostnossl all all 127.0.0.1/32 trust
hostnossl all all ::1/128 trust
hostssl all all 127.0.0.1/32 cert
hostssl all all ::1/128 cert
CONF
su - dispatch -c '/usr/lib/postgresql/17/bin/pg_ctl -D /data/db restart -w -t 30'

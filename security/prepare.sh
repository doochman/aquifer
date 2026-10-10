#!/usr/bin/env bash
set -euo pipefail
umask 077
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT="${COMPOSE_PROJECT_NAME:-aquifer}"
ROOT="${LOCAL_SECURITY_ROOT:-$HOME/.local/share/local-lakehouse-security}/$PROJECT"
VOLUME="${PROJECT}_trino_security"
command -v openssl >/dev/null
command -v htpasswd >/dev/null
WORKSPACE="$(cd "$SCRIPT_DIR/../.." && pwd -P)"
[ ! -L "$ROOT" ] || { printf 'Refusing symlinked security directory.\n' >&2; exit 1; }
mkdir -p "$ROOT"
ROOT="$(cd "$ROOT" && pwd -P)"
case "$ROOT/" in
  "$WORKSPACE/"*) printf 'Security material must be outside the workspace.\n' >&2; exit 1 ;;
esac
chmod 700 "$ROOT"
if [ ! -f "$ROOT/ca.crt" ]; then
  [ -z "$(ls -A "$ROOT")" ] || { printf 'Refusing to overwrite partial security material.\n' >&2; exit 1; }
  openssl req -x509 -newkey rsa:3072 -nodes -sha256 -days 3650 -config "$SCRIPT_DIR/ca.cnf" -keyout "$ROOT/ca.key" -out "$ROOT/ca.crt" 2>/dev/null
  openssl req -new -newkey rsa:3072 -nodes -sha256 -config "$SCRIPT_DIR/server.cnf" -keyout "$ROOT/server.key" -out "$ROOT/server.csr" 2>/dev/null
  openssl x509 -req -in "$ROOT/server.csr" -CA "$ROOT/ca.crt" -CAkey "$ROOT/ca.key" -CAcreateserial -days 90 -sha256 -extfile "$SCRIPT_DIR/server.cnf" -extensions server_ext -out "$ROOT/server.crt" 2>/dev/null
  openssl rand -hex -out "$ROOT/keystore.password" 32
  openssl rand -hex -out "$ROOT/internal-secret" 32
  openssl pkcs12 -export -inkey "$ROOT/server.key" -in "$ROOT/server.crt" -certfile "$ROOT/ca.crt" -out "$ROOT/server.p12" -passout "file:$ROOT/keystore.password"
  for account in customer-profile-reference showcase airflow untrusted; do
    openssl rand -hex -out "$ROOT/$account.password" 32
  done
fi
for account in customer-profile-reference showcase airflow untrusted; do
  if [ -f "$ROOT/password.db" ]; then
    htpasswd -iB -C 10 "$ROOT/password.db" "$account" < "$ROOT/$account.password" >/dev/null 2>&1
  else
    htpasswd -iBc -C 10 "$ROOT/password.db" "$account" < "$ROOT/$account.password" >/dev/null 2>&1
  fi
done
openssl verify -CAfile "$ROOT/ca.crt" "$ROOT/server.crt" >/dev/null
openssl x509 -checkend 86400 -noout -in "$ROOT/server.crt" >/dev/null || { printf 'Server certificate requires explicit renewal.\n' >&2; exit 1; }
docker volume create --label "com.docker.compose.project=$PROJECT" --label com.docker.compose.volume=trino_security "$VOLUME" >/dev/null
docker run --rm --network none --user 0 --entrypoint /bin/bash -v "$ROOT:/source:ro" -v "$VOLUME:/destination" aquifer-airflow:3.3.2 -ec '
  install -m 0444 /source/ca.crt /destination/ca.crt
  for name in server.p12 keystore.password internal-secret password.db; do
    install -o 1000 -g 1000 -m 0400 "/source/$name" "/destination/$name"
  done
'
printf 'Prepared local TLS/authentication material in %s (private; do not share passwords or keys).\n' "$ROOT"
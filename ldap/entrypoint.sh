#!/bin/sh
set -e

DOMAIN="${LDAP_DOMAIN:-example.com}"
ADMIN_PASSWORD="${LDAP_ADMIN_PASSWORD:-admin}"

# Build base DC from domain (example.com -> dc=example,dc=com)
DC=$(echo "$DOMAIN" | awk -F. '{for(i=1;i<=NF;i++) printf "dc=%s%s",$i,(i<NF?",":"")}')

mkdir -p /var/lib/openldap/openldap-data /var/run/openldap
chown -R ldap:ldap /var/lib/openldap/openldap-data /var/run/openldap /etc/openldap

# Write slapd.conf
cat > /etc/openldap/slapd.conf <<EOF
include   /etc/openldap/schema/core.schema
include   /etc/openldap/schema/cosine.schema
include   /etc/openldap/schema/inetorgperson.schema

modulepath /usr/lib/openldap
moduleload back_mdb.so

pidfile   /var/run/openldap/slapd.pid
argsfile  /var/run/openldap/slapd.args

database  mdb
maxsize   1073741824
suffix    "${DC}"
rootdn    "cn=admin,${DC}"
rootpw    "${ADMIN_PASSWORD}"
directory /var/lib/openldap/openldap-data
EOF

echo "Starting slapd for LDIF bootstrap (DC: ${DC})..."
slapd -f /etc/openldap/slapd.conf -h "ldap://127.0.0.1:389/" -u ldap -g ldap

# Wait for slapd to be ready
for i in $(seq 1 15); do
  if ldapsearch -x -H ldap://127.0.0.1:389 -b "" -s base > /dev/null 2>&1; then
    echo "slapd is ready"
    break
  fi
  echo "Waiting for slapd... ($i)"
  sleep 1
done

DC_VALUE=$(echo "$DOMAIN" | cut -d. -f1)
echo "Creating root entry ${DC}..."
ldapadd -x -H ldap://127.0.0.1:389 \
  -D "cn=admin,${DC}" \
  -w "${ADMIN_PASSWORD}" <<EOF
dn: ${DC}
objectClass: top
objectClass: dcObject
objectClass: organization
o: ${LDAP_ORGANISATION:-Example}
dc: ${DC_VALUE}
EOF

echo "Loading config.ldif..."
ldapadd -x -H ldap://127.0.0.1:389 \
  -D "cn=admin,${DC}" \
  -w "${ADMIN_PASSWORD}" \
  -f /config.ldif && echo "LDIF loaded OK" || echo "LDIF load warning (entries may already exist)"

# Stop and restart in foreground on all interfaces
echo "Restarting slapd in foreground..."
kill "$(cat /var/run/openldap/slapd.pid)" 2>/dev/null || true
sleep 1

exec slapd -f /etc/openldap/slapd.conf -h "ldap://0.0.0.0:389/" -u ldap -g ldap -d 1

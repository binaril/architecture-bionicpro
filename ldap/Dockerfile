FROM alpine:3.19

RUN apk add --no-cache openldap openldap-back-mdb openldap-clients su-exec

ENV LDAP_DOMAIN=example.com \
    LDAP_ORGANISATION=Example \
    LDAP_ADMIN_PASSWORD=admin

COPY entrypoint.sh /entrypoint.sh
COPY config.ldif /config.ldif
RUN chmod +x /entrypoint.sh

EXPOSE 389

ENTRYPOINT ["/entrypoint.sh"]

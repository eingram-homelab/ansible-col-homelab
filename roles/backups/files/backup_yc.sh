#!/bin/sh

BACKUPDIR="/home/shared/app_backups"
SITE="www.ycdisp.com"
DB="yandcprod"
DBUSER="yandcprod"
DBPW="zTdT6Phdjfu"
DATE=`date +%d%m%y%H%M`

podman exec ${SITE}-db mysqldump -u $DBUSER -p${DBPW} $DB | gzip -c > ${BACKUPDIR}/dbbackup_${DB}_${DATE}.bak.gz

tar -czf ${BACKUPDIR}/sitebackup_${SITE}_${DATE}.tar.gz /home/poduser/code/${SITE}/public_html

find ${BACKUPDIR}/site* -mtime +5 -exec rm {} \;
find ${BACKUPDIR}/db* -mtime +5 -exec rm {} \;
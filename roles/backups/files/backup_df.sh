#!/bin/sh

BACKUPDIR="/home/shared/app_backups"
SITE="www-df"
DB="smfprod"
DBUSER="smfprod"
DBPW="Xd1ySPt8pMCr"
DATE=`date +%d%m%y%H%M`

podman exec ${SITE}-mariadb mysqldump -u $DBUSER -p${DBPW} $DB | gzip -c > ${BACKUPDIR}/dbbackup_${DB}_${DATE}.bak.gz

tar -czf ${BACKUPDIR}/sitebackup_${SITE}_${DATE}.tar.gz /home/poduser/code/${SITE}/public_html

find ${BACKUPDIR}/site* -mtime +5 -exec rm {} \;
find ${BACKUPDIR}/db* -mtime +5 -exec rm {} \;
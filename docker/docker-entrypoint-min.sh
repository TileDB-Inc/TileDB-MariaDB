#!/bin/bash
set -eo pipefail
shopt -s nullglob

/opt/server/bin/mariadb -e "CREATE USER 'tile'@'localhost' IDENTIFIED BY 'tile';"
/opt/server/bin/mariadb -e "GRANT ALL PRIVILEGES ON *.* TO 'tile'@'localhost' WITH GRANT OPTION"
/opt/server/bin/mariadb -e "CREATE USER 'tile'@'%' IDENTIFIED BY 'tile';"
/opt/server/bin/mariadb -e "GRANT ALL PRIVILEGES ON *.* TO 'tile'@'%' WITH GRANT OPTION"
/opt/server/bin/mariadb -e "FLUSH PRIVILEGES"

exec "$@"

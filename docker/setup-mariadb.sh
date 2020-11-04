#!/bin/bash

/opt/server/scripts/mariadb-install-db --auth-root-authentication-method=normal --datadir=/var/lib/mysql
/opt/server/bin/mariadbd --datadir=/var/lib/mysql &

for i in {30..0}; do
  if /opt/server/bin/mariadb -u root -e "SELECT 1;" &> /dev/null; then
    break
  fi
  echo 'MariaDB starting.....'
  sleep 0.1
done

/opt/server/bin/mariadb -u root -e "CREATE USER 'root'@'%';"
/opt/server/bin/mariadb -u root -e "GRANT ALL ON *.* TO 'root'@'%' WITH GRANT OPTION;"
/opt/server/bin/mariadb -u root -e "FLUSH PRIVILEGES;"
/opt/server/bin/mariadb -u root -e "FLUSH TABLES;"

killall mariadbd
sleep 5

#!/bin/bash
set -eo pipefail
shopt -s nullglob

# if command starts with an option, prepend mysqld
if [ "${1:0:1}" = '-' ]; then
	set -- mysqld "$@"
fi

# skip setup if they want an option that stops mysqld
wantHelp=
for arg; do
	case "$arg" in
		-'?'|--help|--print-defaults|-V|--version)
			wantHelp=1
			break
			;;
	esac
done

# Support both MARIADB_ and MYSQL_ parameters
# Docker expects one, helm another so lets do both
if [ ! -z "$MARIADB_ROOT_PASSWORD" ]; then
    export MYSQL_ROOT_PASSWORD=${MARIADB_ROOT_PASSWORD}
fi

if [ ! -z "$MARIADB_ALLOW_EMPTY_PASSWORD" ]; then
    export MYSQL_ROOT_PASSWORD=${MARIADB_ALLOW_EMPTY_PASSWORD}
fi

if [ ! -z "$MARIADB_RANDOM_ROOT_PASSWORD" ]; then
    export MYSQL_ROOT_PASSWORD=${MARIADB_RANDOM_ROOT_PASSWORD}
fi

if [ ! -z "$MARIADB_USER" ]; then
    export MYSQL_USER=${MARIADB_USER}
fi

if [ ! -z "$MARIADB_USERNAME" ]; then
    export MYSQL_USER=${MARIADB_USERNAME}
fi

if [ ! -z "$MARIADB_PASSWORD" ]; then
    export MYSQL_PASSWORD=${MARIADB_PASSWORD}
fi

if [ ! -z "$MARIADB_DATABASE" ]; then
    export MYSQL_DATABASE=${MARIADB_DATABASE}
fi

# usage: file_env VAR [DEFAULT]
#    ie: file_env 'XYZ_DB_PASSWORD' 'example'
# (will allow for "$XYZ_DB_PASSWORD_FILE" to fill in the value of
#  "$XYZ_DB_PASSWORD" from a file, especially for Docker's secrets feature)
file_env() {
	local var="$1"
	local fileVar="${var}_FILE"
	local def="${2:-}"
	if [ "${!var:-}" ] && [ "${!fileVar:-}" ]; then
		echo >&2 "error: both $var and $fileVar are set (but are exclusive)"
		exit 1
	fi
	local val="$def"
	if [ "${!var:-}" ]; then
		val="${!var}"
	elif [ "${!fileVar:-}" ]; then
		val="$(< "${!fileVar}")"
	fi
	export "$var"="$val"
	unset "$fileVar"
}

_check_config() {
	toRun=( "$@" --verbose --help --log-bin-index="$(mktemp -u)" )
	if ! errors="$("${toRun[@]}" 2>&1 >/dev/null)"; then
		cat >&2 <<-EOM

			ERROR: mysqld failed while attempting to check config
			command was: "${toRun[*]}"

			$errors
		EOM
		exit 1
	fi
}

# Fetch value from server config
# We use mysqld --verbose --help instead of my_print_defaults because the
# latter only show values present in config files, and not server defaults
_get_config() {
	local conf="$1"; shift
	"$@" --verbose --help --log-bin-index="$(mktemp -u)" 2>/dev/null \
		| awk '$1 == "'"$conf"'" && /^[^ \t]/ { sub(/^[^ \t]+[ \t]+/, ""); print; exit }'
	# match "datadir      /some/path with/spaces in/it here" but not "--xyz=abc\n     datadir (xyz)"
}

# allow the container to be started with `--user`
if [ "$1" = 'mysqld' -a -z "$wantHelp" -a "$(id -u)" = '0' ]; then
	_check_config "$@"
	DATADIR="$(_get_config 'datadir' "$@")"
	mkdir -p "$DATADIR"
	find "$DATADIR" \! -user mysql -exec chown mysql '{}' +
	exec gosu mysql "$BASH_SOURCE" "$@"
fi

if [ "$1" = 'mysqld' -a -z "$wantHelp" ]; then
	# still need to check config, container may have started with --user
	_check_config "$@"
	# Get config
	DATADIR="$(_get_config 'datadir' "$@")"

	if [ ! -d "$DATADIR/mysql" ]; then
		file_env 'MYSQL_ROOT_PASSWORD'
		if [ -z "$MYSQL_ROOT_PASSWORD" -a -z "$MYSQL_ALLOW_EMPTY_PASSWORD" -a -z "$MYSQL_RANDOM_ROOT_PASSWORD" ]; then
			echo >&2 'error: database is uninitialized and password option is not specified '
			echo >&2 '  You need to specify one of MYSQL_ROOT_PASSWORD, MYSQL_ALLOW_EMPTY_PASSWORD and MYSQL_RANDOM_ROOT_PASSWORD'
			exit 1
		fi

		mkdir -p "$DATADIR"

		echo 'Initializing database'
		installArgs=( --datadir="$DATADIR" --rpm )
		if { mysql_install_db --help || :; } | grep -q -- '--auth-root-authentication-method'; then
			# beginning in 10.4.3, install_db uses "socket" which only allows system user root to connect, switch back to "normal" to allow mysql root without a password
			# see https://github.com/MariaDB/server/commit/b9f3f06857ac6f9105dc65caae19782f09b47fb3
			# (this flag doesn't exist in 10.0 and below)
			installArgs+=( --auth-root-authentication-method=normal )
		fi
		# "Other options are passed to mysqld." (so we pass all "mysqld" arguments directly here)
		mysql_install_db "${installArgs[@]}" "${@:2}"
		echo 'Database initialized'
	fi
fi

exec "$@"

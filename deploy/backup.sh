#!/bin/sh
# Backup of the memex database (DESIGN §7): a custom-format pg_dump of the whole database, tombstones, grants
# and API key hashes included, into deploy/backups/ (owner-only: it holds every uploaded record).
#   deploy/backup.sh                        # prints the new file's absolute path
#   deploy/backup.sh restore <file> <db>    # into a NEW, empty database <db>; never over the live one
# The live database is MEMEX_PG_DB (default memex), as in compose.yaml.
# Old dumps are not deleted: move the ones you no longer need out yourself (to the trash).
# A backup taken before a forget still holds what was forgotten: restoring it brings that back. After a restore,
# repeat every forget made since the backup was taken.
set -eu
umask 077
here=$(cd "$(dirname "$0")" && pwd)
C="docker compose -f $here/compose.yaml"
live=${MEMEX_PG_DB:-memex}
case "${1:-dump}" in
  dump)
    mkdir -p "$here/backups"
    f=$(mktemp "$here/backups/memex-$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX") # unique: two runs never share a file
    $C exec -T db pg_dump -U memex -d "$live" -Fc > "$f"
    mv "$f" "$f.dump"
    echo "$f.dump"
    ;;
  restore)
    [ $# -eq 3 ] || { echo "usage: $0 restore <file> <new-db>" >&2; exit 2; }
    file=$(cd "$(dirname "$2")" && pwd)/$(basename "$2") target=$3 # resolved from the caller's directory
    [ -f "$file" ] || { echo "no such file: $2" >&2; exit 2; }
    # A plain name only: pg_restore -d also takes connection strings ("dbname=memex" would be the live database).
    case "$target" in
      *[!a-z0-9_]* | [!a-z_]* | "") echo "database name must match [a-z_][a-z0-9_]*: $target" >&2; exit 2 ;;
    esac
    [ ${#target} -le 63 ] || { echo "database name longer than 63 bytes" >&2; exit 2; }
    [ "$target" != "$live" ] && [ "$target" != memex ] || { echo "refusing to restore over the live database" >&2; exit 2; }
    $C exec -T db createdb -U memex "$target"
    $C exec -T db pg_restore -U memex -d "$target" --no-owner --exit-on-error < "$file"
    echo "restored $file into $target"
    ;;
  *) echo "usage: $0 [dump] | restore <file> <new-db>" >&2; exit 2 ;;
esac

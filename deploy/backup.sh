#!/bin/sh
# Daily backup of the memex database (DESIGN §7): a custom-format pg_dump of the whole database, tombstones
# and grants included, written next to this script under backups/, keeping the newest KEEP (default 14).
#   deploy/backup.sh                       # from the repository root, with the compose project up
#   deploy/backup.sh restore <file> <db>   # into a NEW, empty database <db> on the same server (never over memex)
# A backup taken before a forget still holds what was forgotten: restoring it brings that back. Forget again
# after a restore (the forget requests are the client's to repeat; the tombstones in a newer backup do not
# reach an older one).
set -eu
cd "$(dirname "$0")"
C="docker compose -f compose.yaml"
case "${1:-dump}" in
  dump)
    mkdir -p backups
    f="backups/memex-$(date -u +%Y%m%dT%H%M%SZ).dump"
    $C exec -T db pg_dump -U memex -d memex -Fc > "$f.part"
    mv "$f.part" "$f"
    ls -1t backups/memex-*.dump | tail -n +"$((${KEEP:-14} + 1))" | while read -r old; do rm -f -- "$old"; done
    echo "$f"
    ;;
  restore)
    file=$2 target=$3
    [ "$target" != memex ] || { echo "refusing to restore over the live database" >&2; exit 2; }
    $C exec -T db createdb -U memex "$target"
    $C exec -T db pg_restore -U memex -d "$target" --no-owner --exit-on-error < "$file"
    echo "restored $file into $target"
    ;;
  *) echo "usage: $0 [dump] | restore <file> <new-db>" >&2; exit 2 ;;
esac

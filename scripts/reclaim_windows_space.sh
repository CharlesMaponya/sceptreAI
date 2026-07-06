#!/usr/bin/env bash
set -Eeuo pipefail

DATA_UUID="4f660106-fe06-47fd-a7d6-38d162f6057f"
ROOT_UUID="414fe349-3c54-4164-81e7-892a3a394931"
DATA_MOUNT="/data"
STATE_FILE="/data/.home-migration-prepared"
INSTALLED_SCRIPT="/usr/local/sbin/reclaim-windows-space"

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

note() {
  printf '\n%s\n' "$*"
}

require_root() {
  (( EUID == 0 )) || die "Run this with sudo."
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

mount_uuid() {
  findmnt -nro UUID --target "$1" 2>/dev/null || true
}

check_machine() {
  local root_uuid
  root_uuid="$(mount_uuid /)"
  [[ "$root_uuid" == "$ROOT_UUID" ]] ||
    die "Root filesystem UUID is '$root_uuid', expected '$ROOT_UUID'. Refusing to touch a different machine/layout."
}

check_data_mount() {
  local data_uuid data_type
  mountpoint -q "$DATA_MOUNT" || die "$DATA_MOUNT is not mounted."
  data_uuid="$(mount_uuid "$DATA_MOUNT")"
  data_type="$(findmnt -nro FSTYPE --target "$DATA_MOUNT")"
  [[ "$data_uuid" == "$DATA_UUID" ]] ||
    die "$DATA_MOUNT UUID is '$data_uuid', expected '$DATA_UUID'."
  [[ "$data_type" == "ext4" ]] || die "$DATA_MOUNT is '$data_type', expected ext4."
}

install_self() {
  local source_path
  source_path="$(readlink -f "${BASH_SOURCE[0]}")"
  install -o root -g root -m 0755 "$source_path" "$INSTALLED_SCRIPT"
}

sync_home() {
  rsync \
    --archive \
    --acls \
    --xattrs \
    --hard-links \
    --sparse \
    --numeric-ids \
    --delete \
    --human-readable \
    --info=progress2 \
    --exclude='/lost+found' \
    --exclude='/.home-migration-prepared' \
    /home/ "$DATA_MOUNT"/
}

repair_expanded_sparse_files() {
  local source_file destination_file relative_path
  local source_size source_allocated destination_allocated

  while IFS= read -r -d '' source_file; do
    relative_path="${source_file#/home/}"
    destination_file="$DATA_MOUNT/$relative_path"
    [[ -f "$destination_file" ]] || continue

    source_size="$(stat -c %s "$source_file")"
    source_allocated="$(( $(stat -c %b "$source_file") * 512 ))"
    destination_allocated="$(( $(stat -c %b "$destination_file") * 512 ))"

    # If a sparse source became a substantially larger fully allocated copy,
    # remove only that destination copy so rsync --sparse can recreate it.
    if (( source_allocated * 2 < source_size &&
          destination_allocated > source_allocated * 2 )); then
      note "Recreating expanded sparse file: $relative_path"
      rm -f -- "$destination_file"
    fi
  done < <(find /home -xdev -type f -size +1G -print0)
}

prepare() {
  local used_bytes available_bytes unexpected
  require_root
  require_command rsync
  require_command findmnt
  check_machine
  check_data_mount

  if [[ ! -e "$STATE_FILE" ]]; then
    unexpected="$(
      find "$DATA_MOUNT" -mindepth 1 -maxdepth 1 \
        ! -name lost+found \
        ! -name .home-migration-prepared \
        -print -quit
    )"
    [[ -z "$unexpected" ]] ||
      die "$DATA_MOUNT is not empty (found '$unexpected'). Move that data elsewhere before continuing."
  fi

  install_self
  printf 'Home migration in progress; managed by %s\n' "$INSTALLED_SCRIPT" >"$STATE_FILE"
  repair_expanded_sparse_files

  used_bytes="$(du -sx --block-size=1 /home | awk '{print $1}')"
  available_bytes="$(df --output=avail --block-size=1 "$DATA_MOUNT" | tail -n 1 | tr -d ' ')"
  (( available_bytes > used_bytes + 5368709120 )) ||
    die "$DATA_MOUNT needs enough free space for /home plus a 5 GiB safety margin."

  note "Copying /home to the former Windows partition. Ubuntu remains unchanged and bootable during this step."
  sync_home
  sync

  note "Initial copy complete."
  cat <<'EOF'

Next:
  1. Close your applications and log out of the graphical desktop.
  2. Press Ctrl+Alt+F3 and log in at the text console.
  3. Run:

       sudo systemctl stop display-manager
       cd /
       sudo /usr/local/sbin/reclaim-windows-space activate

The activate step performs a final sync and updates /etc/fstab. It does not delete
the original /home.
EOF
}

activate() {
  local backup timestamp tmp
  require_root
  require_command rsync
  require_command findmnt
  check_machine
  check_data_mount
  [[ -e "$STATE_FILE" ]] || die "Run the prepare stage first."

  if systemctl is-active --quiet display-manager.service; then
    die "The graphical login is still running. Log out, switch to Ctrl+Alt+F3, and run 'sudo systemctl stop display-manager' first."
  fi

  note "Performing the final /home sync."
  sync_home
  sync

  timestamp="$(date +%Y%m%d-%H%M%S)"
  backup="/etc/fstab.before-home-migration-$timestamp"
  cp --archive /etc/fstab "$backup"
  tmp="$(mktemp)"

  awk -v device="UUID=$DATA_UUID" '
    /^[[:space:]]*#/ || NF < 2 {
      print
      next
    }
    !($1 == device && ($2 == "/data" || $2 == "/home")) {
      print
    }
  ' /etc/fstab >"$tmp"
  printf 'UUID=%s /home ext4 defaults 0 2\n' "$DATA_UUID" >>"$tmp"
  install -o root -g root -m 0644 "$tmp" /etc/fstab
  rm -f "$tmp"

  if ! findmnt --verify --verbose --tab-file /etc/fstab; then
    cp --archive "$backup" /etc/fstab
    systemctl daemon-reload
    die "The new fstab failed validation, so the original was restored."
  fi
  systemctl daemon-reload
  sync

  note "Activation is ready. The previous fstab is backed up at $backup."
  cat <<'EOF'

Reboot now:

  sudo reboot

After signing back in, verify the migration:

  sudo /usr/local/sbin/reclaim-windows-space verify

Do not run cleanup until your files and applications look normal.
EOF
}

verify() {
  local home_uuid home_source
  require_root
  require_command findmnt
  check_machine

  mountpoint -q /home || die "/home is not a separate mount. Do not clean up."
  home_uuid="$(mount_uuid /home)"
  home_source="$(findmnt -nro SOURCE --target /home)"
  [[ "$home_uuid" == "$DATA_UUID" ]] ||
    die "/home UUID is '$home_uuid', expected '$DATA_UUID'. Do not clean up."
  [[ -d /home/daemon-griffons ]] ||
    die "/home/daemon-griffons is missing. Do not clean up."

  note "Migration verified: /home is mounted from $home_source."
  df -hT / /home
  cat <<'EOF'

Your old /home is still safely stored underneath the new mount. Use Ubuntu for a
while and inspect your files. When you are satisfied, reclaim its space with:

  sudo /usr/local/sbin/reclaim-windows-space cleanup

Cleanup permanently deletes only the old, hidden copy after rechecking the mount.
EOF
}

cleanup() {
  local home_uuid root_view old_home old_size
  require_root
  require_command findmnt
  check_machine

  mountpoint -q /home || die "/home is not a separate mount. Refusing cleanup."
  home_uuid="$(mount_uuid /home)"
  [[ "$home_uuid" == "$DATA_UUID" ]] ||
    die "/home is not the migrated filesystem. Refusing cleanup."
  [[ -d /home/daemon-griffons ]] ||
    die "Migrated home directory is missing. Refusing cleanup."

  root_view="/mnt/root-before-home"
  old_home="$root_view/home"
  mkdir -p "$root_view"
  mountpoint -q "$root_view" && die "$root_view is already a mount point."
  mount --bind / "$root_view"
  trap 'mountpoint -q "$root_view" && umount "$root_view"' EXIT

  [[ "$(mount_uuid "$old_home")" == "$ROOT_UUID" ]] ||
    die "The hidden old /home is not on the expected root filesystem."
  [[ "$(stat -c %d /home)" != "$(stat -c %d "$old_home")" ]] ||
    die "Old and new /home resolve to the same filesystem. Refusing cleanup."

  note "Copying across any old-only files one last time."
  rsync \
    --archive \
    --acls \
    --xattrs \
    --hard-links \
    --numeric-ids \
    --ignore-existing \
    "$old_home"/ /home/
  sync

  old_size="$(du -shx "$old_home" | awk '{print $1}')"
  note "Deleting the verified old /home copy ($old_size) from the 90 GB root filesystem."
  find "$old_home" -xdev -mindepth 1 -delete
  sync
  umount "$root_view"
  trap - EXIT
  rmdir "$root_view"
  rm -f /home/.home-migration-prepared

  note "Space reclaimed successfully."
  df -hT / /home
}

usage() {
  cat <<EOF
Usage: sudo $0 {prepare|activate|verify|cleanup}

  prepare   Copy /home onto the 149 GB former Windows partition.
  activate  Final-sync and configure that partition as /home.
  verify    Verify the new /home mount after reboot.
  cleanup   Permanently remove the old hidden copy and free root space.
EOF
}

case "${1:-}" in
  prepare) prepare ;;
  activate) activate ;;
  verify) verify ;;
  cleanup) cleanup ;;
  *) usage; exit 2 ;;
esac

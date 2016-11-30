sudo mkfs.ext4 -F -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/disk/by-id/google-pg-data-disk-us &&
sudo mkdir -p /mnt/disks/pg-data && 
sudo mount -o discard,defaults /dev/disk/by-id/google-pg-data-disk-us /mnt/disks/pg-data &&
sudo umount /mnt/disks/pg-data

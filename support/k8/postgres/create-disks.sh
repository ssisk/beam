# https://blog.oestrich.org/2015/08/running-postgres-inside-kubernetes/
# note - you'll need to replace MACHINENAME with a valid machine
gcloud compute disks create pg-data-disk-us --size 50GB --zone us-central1-b &&
gcloud compute instances attach-disk MACHINENAME --disk pg-data-disk-us --zone us-central1-b --device-name=pg-data-disk-us

#./kubectl.sh create -f postgres-persistence.yml
#./kubectl.sh create -f postgres-claim.yml
kubectl.sh create -f postgres-pod-no-vol.yml
kubectl.sh create -f postgres-service.yml

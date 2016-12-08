There are the variables you'll need to replace: 
GCP_PROJECT
CLIENT_GCE_VM
GCP_REGION
YOUR_USERNAME

Instructions from https://dcos.io/docs/1.8/administration/installing/cloud/gce/
Note that those instructions changed several times while I was creating this, so the below could potentially be out of date. However, the linked instructions are of a form that easily allows skipping steps, so I created the below.

1. Create GCE networking subnet mesos-test TODO - gcloud command for this?
1. gcloud compute --project "GCP_PROJECT" firewall-rules create "mesos-master-bootstrap-public-port" --allow tcp:8080 --network "mesos-test" --source-ranges "0.0.0.0/0" --target-tags "mesos-master" 
1. gcloud compute --project "GCP_PROJECT" instances create "CLIENT_GCE_VM" --zone "GCP_REGION" --machine-type "n1-standard-1" --network "mesos-test" --metadata "block-project-ssh-keys=true" --maintenance-policy "MIGRATE" --scopes default="https://www.googleapis.com/auth/cloud-platform" --tags "mesos-master" --image "/centos-cloud/centos-7-v20160921" --boot-disk-size "10" --boot-disk-type "pd-standard" --boot-disk-device-name "CLIENT_GCE_VM"
1. gcloud compute ssh CLIENT_GCE_VM --zone GCP_REGION
1. yes | sudo yum update &&
yes | sudo yum install epel-release &&
yes | sudo yum install python-pip &&
yes | sudo pip install -U pip &&
yes | sudo pip install 'apache-libcloud==1.2.1' &&
yes | sudo pip install 'docker-py==1.9.0' &&
yes | sudo yum install git ansible
1. ssh-keygen -t rsa -f ~/.ssh/id_rsa -C YOUR_USERNAME
1. cp ~/.ssh/id_rsa.pub ~/.ssh/backup_id_rsa_pub
1. vim ~/.ssh/id_rsa.pub TODO - what to do here? (see instructions)
1. chmod 400 ~/.ssh/id_rsa
1. gcloud compute project-info add-metadata --metadata-from-file sshKeys=~/.ssh/id_rsa.pub
1. sudo vim /etc/selinux/config
1. REBOOT!!! If not, you'll need to start again.
1. sudo tee /etc/yum.repos.d/docker.repo <<-'EOF'
[dockerrepo]
name=Docker Repository
baseurl=https://yum.dockerproject.org/repo/main/centos/7/
enabled=1
gpgcheck=1
gpgkey=https://yum.dockerproject.org/gpg
EOF
1. yes | sudo yum install docker-engine-1.11.2
1. sudo vim /usr/lib/systemd/system/docker.service TODO - what to put here? 
1. sudo systemctl daemon-reload
1. sudo systemctl start docker.service
1. sudo docker run hello-world #just checking that docker works
1. git clone https://github.com/ajazam/dcos-gce
1. cd dcos-gce
1. vim group_vars/all
changed the following:
project
login_name
bootstrap_public_ip
from the GCE vm instance page - "external ip"
zone
1. vim ~/.ansible.cfg
1. vim hosts
changed the following:
master0 ip
notes say to do it bootstrap_pub_ip + 1, but that didn't work for me.
1. ansible-playbook -i hosts install.yml
1. gcloud compute --project "GCP_PROJECT" firewall-rules create "mesos-master-ux-port" --allow tcp:5050 --network "mesos-test" --source-ranges "0.0.0.0/0" --target-tags "master"
1. install DC/OS gui? not sure if I did this or not - https://dcos.io/docs/1.7/administration/installing/custom/gui/  
1. ansible-playbook -i hosts add_agents.yml --extra-vars "start_id=0001 end_id=0002 agent_type=private"

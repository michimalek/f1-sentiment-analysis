
# DE Assignment 2 - Group 4

To deploy this on GCS, execute the following steps;
- Installing using Docker & Docker compose using `sh`
```
sh docker.sh
sh docker_compose.sh
```
- Create the folders notebooks, checkpoint, and data at your VM with read_write permissions
```
mkdir notebooks
mkdir data
mkdir checkpoint
sudo chmod 777 notebooks/
sudo chmod 777 data/
sudo chmod 777 checkpoint/
```
- Change the volume paths in the `docker_compose.yml` file
- Change the ip address in the .env
```
cd 'work-dir'/deployment/
touch .env
nano .env
```
- Start the cluster
```
sudo docker-compose build
sudo docker-compose up -d
```
- Create firewall rules
```
gcloud compute firewall-rules create jupyter-port --allow tcp:8888
gcloud compute firewall-rules create spark-master-port --allow tcp:7077
gcloud compute firewall-rules create spark-master-ui-port --allow tcp:8080
gcloud compute firewall-rules create spark-driver-ui-port --allow tcp:4040
gcloud compute firewall-rules create spark-worker-1-ui-port --allow tcp:8081
gcloud compute firewall-rules create spark-worker-2-ui-port --allow tcp:8082
gcloud compute firewall-rules create kafka-port --allow tcp:9092
```
- Open Jupyter lab using the token, printed in the logs
```
sudo docker logs spark-driver-app
```
- Upload the notebooks from the repo to the /notebooks directory

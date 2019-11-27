## Note

**This project was built on Ubuntu 18.04 (Running on Windows Subsystem Linux(WSL), Kernel:4.19.75-microsoft-standard). Tested in Ubuntu 18.04 (bionic beaver, Kernel:5.2.0)**


## Requirements
* OS: Linux (Ubuntu 18.04)
* Docker
* Flink (v1.9.1)
* Kafka (v2.12)
* Elasticsearch (v5.6.0)
* Kibana (v5.6.0)
* Redis (Latest Docker image)
* Python (v3.*)


**Install `Maven`**
```bash
  sudo apt install maven    
```

**Install `OpenJDK8`**
```bash
  sudo apt install openjdk-8-jdk
```    

**Install `Docker`**
For installing Docker please follow the instructions [here](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-18-04)

**Install `Python`**
For installing Python please follow the instructions [here](http://ubuntuhandbook.org/index.php/2019/02/install-python-3-7-ubuntu-18-04/)

**Install `pip3`**
```bash
 sudo apt install python3-pip
 pip3 install -r code/customer-code/requirements.txt
```    


## Deployment 
Note:All the commands below should be run from the **root directory of the repository**. Some of the bash scipts require root access. So, if asked, please provide the root credentials.

**The following script will deploy the whole pipeline and download test data**

```bash
 code/deployment-scripts/deploy-all
```


## Running the pipeline


Upload the schema of the **final sink `Elasticsearch` (mysimpbdp-coredms)**

```bash
 code/customer-code/coredms-schema-upload
```

**Transform the location id ==> (lat,lon) pairs and Populating `Redis`**

```bash
 python3 code/customer-code/customer_transformer.py
```


**Running Customerstreamapp**

```bash
 code/customer-code/run-customerstreamapp
```


**Starting streaming customer data to `kafka`**

```bash
 python3 code/customer-code/customer_producer.py
```




## For cleanup (Removing all)

```bash
 code/deployment-scripts/cleanup
```





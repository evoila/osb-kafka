 # Service Broker
This repository is part of our service broker project. For documentation see [evoila/cf-service-broker](https://github.com/evoila/cf-service-broker)

# Local Development
To use this repository and develop the Service Broker check it out via:

```shell
git clone git@github.com:evoila/cf-service-broker-mongodb.git
git submodule update --init --recursive
``` 

Configure it in Intellij with the following properties:

```shell
SPRING_CLOUD_CONFIG_URI=http://config-server.your.domain.com
SPRING_APPLICATION_NAME=mysql-dev
SPRING_CLOUD_CONFIG_USERNAME=admin
SPRING_CLOUD_CONFIG_PASSWORD=cloudfoundry
SPRING_PROFILES_ACTIVE=local,development
```

# How to use it
This Service Broker supports two different was of deployment. 

Deployment Mode | Description | Scalable
------------ | ------------- | -------------
Shared Instance | Deploying a collection into an existing MongoDB Cluster | No
Dedicated Instance | A dedicated deployment of a single VM or multi VM MongoDB Cluster | Yes

## Deploy Shared Instance
To use a shared instance offering you need to provide a MongoDB Cluster, which is available through a network
connection from this Service Broker. You can use our Bosh MongoDB Deployment for that.

Next step is to configure the `application.yml` file in config repository, which enables the deployment to an 
existing Cluster. 

First thing you need to add is the following into your file:

```yaml
existing:
  endpoint:
    hosts: 
    - 127.0.0.1
    port: 27017
    database: admin
    username: admin
    password: admin
```

The next step is to configure a plan in the catalog for the existing endpoints. Example:

```yaml
- id: 2fc4b491-858c-41f4-803a-2309c3900
  name: XS
  description: A shared cluster for dev/test
  free: false
  platform: EXISTING_SERVICE
  metadata:
    connections: 1000
```

## Deploy Dedicated Instance(s)
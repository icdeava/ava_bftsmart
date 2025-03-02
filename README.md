### Installation Instructions:

#### Required Libraries:

apt-get update
sudo apt install default-jre
apt-get -y install build-essential

apt-get -y install curl

curl -s "https://get.sdkman.io" | bash

sdk install gradle 7.4.2
apt-get -y install ant
apt-get -y install iputils-ping

#### To compile the code:
enter ava_bftsmart directory and type 
sudo ./gradlew installDist 

#### Sample notebooks for running a simulation with 2 clusters (each containing 4 nodes) and a client per cluster are as follows:

1) prepare_config.ipynb to generate the configuration files.
2) local_simulation.ipynb to run the simulation.


cd

apt-get update
sudo apt install default-jre
apt-get -y install build-essential

apt-get -y install curl

curl -s "https://get.sdkman.io" | bash

sdk install gradle 7.4.2
apt-get -y install ant
apt-get -y install iputils-ping
clear
./gradlew installDist

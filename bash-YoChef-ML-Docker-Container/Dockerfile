FROM        steveny/predictionio:0.12.0
LABEL       maintainer="OscarGChavez"

# Agregando archivos necesarios
ADD        ./init.sh /home/pio/init.sh
ADD        ./retrain.sh /home/pio/retrain.sh

# Variables de entorno
#

# Exponer puertos necesarios
EXPOSE      22
EXPOSE      8000
EXPOSE      7070

# Directorio de ejecución de comandos
WORKDIR /

# Comandos a correr al crear la imagen
RUN sudo apt update && sudo apt install git openssh-server vim -y
RUN echo 'pio:pio' | sudo chpasswd
# RUN sudo git clone https://oauth2:ZHtYLds6Rk4zKqdkivpf@gitlab.com/oscar.garcia.c/yochef-ml.git /YoChef
RUN cd /PredictionIO-0.12.0-incubating/lib && wget https://yochef.s3.amazonaws.com/libs/mysql-connector-java-5.1.46.tar.gz && tar zxvfC mysql-connector-java-5.1.46.tar.gz .
RUN echo "MYSQL_JDBC_DRIVER=$PIO_HOME/lib/mysql-connector-java-5.1.46/mysql-connector-java-5.1.46.jar" | sudo tee -a /PredictionIO-0.12.0-incubating/conf/pio-env.sh > /dev/null
RUN echo "PIO_STORAGE_SOURCES_MYSQL_TYPE=jdbc" | sudo tee -a /PredictionIO-0.12.0-incubating/conf/pio-env.sh > /dev/null
RUN echo "PIO_STORAGE_SOURCES_MYSQL_URL=jdbc:mysql://yochefv01.ch7tumi7xe07.us-east-1.rds.amazonaws.com/YoChef?useSSL=false" | sudo tee -a /PredictionIO-0.12.0-incubating/conf/pio-env.sh > /dev/null
RUN echo "PIO_STORAGE_SOURCES_MYSQL_USERNAME=pio_usr" | sudo tee -a /PredictionIO-0.12.0-incubating/conf/pio-env.sh > /dev/null
RUN echo "PIO_STORAGE_SOURCES_MYSQL_PASSWORD=Pio2019!" | sudo tee -a /PredictionIO-0.12.0-incubating/conf/pio-env.sh > /dev/null
RUN sudo sed -i 's/PIO_STORAGE_REPOSITORIES_METADATA_SOURCE=ELASTICSEARCH/PIO_STORAGE_REPOSITORIES_METADATA_SOURCE=MYSQL/g' /PredictionIO-0.12.0-incubating/conf/pio-env.sh
RUN sudo sed -i 's/PIO_STORAGE_REPOSITORIES_EVENTDATA_SOURCE=HBASE/PIO_STORAGE_REPOSITORIES_EVENTDATA_SOURCE=MYSQL/g' /PredictionIO-0.12.0-incubating/conf/pio-env.sh
RUN sudo sed -i 's/PIO_STORAGE_REPOSITORIES_MODELDATA_SOURCE=LOCALFS/PIO_STORAGE_REPOSITORIES_MODELDATA_SOURCE=MYSQL/g' /PredictionIO-0.12.0-incubating/conf/pio-env.sh

# Comandos a correr al iniciar el container
ENTRYPOINT /home/pio/init.sh
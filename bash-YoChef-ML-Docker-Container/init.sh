#!/bin/bash

echo "--- BEGIN ---"

echo "--- Step 1 ---"
# Iniciar servicio SSH para conexiones remotas
sudo service ssh restart

echo "--- Step 2 ---"
# Configurar variables de entorno para nuevos sesiones
echo "export PATH=${PIO_HOME}/:${PATH}" > /home/pio/.profile

echo "--- Step 3 ---"
# Descargar codigo app YoChef desde repo privado
sudo git clone https://oauth2:ZHtYLds6Rk4zKqdkivpf@gitlab.com/oscar.garcia.c/yochef-ml.git /YoChef && cd /YoChef

echo "--- Step 4 ---"
# Iniciar servicios para ML
pio-start-all

echo "--- Step 4 ---"
# Crear App en Pio
ACCESS_KEY="$(pio app new YoChef | awk '/Key:/ {print $5}')"

#Descargar librerias necesarias para el proyecto
echo "--- Step 5 ---"
sudo pip install setuptools

echo "--- Step 6 ---"
sudo pip install wheel

echo "--- Step 7 ---"
sudo pip install predictionio

# sudo curl https://raw.githubusercontent.com/apache/spark/master/data/mllib/sample_movielens_data.txt --create-dirs -o data/sample_movielens_data.txt
# python /YoChef/data/import_eventserver.py --access_key $ACCESS_KEY

#Modificar nombre de applicación
echo "--- Step 8 ---"
sudo sed -i 's/INVALID_APP_NAME/YoChef/g' engine.json

#Otorgar permisos necesarios
echo "--- Step 9 ---"
sudo chmod -R 751 /YoChef

echo "--- Step 10 ---"
# Crear ML
pio build

# Entrenar ML
echo "--- Step 11 ---"
pio train --skip-sanity-check

# Correr ML
echo "--- Step 12 ---"
pio deploy

echo "--- END ---"

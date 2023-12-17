# StateData

El README consta de dos apartados importantes: una descripción breve acerca del código existente dentro de este repositorio y un manual de usuario para poder ejecutar dicha aplicación

## Descripción de código

El repositorio se puede entender como una división de dos partes: los distintos scripts ejecutados en la plataforma de Google Cloud y la aplicación web generada para poder mostrar los datos de una forma atractiva y justificada.
### Scripts generados
Existen cinco scripts que se han generado, cada uno con el propósito de obtener una gráfica que ayude a comprender los datos almacenados en el dataset y poder obtener una conclusión que concuerde con lo visto. Cada script hace uso de PySpark, pandas para poder generar el dataframe y de matplotlib para poder generar cada una de las gráficas.
    1. variabilityAnalysis.py: donde obtenemos los 6 distritos con más variabilidad de precios dentro del mercado.
    2. datesAnalisis.py: donde se obtiene la variación de los distritos obtenidos en el script anterior con respecto a los años.
    3. oldNewPropertyAnalysis.py: se realiza un estudio para poder obtener la tendencia de compras con respecto al estado de la propiedad (viejo o nuevo)
    4. pricesAnalysis.py: se obtiene el precio promedio a lo largo de los años de los 6 distritos anteriormente obtenidos 
    5. typePropertyAnalysis.py: obtenemos la cantidad de transacciones de bienes inmuebles que se han realizado con respecto al tipo de vivienda 
### Aplicación Web
Una vez generados los scripts y ejecutados en la plataforma Google Cloud se obtienen las gráficas, tanto de resultados como de rendimiento, ubicados en la carpeta images.
Se ha utilizado HTML y CSS para poder generar una aplicación web que muestre de manera detallada esos resultados así como una descripción y unas conclusiones obtenidas.

## Manual de Usuario
  1. Registrese y obtenga créditos en Google Cloud
  2. Descargue el dataset proporcionado. Puede descargarlo aquí.
  3. Cree un bucket dentro de Cloud Storage de clase standard y cargue ahi el dataset junto con los ficheros .py
  4. Cree un clúster. Abra la consola cloud shell y escriba lo siguiente: **gcloud dataproc clusters create example-cluster --region europe-west6 --enable-component-gateway --master-boot-disk-size 50GB --worker-boot-disk-size 50GB**
  5. Asegurese de que esta en estado 'En ejecución' en el apartado Clústeres de Dataproc.
  6. Acceda al apartado Jobs dentro de la sección Data proc, dentro seleccione create a new job. Rellene los siguientes campos de la forma:
    - Region: europe-west6
    - Cluster: example-cluster
    - JobType: PySpark
    - Main Python file: **gs://BUCKET/PYTHON. Siendo BUCKET el nombre del cluster creado y python el nombre del fichero python que se quiere ejecutar**
    - De al botón submit.
    - Vea los resultados en consola

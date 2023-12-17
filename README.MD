# StateData

El README consta de dos apartados importantes: una descripción breve acerca del código existente dentro de este repositorio y un manual de usuario para poder ejecutar dicha aplicación

## ** Manual de Usuario **
  1. Registrese y obtenga créditos en Google Cloud
  2. Descargue el dataset proporcionado. Puede descargarlo aquí.
  3. Cree un bucket dentro de Cloud Storage de clase standard y cargue ahi el dataset junto con los ficheros .py
  4. Cree un clúster. Abra la consola cloud shell y escriba lo siguiente:
    ** gcloud dataproc clusters create example-cluster --region europe-west6 --enable-component-gateway --master-boot-disk-size 50GB --worker-boot-disk-size 50GB **
  5. Asegurese de que esta en estado 'En ejecución' en el apartado Clústeres de Dataproc.
  6. Acceda al apartado Jobs dentro de la sección Data proc, dentro seleccione create a new job. Rellene los siguientes campos de la forma:
    1. Region: europe-west6
    2. Cluster: example-cluster
    3. JobType: PySpark
    4. Main Python file:
      ** gs://BUCKET/PYTHON. Siendo BUCKET el nombre del cluster creado y python el nombre del fichero python que se quiere ejecutar **
    5. De al botón submit.
    6. Vea los resultados en consola

1. Arquitectura
La arquitectura de nuestro proyecto consta de dos elementos, de los cuales comentaremos sus componentes. Como mostramos en la siguiente imagen, por un lado tenemos IBM y por el otro nuestro equipo local. 


IBM Cloud Object Storage: como su nombre indica, es una nube donde almacenamos la información que procesaremos y la información una vez guardada. Utilizamos un bucket llamado “task2-sd”.
IBM Cloud Functions: se trata de una nube que nos permite ejecutar nuestra aplicación de forma remota.
Main.py: es un archivo Python donde pre-procesamos los datos y creamos un archivo CSV más amplio. Una vez creado, lo subimos al Cloud para su posterior procesamiento.
Notebook (jupyter): nos permite ejecutar código Python de forma local y mostrar los Plots resultantes.
Ficheros configuración: nos permiten automatizar la conexión a IBM y la ejecución de la aplicación.

2. Decisiones de diseño
Como primera decisión de diseño acordamos realizar únicamente la parte de manipulación de datos, obteniendo datasets (csv) ya creados, por lo tanto no participamos en Hackathon.
Para el procesamiento de datos ejecutamos en la nube la función processData() en la cual unimos dos datasets previamente creados. El dataset resultante lo subimos a la nube para simplificar las posteriores consultas con la función getData().
Para la consulta de los datos del dataset utilizamos la librería pandas que nos permite tratar el archivo csv como una base de datos, permitiéndonos filtrar y obtener los datos necesarios mediante consultas de tipo SQL, utilizando el sqldf que nos ofrece pandasql. Este proceso de consulta del dataset lo realizamos ejecutando la función getData() en la nube mediante la librería lithops, a la cual se le pasa por parámetro la consulta SQL a ejecutar.
En la función getData()obtenemos el objeto de la nube, una vez obtenido el objeto aplicamos el formato utf-8 y convertimos el campo de fecha a Date, para posteriormente ejecutar la consulta SQL que recibe por parámetro.
Para la creación de los gráficos de líneas y barras utilizamos la librería matplotlib de forma local. Decidimos crear un par de funciones para la creación de los gráficos de líneas (graph_plot() y graph_plot_multiline()), ya que es el tipo de gráfico que más utilizamos. Para formatar la fecha que se muestra en los gráficos utilizamos la función formatar().
Para finalizar, las librerías utilizadas se encuentran en el requirements.

3. Volumen de datos
Hemos utilizado diversos datasets ya existentes, uno con volumen de datos de 203.000 y el otro de 210. Uno contiene información sobre datos de Covid diarios en Cataluña y el otro datos sobre defunciones  y altas hospitalarias, respectivamente. Para comprobar su contenido, incluimos los enlaces de la fuente de origen:
https://analisi.transparenciacatalunya.cat/Salut/Registre-de-casos-de-COVID-19-a-Catalunya-per-muni/jj6z-iyrp
https://analisi.transparenciacatalunya.cat/Salut/Incid-ncia-de-la-COVID-19-a-Catalunya/623z-r97q

Realizamos un merge entre estos dos datasets para crear uno nuevo procesado con la información (columnas) que nos interesa. Al realizar un merge into left, el dataset resultante tiene el mismo volumen de datos que el primero,  203.000. 
Intentamos realizar un merge adicional pero, el Cloud nos devolvía un error de que superabamos la memoria disponible de ejecución, escogieramos un dataset con gran volumen de datos o pequeño.


4. Intensidad computación
Para la etapa dos hemos creado una única función processData(). Esta única función lee las columnas de interés de los dos datasets mencionados anteriormente, realiza el merge y lo sube al Cloud.
Para la tercera etapa hemos implementado cuatro funciones. formatar() cambia el formato de las querys obtenidas, graph_plot() y graph_plot_multiline() crean las diversas gráficas a mostrar y getData() lee el objeto del Cloud y realiza la consulta deseada. 



5. Juegos de pruebas
Como juego de pruebas hemos implementado diversas consultas al IMB COS, donde tenemos datos sobre casos de covid 19 y datos de defunciones, ingresos y altas hospitalarias en Cataluña.
Hemos realizado las siguientes pruebas:
- Consultar la cantidad de casos COVID por tiempo en la comarca del Tarragonés a lo largo del tiempo. Podremos ver la evolución de los casos en dicha comarca.
- Consultar la cantidad total de casos COVID en ciertas comarcas. Para no saturar el gráfico, mostramos las comarcas de la A hasta la L.
- Para completar la prueba anterior, realizamos la misma consulta pero, de las comarcas desde la M hasta la Z.
- Consultar los casos de COVID en un mes (enero del 2020) en todas las comarcas de Cataluña.
- Consulta paralelizada de cantidad de casos en la comarca del Tarragonés. Detalladamente, mostramos la evolución de los casos a lo largo del tiempo separados por meses, en diversos gráficos. Realizamos diversas consultas paralelizadas.
- Consulta paralelizada de cantidad de altas diarias y defunciones durante el año 2020 y su comparación.

Mostramos la segunda prueba y su gráfica resultante como ejemplo.


Para ver el juego de pruebas completo, ejecutar el Jupyter Notebook.


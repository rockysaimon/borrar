Operador IN

- No ejecutar una consulta SELECT dentro de un IN.


Ejemplo: No usar queries de la siguiente manera:


SELECT

num_doc,

nombre,

apellido,

telefono

FROM zona.tabla_1

WHERE num_doc IN (SELECT num_doc FROM zona.tabla_2);


- Los valores dentro de un IN no deben superar los 20 elementos, más de estos deben crear tabla de parámetros en zona de datos de Procesos (ZDP).


Ejemplo: Si tiene un query como el siguiente:


SELECT

num_doc,

nombre,

apellido,

codigo

FROM zona.tabla_1

WHERE codigo IN (cod1, cod2, cod3, cod4, …, codn);


Para este caso puede crear una tabla paramétrica


Si necesita hacer búsqueda de todos los valores de la variable código


CREATE TABLE proceso.tbl_parametrica

STORED AS PARQUET TBLPROPERTIES AS

SELECT DISTINCT

codigo,

.

.

.

from zona.tabla

;

COMPUTE STATS proceso. tbl_parametrica;


Si necesita seleccionar valores específicos de la variable código


CREATE TABLE proceso.tbl_parametrica

(

codigo data_type,

.

.

.

)

STORED AS PARQUET TBLPROPERTIES;


Proceda a insertar sobre la tabla solo aquellos valores de la tabla código


INSERT INTO proceso.tbl_parametrica (codigo, …) VALUES (cod1,…),

VALUES (cod2,…),

VALUES (cod3,…),

.

.

.

;

COMPUTE STATS proceso.tbl_parametrica;


Finalmente cruce la tabla de interés con la tabla paramétrica considerando el campo codigo.

SELECT

t1.num_doc,

t1.nombre,

t1.apellido,

t1.codigo

FROM zona.tabla_1 t1

JOIN proceso.tbl_parametrica t2

ON t1.codigo = t2.codigo ;

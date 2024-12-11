# borrar
TENGO UN PROBLEMA EN SQL PARA CONTAR POR DÍAS LABORALES LOS CASOS QUE ESTÁN EN PROCESO Y VENCIDOS, RESULTA QUE TENGO UNA TABLA DE CASOS, DONDE TIENE TODAS LAS COLUMNAS QUE TIENE CADA CASO, YA SEA FECHA DE INICIO, INVESTIGADOR, ETC, Y HAY 2 TABLAS MÁS, UNA QUE TIENE TODAS LA FECHAS EN FORMATO AÑO-MES-DÍA, Y LA OTRA QUE TIENE LOS DÍAS FESTIVOS EN ESE MISMO FORMATO, LO QUE NECESITO ES QUE POR DÍAS LABORALES, ES DECIR, UN ANTIJOIN DE LOS DÍAS DEL AÑO CON LOS DÍAS FESTIVOS, SE HAGA UN CONTEO DE CASOS EN PROCESO (QUE SON CASOS ABIERTOS, DENTRO DEL TIEMPO DE EJECUCIÓN Y NO SE HAN CERRADO), Y DE LOS CASOS VENCIDOS (QUE SON CASOS ABIERTOS, FUERA DEL TIEMPO DE EJECUCIÓN, Y NO SE HAN CERRADO), POR LO QUE NECESITO LA LÓGICA DE CÓMO HACERLO, YA QUE DEBE SER UN HISTÓRICO, NO EL DE AHORA, SINO UN HISTÓRICO DE DIAS LABORALES DESDE LA PRIMER FECHA EN QUE SE HIZO, ESTAS SON LAS COLUMNAS QUE TIENE LA TABLA DE CASOS:
['Id_Caso', 'Codigo_LE_SIIFRA', 'Fecha_Registro_Base', 'Origen_Alerta',
       'Area_Origen_Alerta', 'Fecha_Reporte_Canal', 'Fecha_Reporte_SIE',
       'Fecha_Reporte_Jefe_SIE', 'Categoria_Ini_Caso', 'Tipo_Ini_Caso',
       'Criticidad_Caso', 'Comite_Superv_Rpta', 'Nivel_Prioridad',
       'Numero_Denuncia', 'Fecha_Denuncia_Penal', 'Estado_Proceso_Penal',
       'Valor_Pagado_Aseg', 'Satisfacción_Cliente', 'Investigador_Responsable',
       'Fecha_Entrega_Responsable', 'Tiempo_Estimado_Invest', 'Fraude',
       'Modalidad_Fraude', 'Producto_Afectado', 'Categoria_Fin_Caso',
       'Tipo_Fin_Caso', 'Fecha_Ini_Invest', 'Presupuesto_Estimado',
       'Fecha_Ocurrencia', 'Valor_Caso', 'Valor_Protegido', 'Valor_Recuperado',
       'Envio_Informe_GH', 'Fecha_Envio_Informe_GH', 'Envio_Informe_AI',
       'Fecha_Envio_Informe_AI', 'Envio_Informe_AE', 'Fecha_Envio_Informe_AE',
       'Envio_Informe_Comite', 'Fecha_Envio_Informe_Comite',
       'Reclamacion_Aseguradora', 'Valor_Reclamado_Aseg', 'Presupuesto_Real',
       'Privilegio_ClienteAbogado', 'Etapa_Proceso_Invest', 'Estado_Caso',
       'Hallazgos', 'Pendientes', 'Descripcion_Caso', 'Línea_Ética',
       'Fecha_Fin_Invest', 'Mala_Práctica', 'Auxiliar_Apoyo',
       'Contabilización', 'Fecha_Contabilización', 'Tramitadores',
       'Valor_Contabilizacion', 'Tipologia_Asobancaria', 'Fecha_Sentencia',
       'Delito', 'Firma_Abogados', 'Denunciado', 'Cat_Clasificación_Alerta',
       'SubCat_Clasificación_Alerta', 'Tipologia_Mala_Práctica',
       'Fecha_Debe_Cerrar', 'Dias_Meta', 'Pendiente_GH', 'Automatica',
       'Tipo_Alerta', 'Tipo_Empleado', 'Complejidad', 'Envio_Noti_Inicio_Caso',
       'Mesa_Contencion', 'Primer_Contacto', 'Justi_Vencimiento',
       'Dias_Meta_Reg', 'Fecha_Compromiso_Cierre', 'Prorroga_Le']
LA Fecha_Fin_Invest sólo la tiene si el caso ya se cerró y Fecha_Compromiso_Cierre es nula, si el caso sigue abierto, Fecha_Fin_Invest es null, pero la columna Fecha_Compromiso_Cierre tiene valor
necesito ayuda para la lógica de cómo hacerlo, el resultado debe ser algo así:
<img width="219" alt="{50257A38-4630-4995-9B58-A11476669B62}" src="https://github.com/user-attachments/assets/78c89f8c-6d23-440c-8c0e-48600e91d930" />


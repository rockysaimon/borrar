# borrar

'''
Autor: sizapa
fecha última actualización: 08/11/2024
'''

import DB_AS400
import DB_LZ
import DB_SQL
import LOG
from _operator import concat
from asn1crypto._ffi import null
from bs4 import BeautifulSoup
from datetime import datetime, date, timedelta
from dateutil import rrule
from dateutil.relativedelta import relativedelta
from numpy import int64
import numpy as np
import os.path
from os.path import isfile,join
from os import walk
from os import listdir
import pandas as pd
import pickle
import pyodbc
import re
import sqlite3
import sys, cgi
import time
from xlsxwriter.utility import xl_range, xl_col_to_name
#from Indicador_Monitoreos.Monitoreos_revision import calcular_indicadores_revision
#from Indicador_Monitoreos.monitoreos_seguimiento import calcular_indicador_seguimiento

modo_ejec = {'PGC_Plan_Gestion_Comercial_Vendedor':'NO_LZ',
             'Plan_Gestion_Comercial_cliente':'NO_LZ' }


archivo_codtrn = '\\\\sbmdebns03\\VP_SERV_ADMIN\\DIR_GEST_FRAUDE\\GCIA_SERV_INV_ESP\\SIE\\MINERIA\\DATOS\\COD_TRN_ITC.tab'
archivo_cdttrn = '\\\\sbmdebns03\\VP_SERV_ADMIN\\DIR_GEST_FRAUDE\\GCIA_SERV_INV_ESP\\SIE\\MINERIA\\DATOS\\TRN_CDT.tab'





def LZ_get_PGC_Plan_Gestion_Comercial_cliente_Nuevo(docClientes,year_ini, LOG):
    DB_LZ.df_ingestion = DB_LZ.load_ingestion()
    docCliente_consulta = ",".join(['{}'.format(str(k).zfill(15)) for k in docClientes])
    #docCliente_consulta=docClientes
    LOG.log(f"EL DOC ES {docCliente_consulta}")
    year_ini = int(year_ini)
    year_fin = int(time.strftime("%Y"))

    df = pd.DataFrame([])
    while year_ini<=year_fin:
        print("entra")
        sql ="""
            SELECT
              cod_ase_vta_dir AS CODASE,
              cast(num_doc_vta_dir as bigint) AS DOC_ASESOR,
              nombre_ase_vta_dir AS NOMBRE_ASESOR,
              nivel_ase_vta_dir AS CARGO_ASESOR,
              cast(num_doc_cli as bigint) AS DOC_CLIENTE,
              T1.nombre_cli AS NOMBRE_CLIENTE,
              segmento AS SEGMENTO,
              cod_of_apert_vta_dir AS COD_OFI_APERTU,
              of_apert_vta_dir AS OFI_APERTU,
              cod_of_vta_dir,
              of_vta_dir,
              TRIM(APLICACION) AS APLICACION,
              PLAN,
              nombre_prod AS NOMBRE_PRODUCTO,
              CAST(producto AS STRING) AS NRO_PRODUCTO,
              monto AS VALOR ,
              f_originacion AS FECHA_ACTIV,
              PLAZO,
              region_vend_vta_dir,
              zona_apert_vta_dir,
              zona_vend_vta_dir,
              cod_ase_vta_ref AS COD_ASESOR_VTA_REFERIDO,
              num_doc_vta_ref AS DOC_ASESOR_VTA_REFERIDO,
              nombre_ase_vta_ref AS NOMBRE_ASESOR_VTA_REFERIDO,
              ciudad_vta_ref AS CIUDAD_VTA_REFERIDO,
              compania AS EMPRESA,
              f_nacim_cli as FECHA_NACIMIENTO_CLIENTE,
              ciudad_nacim AS CIUDAD_NACIMIENTO_CLIENTE,
              edad_cli AS EDAD_CLIENTE,
              f_vinc AS FECHA_VINCULACION_CLIENTE,
              tipo_dirp AS TIPO_DIRECCION_CLIENTE,
              dir_p AS DIRECCION_CLIENTE,
              nombre_ciudad_dirp AS CIUDAD_DIRECCION_CLIENTE,
              email1 AS CORREO_CLIENTE,
              celular1 AS CELULAR_CLIENTE,
              tel1_valido AS TELEFONO_CLIENTE,
              dir_1 AS DIRECCION_2_CLIENTE,
              nombre_ciudad_dir1 AS CIUDAD_DIRECCION_2_CLIENTE,
              cel1_contactabilidad AS CEL_CONTACTABILIDAD,
              email1_contactabilidad AS CORREO_CONTACTABILIDAD

            FROM resultados_cdex_medicion_com.rep_ventas_productos as T1
            LEFT JOIN RESULTADOS_VSPC_CLIENTES.MASTER_CUSTOMER_DATA as T2 ON T1.llave_nombre = T2.llave_sistema 

            where T1.year = {0} and {1} and T1.f_originacion>=2020 and T1.num_doc_vta_dir IN ({2})

            """.format(year_ini,DB_LZ.get_ultima_inges_alias('master_customer_data','T2'),
                            docCliente_consulta)

        df_pgc = DB_LZ.sql_get(sql)
        print(sql)
        df = df.append(df_pgc)
        LOG.log("Consultando año: "+str(year_ini))
        year_ini+=1
    df.columns = map(str.upper, map(str, df.columns))
    df = df.drop_duplicates(['CODASE','DOC_ASESOR','DOC_CLIENTE','NOMBRE_CLIENTE','OFI_APERTU','PLAN','NOMBRE_PRODUCTO','NRO_PRODUCTO','VALOR','FECHA_ACTIV','COD_ASESOR_VTA_REFERIDO','DOC_ASESOR_VTA_REFERIDO'])
    print(df.columns)   
    #aquí tenemos a los doc de los clientes para la cartera castigada y de riesgos
    docs=df["DOC_CLIENTE"].values
    documentos_clientes=" ".join(map(str,docs))
    print("DOCUMENTOS DE LOS CLIENTES")
    print(documentos_clientes)


    df_cartera=AS400_get_cartera_castigada(documentos_clientes.split(), LOG)
    #df_cartera.reset_index(inplace=True, drop=True)
    print(df_cartera.columns)
    df_vencida=LZ_get_cartera_vencida_riesgos(documentos_clientes.split(), LOG)
    #df_vencida.reset_index(inplace=True, drop=True)
    print(df_vencida.columns)
    df=pd.merge(df,df_cartera, how="left", on=["NRO_PRODUCTO"])
    df=pd.merge(df,df_vencida, how="left", on=["NRO_PRODUCTO"])
    #df=pd.concat([df,df_cartera,df_vencida], axis=1)
    if not df.empty:
            df['DESC_PRODUCTO'] = df['DESC_PRODUCTO'].replace(np.nan,'')
            df.OBLIGACION=df.apply(lambda x: x.OBLIGACION[-4:] if x.DESC_PRODUCTO.strip()=='TARJETA DE CREDITO' else x.OBLIGACION , axis=1)
     
    return df


#cartera castigada
def get_cartera_castigada(archivo_resultado, documentos,usuarioc,consulta):
    try:
        LOG.log("Ejecuci�n Iniciada")

        fechacon_log = time.strftime("%Y%m%d") 
        horacon_log = time.strftime("%H%M%S") 
        horacon_log=int(horacon_log)
        Log_consultas_inicia(usuarioc, consulta, fechacon_log, horacon_log, documentos)
        
        #df_cartera = LZ_get_cartera_castigada(documentos.split(), LOG)
        df_cartera = AS400_get_cartera_castigada(documentos.split(), LOG)
        
        LOG.log("Guardar resultado")
        if not df_cartera.empty:  
            df_cartera.NRO_PRODUCTO=df_cartera.apply(lambda x: x.NRO_PRODUCTO[-4:] if x.PRODUCTO=='TARJETA MC' or  x.PRODUCTO=='AMERICAN EXPRESS' or x.PRODUCTO=='TARJETA VISA'  else x.NRO_PRODUCTO , axis=1)
        
        writer = pd.ExcelWriter(archivo_resultado)
        df_format = {'NRO_PRODUCTO':'text', 'VALOR_TOTAL_CASTIGO':'money', 'VALOR_INI_OBLIG':'money'}
        writer = df_to_excel_format(df_cartera, writer, "CARTERA_CASTIGADA", df_format)
        writer.save()

        Log_consultas_finaliza(usuarioc,consulta,fechacon_log,horacon_log)

        LOG.log("Ejecuci�n Finalizada")
            
    except:
        LOG.log("Error: " + str(sys.exc_info()).replace('\n', ' ').replace('\r',''))


def AS400_get_cartera_castigada(documentos, LOG):
    documentos = ",".join(["'{}'".format(str(k).zfill(15)) for k in documentos])
    LOG.log("Consultar cartera castigada")
    LOG.log(f"DOC CASTERA CASTIGADA {documentos}")
    sql = """
        SELECT
        TRIM(AMNOAC) AS NRO_PRODUCTO,
          AMDTCA AS FECHA_CASTIGO,
          AMAMTO AS VALOR_TOTAL_CASTIGO
        
        FROM VISIONR.AMBAL AS T1
        INNER JOIN VISIONR.CXREF AS T2 ON AMCDAP = CXCDAP AND AMNOAC = CXNOAC
        INNER JOIN VISIONR.CNAME AS T3 ON CNNAMK = CXNAMK
        WHERE CNNOSS IN ({0})
        ORDER BY CNNOSS, AMNOAC
    """.format(documentos)      
    df = DB_AS400.sql_get(sql, 'NACIONAL')
    return df

#cartera riegos

def get_cartera_riesgos(archivo_resultado,cedula,usuarioc,consulta,id_consulta): 
    try:
        LOG.log("Ejecuci�n Iniciada")
        print(cedula)
        fechacon_log = time.strftime("%Y%m%d") 
        horacon_log = time.strftime("%H%M%S") 
        horacon_log=int(horacon_log)
        #print('antes')
        Log_consultas_inicia_pwa(usuarioc, consulta, fechacon_log, horacon_log, cedula,id_consulta)
        LOG.log("Obteniendo Cartera vencida")                
        df_vencidos = LZ_get_cartera_vencida_riesgos(cedula.split(), LOG)
        if not df_vencidos.empty:
            df_vencidos['DESC_PRODUCTO'] = df_vencidos['DESC_PRODUCTO'].replace(np.nan,'')
            df_vencidos.OBLIGACION=df_vencidos.apply(lambda x: x.OBLIGACION[-4:] if x.DESC_PRODUCTO.strip()=='TARJETA DE CREDITO' else x.OBLIGACION , axis=1)
        
        LOG.log("Generando archivo de resultado")      
        
        writer = pd.ExcelWriter(archivo_resultado)
           
        writer = df_to_excel_format(df_vencidos, writer, 'Cartera_AlDia_Vencida')
        writer.save()

        Log_consultas_finaliza_pwa(usuarioc,consulta,fechacon_log,horacon_log,id_consulta)
           
        LOG.log("Ejecuci�n Finalizada")
    except:
        LOG.log("Error: " + str(sys.exc_info()).replace('\n', ' ').replace('\r', ''))
        

def LZ_get_cartera_vencida_riesgos(documentos, LOG):
    
    LOG.log(f"DOC CARTERA VENCIDA BEFORE {documentos}")
    documentos = ",".join(["{}".format(k) for k in documentos])
    LOG.log(f"DOC CARTERA VENCIDA {documentos}")
    año = time.strftime("%Y")
    año_atras = int(año) - 1
        
    sql_corte = """
            select MAx(corte) AS CORTE from resultados_riesgos.ceniegarc_lz
            where YEAR >= {0}        
    """.format(año_atras)
    df_corte = DB_LZ.sql_get(sql_corte)
    corte = str(df_corte.iloc[0]['corte']) 
    year = corte[0:4]
    
    sql_venci_vigen = """
                select 
                CAST(CAST(obl341 AS BIGINT) AS STRING)  AS NRO_PRODUCTO,
                SUM(SK) AS SALDO_CAPITAL,
                SUM(cv1) AS CARTERA_VENCIDA,
                PCONS AS DESC_PRODUCTO,
                fdesem AS FECHA_DESEMBOLSO 
                
            from resultados_riesgos.ceniegarc_lz
            where YEAR = {0} AND corte={1} and id IN ({2})
            GROUP BY ID,OBL341,apl,CORTE, sgto,fdesem,vdesem,apl,altmora,name,pcons,segdesc
    """.format(year,corte,documentos)  
    
    df = DB_LZ.sql_get(sql_venci_vigen)
    print(df)
    df.columns = map(str.upper, map(str, df.columns))
    LOG.log("pasó")
    return df



########### otra cosa


def Log_consultas_inicia_pwa(usuario_consulta,consulta,fechacon_log,horacon_log,filtro,id_consulta):
    try:
        usuario = usuario_consulta
        tipo_consulta = consulta
        estado = ''
        if len(filtro)>500:
            lista=list(filtro.split())
            separados=[lista[i:i+25] for i in range(0,len(lista),25)]
            for i in separados:
                param = ",".join(["{}".format(k) for k in i]) 
                sql = """
                    INSERT INTO PRUEBAS_SIE.LOG_CONSULTAS_WEBSIE (USUARIO, TIPO_CONSULTA, FECHACON,HORACON,PARAMETRO_BUSQUEDA,ESTADO,ID_CONSULTA) VALUES ('{0}','{1}',{2},{3},'{4}','{5}','{6}');    
                    """.format(usuario,tipo_consulta,fechacon_log,horacon_log,param,estado,id_consulta)
                DB_SQL.sql_exec(sql, 'SIE')
        else:
            sql = """
                 INSERT INTO PRUEBAS_SIE.LOG_CONSULTAS_WEBSIE (USUARIO, TIPO_CONSULTA, FECHACON,HORACON,PARAMETRO_BUSQUEDA,ESTADO,ID_CONSULTA) VALUES ('{0}','{1}',{2},{3},'{4}','{5}','{6}');    
            """.format(usuario,tipo_consulta,fechacon_log,horacon_log,filtro,estado,id_consulta)
            DB_SQL.sql_exec(sql, 'SIE')
        
    except:
        LOG.log("Error: " + str(sys.exc_info()).replace('\n', ' ').replace('\r', ''))


def Log_consultas_finaliza_pwa(usuario_consulta,consulta,fechacon_log,horacon_log,id_consulta):
    try:
        usuario = usuario_consulta
        tipo_consulta = consulta
        estado = 'Finaliz�'
        sql = """
             UPDATE PRUEBAS_SIE.LOG_CONSULTAS_WEBSIE SET ESTADO = '{0}' WHERE USUARIO = '{1}' AND FECHACON = ({2}) AND HORACON = {3} AND TIPO_CONSULTA = '{4}' AND ID_CONSULTA = '{5}' 
        """.format(estado,usuario,fechacon_log,horacon_log,tipo_consulta,id_consulta)
        print(sql)
        DB_SQL.sql_exec(sql, 'SIE')
        
    except:
        LOG.log("Error: " + str(sys.exc_info()).replace('\n', ' ').replace('\r', ''))     

   
"""
df_to_excel_format
=======================
Parametros:
     df : Dataframe a dar formato de Excel
     writer : 
     sgeet_name : Nombre de la hoja en la cual se ubicara el archivo dentro de archivo de excel
     format : Diccionario que contiene el formato correspondiente de cada columna
     version : 
"""
def df_to_excel_format(df, writer, sheet_name, xls_format = {}, xls_len = {}):

    df.to_excel(writer, sheet_name, index=False)   
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]

    format_title = workbook.add_format({'font_size': 10, 'font_name': 'calibri', 'bold': True, 'bg_color': '#000000', 'font_color': '#FFFFFF'})
    format_money = workbook.add_format({'font_size': 10, 'font_name': 'calibri', 'num_format': '_($ * #,##0_);_($ * (#,##0);_($ * "-"??_);_(@_)'})
    format_percent = workbook.add_format({'font_size': 10, 'font_name': 'calibri', 'num_format': '0.00%'})
    format_font = workbook.add_format({'font_size': 10, 'font_name': 'calibri'})
    format_text = workbook.add_format({'font_size': 10, 'font_name': 'calibri', 'num_format': '@'})
    format_wrap = workbook.add_format({'font_size': 10, 'font_name': 'calibri', 'num_format': '@', 'text_wrap': True} )
    
    columns = len(df.columns)
    column_series = pd.Series(range(columns))
    column_list = column_series.tolist()
    column_dict = dict()
    
    for i in column_list:
        header = df.columns[i]
        column_len = len(header) + 2
        
     
        column_ajust = 2
        
        if(header in xls_format):
            if(xls_format[header] in ["money", "percent"]):
                column_ajust = 4
                                
        column_len = max([len(x) for x in df[df.columns[i]].astype(str)] + [len(df.columns[i])]) + column_ajust        
        if np.isnan(column_len): column_len = 0
        
        column_dict[header] = column_len
        worksheet.set_column(i, i, column_len, format_font)


    for i in column_list:
        header = df.columns[i]
        column_name = xl_col_to_name(i)
        column_len = column_dict[df.columns[i]]
        
        if(header in xls_len):
            column_len = xls_len[header]
        
        if(header in xls_format):
            if(xls_format[header] in ["money"]):        
                worksheet.set_column(column_name+':'+column_name , column_len, format_money)
            elif(xls_format[header] in ["percent"]):        
                worksheet.set_column(column_name+':'+column_name , column_len, format_percent)
            elif(xls_format[header] in ["text"]):        
                worksheet.set_column(column_name+':'+column_name , column_len, format_text)
            elif(xls_format[header] in ["wrap"]):        
                worksheet.set_column(column_name+':'+column_name , column_len, format_wrap)
                           
           
    worksheet.conditional_format(xl_range(0, 0, 0, columns - 1), {'type': 'no_errors', 'format': format_title})
    
    return writer  

def Log_consultas_inicia(usuario_consulta,consulta,fechacon_log,horacon_log,filtro):
    try:
        usuario = usuario_consulta
        tipo_consulta = consulta
        estado = ''
        if len(filtro)>500:
            lista=list(filtro.split())
            separados=[lista[i:i+25] for i in range(0,len(lista),25)]
            for i in separados:
                param = ",".join(["{}".format(k) for k in i]) 
                sql = """
                    INSERT INTO PRUEBAS_SIE.LOG_CONSULTAS_WEBSIE (USUARIO, TIPO_CONSULTA, FECHACON,HORACON,PARAMETRO_BUSQUEDA,ESTADO) VALUES ('{0}','{1}',{2},{3},'{4}','{5}');    
                    """.format(usuario,tipo_consulta,fechacon_log,horacon_log,param,estado)
                DB_SQL.sql_exec(sql, 'SIE')
        else:
            sql = """
                 INSERT INTO PRUEBAS_SIE.LOG_CONSULTAS_WEBSIE (USUARIO, TIPO_CONSULTA, FECHACON,HORACON,PARAMETRO_BUSQUEDA,ESTADO) VALUES ('{0}','{1}',{2},{3},'{4}','{5}');    
            """.format(usuario,tipo_consulta,fechacon_log,horacon_log,filtro,estado)
            DB_SQL.sql_exec(sql, 'SIE')
        
    except:
        LOG.log("Error: " + str(sys.exc_info()).replace('\n', ' ').replace('\r', ''))
        
def Log_consultas_finaliza(usuario_consulta,consulta,fechacon_log,horacon_log):
    try:
        usuario = usuario_consulta
        tipo_consulta = consulta
        estado = 'Finaliz�'
        sql = """
             UPDATE PRUEBAS_SIE.LOG_CONSULTAS_WEBSIE SET ESTADO = '{0}' WHERE USUARIO = '{1}' AND FECHACON = ({2}) AND HORACON = {3} AND TIPO_CONSULTA = '{4}'    
        """.format(estado,usuario,fechacon_log,horacon_log,tipo_consulta)
        print(sql)
        DB_SQL.sql_exec(sql, 'SIE')
        
    except:
        LOG.log("Error: " + str(sys.exc_info()).replace('\n', ' ').replace('\r', ''))




#LZ_get_cartera_vencida_riesgos, AS400_get_cartera_castigada, LZ_get_PGC_Plan_Gestion_Comercial_cliente_Nuevo
'''
LZ_get_PGC_Plan_Gestion_Comercial_cliente_Nuevo(docClientes,year_ini, LOG = None)

AS400_get_cartera_castigada(documentos, LOG = None)

LZ_get_cartera_vencida_riesgos(documentos, log = None)
'''

df=LZ_get_PGC_Plan_Gestion_Comercial_cliente_Nuevo("1102824667".split(),"2020", LOG)
#78078472
print(df.to_string())


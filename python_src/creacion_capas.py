# -*- coding: utf-8 -*-
'''
Created on 17/03/2016

@author: Topo
'''
import geojson
from pyspatialite import dbapi2 as db
import os
import json
import glob
import matplotlib.pyplot as plt
import shutil
import datetime
import subprocess
import sys
import qgis
from PyQt4.QtGui import *
from PyQt4.QtCore import *
from qgis.core import *
from qgis.gui import *

qgisPath = r"/usr"
raiz = os.path.join(os.path.dirname(__file__),"capas")
copiarA = os.path.join(os.path.dirname(__file__),"..","web_src")
nombreBD = "dbMonitoreo.sqlite"
bDatosPath = os.path.join(os.path.dirname(__file__),"..","bd",nombreBD)

def buscarMaximoMinimo(cadena):
    max = 0
    min = 0
    for r in cadena.split("\n"):
        if "STATISTICS_MINIMUM" in r:
            min = float(r.split("=")[-1].strip())        
        elif "STATISTICS_MAXIMUM" in r:
            max = float(r.split("=")[-1].strip())
    return max,min

def buscarNumeroDePuntos(cadena):                                      
    for r in cadena.split("\n"):
        if "Feature Count:" in r:
            return int(r.split(":")[-1].strip())

def noneToNull(valor):
    if valor is None:
        return "NULL"
    return valor

def desplegarColumnas(properties):
    cadena=[]
    for k in properties:
        cadena.append(properties[k])
    print " ".join([noneToNull(e) for e in cadena])
    
def cambioRecientemente(menosDiez,fechas):
    listaFechas = fechas.split(",")
    for f in listaFechas:
        if datetime.datetime.strptime(f, "%Y-%m-%d %H:%M:%S")>= datetime.datetime.strptime(menosDiez, "%Y-%m-%d %H:%M:%S"):
            return "si"
    return "no"

def crearGeoJsonPrecipitacion(final):
    query = """
            SELECT 
            id_estacion,
            tipo,nombre,
            estado,
            SUM(precipitacion) AS precipitacion_24hr ,
            CASE 
                    WHEN  SUM(precipitacion)>= 0.1 AND SUM(precipitacion)<25.1 THEN 'lluvias aisladas' 
                    WHEN SUM(precipitacion)>= 25.1 AND SUM(precipitacion)<50.1 THEN 'chubascos con tormenta fuertes' 
                    WHEN SUM(precipitacion)>= 50.1 AND SUM(precipitacion)<75.1 THEN 'chubascos con tormenta fuertes muy fuertes' 
                    WHEN SUM(precipitacion)>= 75.1 AND SUM(precipitacion)<150.1 THEN 'tormentas intensas' 
                    WHEN SUM(precipitacion)>= 150.1 AND SUM(precipitacion)<250.1 THEN 'torrenciales'
                    WHEN SUM(precipitacion)>=250.1 THEN 'extraordinarios' 
            END AS clasificacion,
            cambioRecientementeSqlite(DATETIME('now','-5 hours','-30 minutes'),GROUP_CONCAT(fecha_hora_precipitacion)) AS cambioRecientemente,
            AsGeoJSON(geom) AS geom_json,
            CASE 
                WHEN SUM(precipitacion)>= 0.1 AND SUM(precipitacion)<25.1 THEN 1
                WHEN SUM(precipitacion)>= 25.1 AND SUM(precipitacion)<50.1 THEN 2
                WHEN SUM(precipitacion)>= 50.1 AND SUM(precipitacion)<75.1 THEN 3
                WHEN SUM(precipitacion)>= 75.1 AND SUM(precipitacion)<150.1 THEN 4
                WHEN SUM(precipitacion)>= 150.1 AND SUM(precipitacion)<250.1 THEN 5
                WHEN SUM(precipitacion)>=250.1 THEN 6
                END AS posicion
            FROM estaciones
            INNER JOIN datos_estaciones
            ON estaciones.id = datos_estaciones.id_fk
            WHERE datos_estaciones.precipitacion>0 AND estaciones.geom IS NOT NULL AND DATETIME("fecha_hora_precipitacion")>=DATETIME('{final}')
            GROUP BY estaciones.id_estacion
            ORDER BY posicion DESC,precipitacion DESC""".format(final=final)
        
    try:
        with db.connect(bDatosPath) as con:
            con.create_function("cambioRecientementeSqlite",2,cambioRecientemente)
            cur = con.cursor()
            cur.execute(query)
            data = cur.fetchall()
            
            listaRegistros = []
            for r in data:
                properties = {}
                properties["id_estacion"] = r[0]
                properties["tipo"] = r[1]
                properties["nombre"] = r[2]
                properties["estado"] = r[3]
                properties["precipitacion"] = r[4]
                properties["clasificacion"] = r[5]
                properties["cambio recientemente"] = r[6]
                # desplegarColumnas(properties)
                geom = r[7]
                geomJson = geojson.loads(geom)    
                feature = geojson.Feature(geometry=geomJson, properties=properties)
                listaRegistros.append(feature)
                
            geojsonColeccion = geojson.FeatureCollection(listaRegistros)
            fw = open(os.path.join(raiz,"procesos/estaciones_24hr.geojson"),"w")
            fw.write(json.dumps(geojsonColeccion)) 
            fw.close()       
              
    except db.Error, e:
        print "Error %s:" % e.args[0]

def crearJsonPrecipitacion(final):
    query = """
            SELECT id_estacion,tipo,nombre,estado,SUM(precipitacion) AS precipitacion_24hr ,
                CASE 
                        WHEN  SUM(precipitacion)>= 0.1 AND SUM(precipitacion)<25.1 THEN 'lluvias aisladas' 
                        WHEN SUM(precipitacion)>= 25.1 AND SUM(precipitacion)<50.1 THEN 'chubascos con tormenta fuertes' 
                        WHEN SUM(precipitacion)>= 50.1 AND SUM(precipitacion)<75.1 THEN 'chubascos con tormenta fuertes muy fuertes' 
                        WHEN SUM(precipitacion)>= 75.1 AND SUM(precipitacion)<150.1 THEN 'tormentas intensas' 
                        WHEN SUM(precipitacion)>= 150.1 AND SUM(precipitacion)<250.1 THEN 'torrenciales'
                        WHEN SUM(precipitacion)>=250.1 THEN 'extraordinarios' 
                END AS clasificacion,
                GROUP_CONCAT(fecha_hora_precipitacion) AS fechas,
                GROUP_CONCAT(precipitacion) AS precipitaciones,
                CASE 
                    WHEN SUM(precipitacion)>= 0.1 AND SUM(precipitacion)<25.1 THEN 1
                    WHEN SUM(precipitacion)>= 25.1 AND SUM(precipitacion)<50.1 THEN 2
                    WHEN SUM(precipitacion)>= 50.1 AND SUM(precipitacion)<75.1 THEN 3
                    WHEN SUM(precipitacion)>= 75.1 AND SUM(precipitacion)<150.1 THEN 4
                    WHEN SUM(precipitacion)>= 150.1 AND SUM(precipitacion)<250.1 THEN 5
                    WHEN SUM(precipitacion)>=250.1 THEN 6
                END AS posicion
            FROM estaciones
            INNER JOIN (
                SELECT id_fk,fecha_hora_precipitacion,precipitacion
                FROM datos_estaciones
                WHERE precipitacion>0 AND precipitacion IS NOT NULL AND DATETIME("fecha_hora_precipitacion")>=DATETIME('{final}')
                ORDER BY id_fk,DATETIME(fecha_hora_captura)) AS precipitaciones
            ON estaciones.id = precipitaciones.id_fk
            WHERE geom IS NOT NULL
            GROUP BY id_estacion
            ORDER BY posicion DESC,precipitacion DESC""".format(final=final)
        
    try:
        with db.connect(bDatosPath) as con:
            cur = con.cursor()
            cur.execute(query)
            data = cur.fetchall()
            
            listaRegistros = []
            for r in data:
                properties = {}
                properties["id_estacion"] = r[0]
                properties["tipo"] = r[1]
                properties["nombre"] = r[2]
                properties["estado"] = r[3]
                properties["precipitacion"] = r[4]
                properties["clasificacion"] = r[5]
                properties["fechas"] = r[6].split(",")
                properties["precipitaciones"] = r[7].split(",")
                listaRegistros.append(properties)
            fw = open(os.path.join(raiz,"procesos/estaciones_24hr.json"),"w")
            fw.write(json.dumps(listaRegistros)) 
            fw.close()       
              
    except db.Error, e:
        print "Error %s:" % e.args[0]

def crearInterpolacion():
    ogr2ogrInstruccion = [
                   "ogr2ogr",
                   "-f",
                   "'ESRI Shapefile'",
                   "-t_srs",
                   "EPSG:32615",
                   "-overwrite",
                   os.path.join(raiz,"procesos/estaciones_24hr.shp"),
                   os.path.join(raiz,"procesos/estaciones_24hr.geojson")]
 
    stdout, stderr = subprocess.Popen(
                                      " ".join(ogr2ogrInstruccion),
                                      shell=True,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE).communicate()
                                                
    ogrinfoInstruccion = [
                          "ogrinfo",
                          "-so",
                          os.path.join(raiz,"procesos/estaciones_24hr.shp"),
                          "estaciones_24hr"]
    stdout, stderr = subprocess.Popen(
                                      " ".join(ogrinfoInstruccion),
                                      shell=True,stdout=subprocess.PIPE, 
                                      stderr=subprocess.PIPE).communicate()
      
    numeroDePuntos = buscarNumeroDePuntos(stdout)
    print "Numero de puntos en la capa:",numeroDePuntos
     
    # app = QgsApplication([],True)
    app = QgsApplication([],True)
    app.setPrefixPath(qgisPath, True)
    app.initQgis()
    
    sys.path.append("/usr/share/qgis/python/plugins")
    from processing.core.Processing import Processing
    Processing.initialize()
    import processing

    processing.runalg("grass7:v.surf.idw",os.path.join(raiz,"procesos/estaciones_24hr.shp"),numeroDePuntos,2,"precipitac",False,"358437.822632,800904.643622,1586906.09172,2082253.19389",2000,-1.000000,0.000100,os.path.join(raiz,"procesos/interpolacion_24hr.tif"))        
    app.exitQgis()
    app.exit()
def tendencia(niveles_agua):
    niveles_agua_split = niveles_agua.split(",")    

    if len(niveles_agua_split)>=2:
        actual = float(niveles_agua_split[0])
        anterior = float(niveles_agua_split[1])
        if actual>anterior:
            return "aumento"
        elif actual<anterior:
            return "disminuyó"
        else:
            return "estable"

    return None
    
def crearJsonNivelAgua():    
    query = """
        SELECT nombre,estado,corriente,cuenca,prevencion,alerta,emergencia,nivel_agua,
            (SELECT nivel_agua FROM datos_estaciones WHERE fecha_hora_nivel_agua=datetime(niveles_agua.fecha_hora_captura,"-24 hours") AND id_fk=niveles_agua.id_fk) AS dia_anterior,
            tendencia,
            CASE 
                WHEN  nivel_agua>=prevencion AND nivel_agua<alerta THEN 'prevencion' 
                WHEN nivel_agua>=alerta AND nivel_agua<emergencia THEN 'alerta' 
                WHEN nivel_agua>=emergencia THEN 'emergencia' 
                ELSE 'normal'
            END AS clasificacion,
            CASE 
                WHEN  nivel_agua>=prevencion AND nivel_agua<alerta THEN prevencion-nivel_agua
                WHEN nivel_agua>=alerta AND nivel_agua<emergencia THEN alerta-nivel_agua
                WHEN nivel_agua>=emergencia THEN emergencia-nivel_agua
                ELSE 0
            END AS 'deferencia',
            calcularGastoSqlite(id_estacion,nivel_agua) AS gasto
        FROM estaciones
        INNER JOIN escalas_estaciones
        ON estaciones.id = escalas_estaciones.id_fk
        LEFT JOIN 
                ( 
                SELECT 
                    id_fk,
                    CASE
                        WHEN  total==1 THEN fechas_hora_captura
                        ELSE SUBSTR(fechas_hora_captura,0,INSTR(fechas_hora_captura,',')) 
                    END AS fecha_hora_captura,
                    CASE 
                        WHEN total==1 THEN CAST(niveles_agua AS FLOAT)
                        ELSE CAST(SUBSTR(niveles_agua,0,INSTR(niveles_agua,',')) AS FLOAT) 
                    END AS nivel_agua,
                    tendenciaSqlite(niveles_agua) AS tendencia
                FROM (
                    SELECT id_fk,GROUP_CONCAT(fecha_hora_nivel_agua) AS fechas_hora_captura,GROUP_CONCAT(nivel_agua) AS niveles_agua,COUNT(*) AS total
                    FROM (
                        SELECT id_fk,fecha_hora_nivel_agua,nivel_agua
                        FROM datos_estaciones
                        WHERE nivel_agua IS NOT NULL AND nivel_agua>0 AND  DATETIME(fecha_hora_captura) > DATETIME('now','-3 hours')
                        ORDER BY id_fk,DATETIME(fecha_hora_captura) DESC)
                    GROUP BY id_fk)
                WHERE id_fk IN (SELECT id_fk FROM escalas_estaciones))AS niveles_agua
        ON escalas_estaciones.id_fk = niveles_agua.id_fk
        WHERE nombre IS NOT 'Boca del Cerro'
        ORDER BY grado,nombre ASC"""

    try:
        with db.connect(bDatosPath) as con:
            con.create_function("calcularGastoSqlite",2,calcularGasto)
            con.create_function("tendenciaSqlite",1,tendencia)
            cur = con.cursor()
            cur.execute(query)
            data = cur.fetchall()

            listaRegistros = []
            for r in data:
                d = {                     
                        "nombre":r[0],
                        "estado":r[1],
                        "corriente":r[2],
                        "cuenca":r[3],
                        "prevencion":r[4],
                        "alerta":r[5],
                        "emergencia":r[6],
                        "nivel_agua":r[7],
                        "dia_anterior":r[8],
                        "tendencia":r[9],
                        "clasificacion":r[10],
                        "diferencia":r[11],
                        "gasto":r[12]                        
                }                
                listaRegistros.append(d)
            fw = open(os.path.join(raiz,"procesos/niveles_agua_estaciones.json"),"w")
            fw.write(json.dumps(listaRegistros)) 
            fw.close()
                    
    except db.Error, e:
        print "Error %s:" % e.args[0] 

def crearRenderCortePoligonoPNG():
        ogrinfoInstruccion = [
                              "gdalinfo",
                              "-stats",
                              os.path.join(raiz,"procesos/interpolacion_24hr.tif")]
        stdout, stderr = subprocess.Popen(
                                          " ".join(ogrinfoInstruccion),
                                          shell=True,stdout=subprocess.PIPE, 
                                          stderr=subprocess.PIPE).communicate()
                                          
        max,min = buscarMaximoMinimo(stdout)
        salto = (max-min)/10.0
        
        gdalwarpInstruccion = [
                        "gdalwarp",
                        "-overwrite",
                        "-of",
                        "GTiff",
                        "-dstnodata",
                        "-9999",
                        "-cutline",
                        os.path.join(raiz,"poligono_corte.shp"),
                        "-crop_to_cutline",
                        os.path.join(raiz,"procesos/interpolacion_24hr.tif"),
                        os.path.join(raiz,"procesos/corte_interpolacion_24hr.tif")]  
      
        stdout, stderr = subprocess.Popen(
                                          " ".join(gdalwarpInstruccion),
                                          shell=True,
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE).communicate()        
        
        print "Creado interlavos de colores"        
        print "minino y maximo:",min,max
        print "salto:%.6f" % salto
        
        colores = [
            "164 0 38",
            "215 48 39",
            "244 109 67",
            "253 174 97",
            "254 224 144",
            "224 243 248",
            "171 217 233",
            "116 173 209",
            "69 117 180",
            "49 54 149"]
            
        renglones = []
        for i in range(10):
            r = "%s %s" % (min+salto*i,colores[i])
            print r
            renglones.append(r+"\n")
        fw = open(os.path.join(raiz,"colores_interpolacion.txt"),"w")
        renglones.append("-9999 255 255 255\n")        
        renglones.reverse()
        fw.writelines(renglones)    
        fw.close()
        
        gdaldemInstruccion = [
                              "gdaldem",
                              "color-relief",
                              os.path.join(raiz,"procesos/corte_interpolacion_24hr.tif"),
                              os.path.join(raiz,"colores_interpolacion.txt"),
                              os.path.join(raiz,"procesos/render_corte_interpolacion_24hr.tif")]
        
        stdout, stderr = subprocess.Popen(
                                          " ".join(gdaldemInstruccion),
                                          shell=True,
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE).communicate()  
        
        gdalTranslateInstruccion = [
                                    "gdal_translate",
                                    "-a_nodata",
                                    "255",                    
                                    "-of",
                                    "PNG",
                                    os.path.join(raiz,"procesos/render_corte_interpolacion_24hr.tif"),
                                    os.path.join(raiz,"procesos/render_corte_interpolacion_24hr.png")]
      
        stdout, stderr = subprocess.Popen(
                                          " ".join(gdalTranslateInstruccion),
                                          shell=True,
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE).communicate()
                                          
def crearCorteCurvas():
    ogrinfoInstruccion = [
                          "gdalinfo",
                          "-stats",
                          os.path.join(raiz,"procesos/interpolacion_24hr.tif")]
    stdout, stderr = subprocess.Popen(
                                      " ".join(ogrinfoInstruccion),
                                      shell=True,stdout=subprocess.PIPE, 
                                      stderr=subprocess.PIPE).communicate()
    
    max,min = buscarMaximoMinimo(stdout)
    print "maximo y minimo",min,max
    
    tIntervalo = max - min
    salto =  tIntervalo/10.0
    print "intervalo y salto",tIntervalo,"%.3f" % salto
    
    gdalContourInstruccion = [
                              "gdal_contour",
                              "-a",
                              "pmm",
                              "-i",
                              "%.3f" % salto,
                              os.path.join(raiz,"procesos/interpolacion_24hr.tif"),
                              os.path.join(raiz,"procesos/interpolacion_curvas.shp")]
    
    for f in glob.glob(os.path.join(raiz,"procesos/interpolacion_curvas.*")):
        os.remove(f)
     
    stdout, stderr = subprocess.Popen(
                                      " ".join(gdalContourInstruccion),
                                      shell=True,stdout=subprocess.PIPE, 
                                      stderr=subprocess.PIPE).communicate()
          
    ogr2ogrInstruccion = [
                          "ogr2ogr",
                          "-overwrite",
                          "-clipsrc",
                          os.path.join(raiz,"poligono_corte.shp"),
                          os.path.join(raiz,"procesos/corte_interpolacion_curvas.shp"),
                          os.path.join(raiz,"procesos/interpolacion_curvas.shp")]
               
    stdout, stderr = subprocess.Popen(
                                      " ".join(ogr2ogrInstruccion),
                                      shell=True,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE).communicate()                                      
     
     
     
    ogr2ogrInstruccion = [
                   "ogr2ogr",
                   "-f",
                   "GeoJSON",
                   "-t_srs",
                   "EPSG:4326",
                   os.path.join(raiz,"procesos/corte_interpolacion_curvas.geojson"),
                   os.path.join(raiz,"procesos/corte_interpolacion_curvas.shp")]
     
    if os.path.exists(os.path.join(raiz,"procesos/corte_interpolacion_curvas.geojson")):
        os.remove(os.path.join(raiz,"procesos/corte_interpolacion_curvas.geojson"))
     
    stdout, stderr = subprocess.Popen(
                                      " ".join(ogr2ogrInstruccion),
                                      shell=True,stdout=subprocess.PIPE, 
                                      stderr=subprocess.PIPE).communicate()

def calcularGasto(id_estacion,nivel):
    try:
        if nivel:
    #         Samaria
            if id_estacion == "000000GR03":
                return round(((171.79*(nivel**2))-(4222.5*nivel)+26050),0)
    #         Oxolotan    
            elif id_estacion =="000000OXOL":
                return round(((72.158*(nivel**2))-(4830.5*nivel)+80819),0)
    #         Puyacatengo
            elif id_estacion =="000000GR06":
                return round((61.338*(nivel**2))-(3008.7*nivel)+36882,0)
    #         Tapijulapa
            elif id_estacion =="000000GR05":
                return round((44.46*(nivel**2))-(1452.2*nivel)+11879,0)
    #         San Joaquin
            elif id_estacion =="000000GR04":
                return round((8.5467*(nivel**2))-(312.84*nivel)+2867.6,0)
    #         Teapa
            elif id_estacion =="000000GR07":
                return round((66.093*(nivel**2))-(4528.1*nivel)+77567,0)
    #         Gonzalez
            elif id_estacion =="000000GR02":
                return round(((5.9531*(nivel**2))+(57.973*nivel)-228.92),0)
    #         Pueblo Nuevo
            elif id_estacion =="0000000G02":
                return round((17.697*(nivel**2))-(70.32*nivel)+125.08,0)
    #         Gaviotas
            elif id_estacion =="0000000G01":
                return round((20.054*(nivel**2))+(20.72*nivel)-1.5857,0)
    #         Porvenir
            elif id_estacion =="000000GR01":
                return round((33.434*(nivel**2))+(7.3098*nivel)+272.53,0)
    #         Salto de Agua
            elif id_estacion =="0000000G09":
                return round((3.44*(nivel**2))+(11.741*nivel)+5.7261,0)
    #         Boca del Cerro
            elif id_estacion =="0000000G08":
                return round((16.632*(nivel**2))+(21.216*nivel)-1699.3,0)
    #         San Pedro
            elif id_estacion =="SPEDR27040":
                return round((-124.31*(nivel**2))+(2623.5*nivel)-13223,0)
    except:
        print id_estacion,nivel    
    return None 

def precipitacionEstandar(inicio,final,archJson):
    # query = """   
    #         SELECT id_estacion,tipo,nombre,estado,SUM(precipitacion) AS precipitacion,                
    #             CASE    
    #                     WHEN SUM(precipitacion)>= 0.1 AND SUM(precipitacion)<25.1 THEN 'lluvias aisladas' 
    #                     WHEN SUM(precipitacion)>= 25.1 AND SUM(precipitacion)<50.1 THEN 'chubascos con tormenta fuertes' 
    #                     WHEN SUM(precipitacion)>= 50.1 AND SUM(precipitacion)<75.1 THEN 'chubascos con tormenta fuertes muy fuertes' 
    #                     WHEN SUM(precipitacion)>= 75.1 AND SUM(precipitacion)<150.1 THEN 'tormentas intensas' 
    #                     WHEN SUM(precipitacion)>= 150.1 AND SUM(precipitacion)<250.1 THEN 'torrenciales'
    #                     WHEN SUM(precipitacion)>=250.1 THEN 'extraordinarios' 
    #             END AS clasificacion,
    #             CASE 
    #                     WHEN SUM(precipitacion)>= 0.1 AND SUM(precipitacion)<25.1 THEN 1
    #                     WHEN SUM(precipitacion)>= 25.1 AND SUM(precipitacion)<50.1 THEN 2
    #                     WHEN SUM(precipitacion)>= 50.1 AND SUM(precipitacion)<75.1 THEN 3
    #                     WHEN SUM(precipitacion)>= 75.1 AND SUM(precipitacion)<150.1 THEN 4
    #                     WHEN SUM(precipitacion)>= 150.1 AND SUM(precipitacion)<250.1 THEN 5
    #                     WHEN SUM(precipitacion)>=250.1 THEN 6
    #             END AS posicion
    #         FROM estaciones
    #         INNER JOIN datos_estaciones
    #         ON estaciones.id = datos_estaciones.id_fk
    #         WHERE 
    #             precipitacion IS NOT NULL AND 
    #             precipitacion > 0 AND 
    #             DATETIME(fecha_hora_precipitacion)>=DATETIME('{inicio}') AND 
    #             DATETIME(fecha_hora_precipitacion)<DATETIME('{final}')
    #         GROUP BY id_estacion
    #         ORDER BY posicion DESC,precipitacion DESC
    #         """.format(inicio=inicio, final=final)
    
    query = """   
            SELECT id_estacion,tipo,nombre,estado,SUM(precipitacion) AS precipitacion,                
                CASE    
                        WHEN SUM(precipitacion)>= 0.1 AND SUM(precipitacion)<25.1 THEN 'lluvias aisladas' 
                        WHEN SUM(precipitacion)>= 25.1 AND SUM(precipitacion)<50.1 THEN 'chubascos con tormenta fuertes' 
                        WHEN SUM(precipitacion)>= 50.1 AND SUM(precipitacion)<75.1 THEN 'chubascos con tormenta fuertes muy fuertes' 
                        WHEN SUM(precipitacion)>= 75.1 AND SUM(precipitacion)<150.1 THEN 'tormentas intensas' 
                        WHEN SUM(precipitacion)>= 150.1 AND SUM(precipitacion)<250.1 THEN 'torrenciales'
                        WHEN SUM(precipitacion)>=250.1 THEN 'extraordinarios' 
                END AS clasificacion,
                CASE 
                        WHEN SUM(precipitacion)>= 0.1 AND SUM(precipitacion)<25.1 THEN 1
                        WHEN SUM(precipitacion)>= 25.1 AND SUM(precipitacion)<50.1 THEN 2
                        WHEN SUM(precipitacion)>= 50.1 AND SUM(precipitacion)<75.1 THEN 3
                        WHEN SUM(precipitacion)>= 75.1 AND SUM(precipitacion)<150.1 THEN 4
                        WHEN SUM(precipitacion)>= 150.1 AND SUM(precipitacion)<250.1 THEN 5
                        WHEN SUM(precipitacion)>=250.1 THEN 6
                END AS posicion
            FROM estaciones
            INNER JOIN datos_estaciones
            ON estaciones.id = datos_estaciones.id_fk
            WHERE 
                precipitacion IS NOT NULL AND 
                precipitacion > 0 AND
                DATETIME(fecha_hora_precipitacion)>=DATETIME('{inicio}') AND  
                DATETIME(fecha_hora_precipitacion)<DATETIME('{final}')
            GROUP BY id_estacion
            ORDER BY posicion DESC,precipitacion DESC""".format(inicio=inicio,final=final)
    
    try:
        with db.connect(bDatosPath) as con:
            cur = con.cursor()
            cur.execute(query)
            data = cur.fetchall()
            
            listaRegistros = []
            for r in data:
                d = {
                     "id_estacion":r[0],
                     "tipo":r[1],
                     "nombre":r[2],
                     "estado":r[3],
                     "precipitacion":r[4],                     
                     "clasificacion":r[5]
                 }                     
                listaRegistros.append(d)
            fw = open(os.path.join(raiz,archJson),"w")
            fw.write(json.dumps(listaRegistros)) 
            fw.close()
                    
    except db.Error, e:
        print "Error %s:" % e.args[0]

def fechaFinal():
    fechaEventoActual = datetime.datetime.now()
    fechaFinal = datetime.datetime(fechaEventoActual.year, fechaEventoActual.month, fechaEventoActual.day) + datetime.timedelta(hours=8)
    if fechaEventoActual<fechaFinal:
        fechaFinal = fechaFinal - datetime.timedelta(hours=24)
    return fechaFinal 

def ejecutarTodo(final):
    print "Creando GeoJSon Precipitacion"
    crearGeoJsonPrecipitacion(final)
    print "Creando JSon Precipitacion"
    crearJsonPrecipitacion(final)
    print "Creando Interpolacion"
    crearInterpolacion()
    print "Creando Curvas"
    crearCorteCurvas()
    print "Creando PNG"
    crearRenderCortePoligonoPNG()
    print "Creando Json Nivel del Agua"
    crearJsonNivelAgua()
                        
    inicio =  final - datetime.timedelta(days=1)
    print "Creado 24hr estandar"
    precipitacionEstandar(inicio,final,"procesos/preciptacion_24_estandar.json")
    inicio =  final - datetime.timedelta(days=2)
    print "Creado 48hr estandar"
    precipitacionEstandar(inicio,final,"procesos/preciptacion_48_estandar.json")
    inicio =  final - datetime.timedelta(days=3)
    print "Creado 72hr estandar"
    precipitacionEstandar(inicio,final,"procesos/preciptacion_72_estandar.json")
    
    final =  final + datetime.timedelta(days=1)
    
    rutas =[
            os.path.join(raiz,"procesos/preciptacion_24_estandar.json"),
            os.path.join(raiz,"procesos/preciptacion_48_estandar.json"),
            os.path.join(raiz,"procesos/preciptacion_72_estandar.json")
    ]
    for r in rutas:
        if os.path.exists(r):
            shutil.copy(r,copiarA)
        else:
            print "no existe: ",r
         
    rutas =[
            os.path.join(raiz,"procesos/estaciones_24hr.geojson"),
            os.path.join(raiz,"procesos/estaciones_24hr.json"),            
            os.path.join(raiz,"procesos/niveles_agua_estaciones.json"),
            os.path.join(raiz,"procesos/render_corte_interpolacion_24hr.png"),
            os.path.join(raiz,"procesos/corte_interpolacion_curvas.geojson"),
            os.path.join(raiz,"procesos/fecha.txt")]
    
    for r in rutas:
        if os.path.exists(r):
            shutil.copy(r,copiarA)
        else:
            print "no existe: ",r
    
    return final
            
if __name__ == '__main__':
    print "inicio"

    ejecutarTodo(fechaFinal())

    print "fin..........."

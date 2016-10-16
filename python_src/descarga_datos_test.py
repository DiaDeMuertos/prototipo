# -*- coding: utf-8 -*-
import urllib2
from xml.dom import minidom
from pyspatialite import dbapi2 as db
import datetime
import random
import re
import threading
import thread
import sys
import subprocess
import os
import shutil

nombreBD = "dbMonitoreo.sqlite"
bDatosPath = os.path.join(os.path.dirname(__file__),"..","bd",nombreBD)
raiz = os.path.join(os.path.dirname(__file__),"capas")

def textoToTextoQuery(cadena):
    if cadena:
        cadena = "'"+cadena+"'"
    return cadena

def esNumero(numero):
    if re.match(r"^-?\d$|-?\d\.?\d*$|-?\d*\.?\d$","."):
        return True
    return False

def datosValidos(datos):
    for k in datos:
        if datos[k]["dato"] is not None:
            return True
    return False

def noneToNull(valor):
    return valor if valor else "NULL"

def listaSubConjunto(lista,nElementos):
    subConjunto = []
    while len(subConjunto)!=nElementos:
        elemento = random.choice(lista)
        if elemento not in subConjunto:
            subConjunto.append(elemento)
            lista.remove(elemento)
        else:
            print elemento,"repetido"            
    return subConjunto

def cambiarFormato(fecha_hora):
    bloques = fecha_hora.split(" ") 
    fecha = bloques[0]
    hora = bloques[1]
    
    d,m,y = [int(e) for e in fecha.split("/")]
    H,M,S = [int(e) for e in hora.split(":")]
    
    return datetime.datetime(y,m,d,H,M,S).strftime("%Y-%m-%d %H:%M:%S")

def descargaTexto(url,nombreArchivo):
    response = urllib2.urlopen(url)
    xmlTexto = response.read()
    response.close()
      
    fw = open(nombreArchivo,"w")
    fw.write(xmlTexto)
    fw.close()

def separarDatos(cadena):
    conjunto = {"fecha":"","dato":""}
    if cadena!="":
        cadenaSplit = cadena.split(",")
        subCadenaSplit = cadenaSplit[1].split(" ")
        conjunto["fecha"] = cambiarFormato(cadenaSplit[0])
        conjunto["dato"] = subCadenaSplit[0].replace("[","").replace("]","")
    else:
        conjunto["fecha"] = None
        conjunto["dato"] = None
    return conjunto

def limpiarFormatear(cadena):
    if cadena!="No hay dato":
        cadenasLimpias = []
        cadenasLimpias.append(cadena[:cadena.find("-")])
        cadenasLimpias.append(cadena[cadena.find("-")+1:])
        return unicode("%s,%s"  % (cadenasLimpias[0].rstrip().lstrip(),cadenasLimpias[1].rstrip().lstrip()))
    else:
        return unicode("")

def validarTomarValor(nelemento,padre):
    elemento = padre.getElementsByTagName(nelemento)
    if len(elemento)!=0:
        elemento = "No hay dato" if elemento[0].firstChild is None else elemento[0].firstChild.data
    else:
        return "No hay dato"  
    return elemento

def leerArchivoXML(nombreArchivo):
    datos = {}   
    doc = minidom.parse("salida_id.xml")
    dcp = doc.getElementsByTagName("Dcp")                 
    for d in dcp:
        datos["gasto_operativo"] = separarDatos(limpiarFormatear(validarTomarValor("GO",d)))
        datos["humedad_relativa"] = separarDatos(limpiarFormatear(validarTomarValor("HR",d)))   
        datos["nivel_agua"] = separarDatos(limpiarFormatear(validarTomarValor("NV",d)))
        datos["precipitacion"] = separarDatos(limpiarFormatear(validarTomarValor("PNE",d)))        
        
        # Fix Tempora-----------------------------------------
        tempUltimaHora = validarTomarValor("PNES",d)
        if(tempUltimaHora!="No hay dato"):
            indiceUltimaHora = tempUltimaHora.find("Ultimas 01 hr:")
            if(indiceUltimaHora>=0):
                datos["precipitacion"]["dato"] = tempUltimaHora[indiceUltimaHora:].split(':')[1].strip().split(" ")[0]
            else:
                datos["precipitacion"]["dato"] = None
        else:
            datos["precipitacion"]["dato"] = None
        # ----------------------------------------------------    
        datos["velocidad_causal"] = separarDatos(limpiarFormatear(validarTomarValor("VC",d)))
        
        print "Gasto operativo:%s" % datos["gasto_operativo"]
        print "Humedad relativa: %s" % datos["humedad_relativa"]
        print "Nivel de agua:%s" % datos["nivel_agua"]
        print "Precipitacion: %s" % datos["precipitacion"]
        print "velocidad del causal: %s" % datos["velocidad_causal"]
    return datos     

def datosToBaseDatos(datos,i):
    if datosValidos(datos)==False:
        return
    try:
        with db.connect(bDatosPath) as con:
            cur = con.cursor()
            queryTemplate = "SELECT id FROM estaciones WHERE id_estacion={id_estacion}"
            query =  queryTemplate.format(id_estacion = textoToTextoQuery(i))
            cur.execute(query)
            data = cur.fetchone()
            
            if data:
                id = data[0]
                for k in datos:
                    if datos[k]["fecha"]:
                        queryTemplate = "SELECT * FROM datos_estaciones WHERE id_fk={id} AND fecha_hora_{columaEstacion}={fechaHora}"
                        query = queryTemplate.format(id = id, columaEstacion = k,fechaHora = textoToTextoQuery(datos[k]["fecha"]))
                        print query
                        cur.execute(query)
                        data = cur.fetchall()
                        if data: 
                            print data
                            datos[k]["fecha"] = None
                            datos[k]["dato"] = None
                 
                if datosValidos(datos)==False:
                    print "Conjunto de eventos existentes"
                    return                               
                
                col = "fecha_hora_gasto_operativo,gasto_operativo,fecha_hora_humedad_relativa,humedad_relativa,fecha_hora_nivel_agua,nivel_agua,fecha_hora_precipitacion,precipitacion,fecha_hora_velocidad_causal,velocidad_causal"
                queryTemplate = "INSERT INTO datos_estaciones (id_fk,fecha_hora_captura,%s) " % (col)
                queryTemplate+= "VALUES({id},DATETIME('now'),{fecha_hora_gasto_operativo},{gasto_operativo},{fecha_hora_humedad_relativa},{humedad_relativa},{fecha_hora_nivel_agua},{nivel_agua},{fecha_hora_precipitacion},{precipitacion},{fecha_hora_velocidad_causal},{velocidad_causal})"
    
                query =  queryTemplate.format(
                                    id = id,
                                    fecha_hora_gasto_operativo = noneToNull(textoToTextoQuery(datos["gasto_operativo"]["fecha"])),
                                    gasto_operativo = noneToNull(datos["gasto_operativo"]["dato"]),
                                    fecha_hora_humedad_relativa = noneToNull(textoToTextoQuery(datos["humedad_relativa"]["fecha"])),
                                    humedad_relativa = noneToNull(datos["humedad_relativa"]["dato"]),
                                    fecha_hora_nivel_agua = noneToNull(textoToTextoQuery(datos["nivel_agua"]["fecha"])),
                                    nivel_agua = noneToNull(datos["nivel_agua"]["dato"]),                                    
                                    precipitacion = noneToNull(datos["precipitacion"]["dato"]),
                                    fecha_hora_precipitacion = noneToNull(textoToTextoQuery(datos["precipitacion"]["fecha"])),
                                    velocidad_causal = noneToNull(datos["velocidad_causal"]["dato"]),
                                    fecha_hora_velocidad_causal = noneToNull(textoToTextoQuery(datos["velocidad_causal"]["fecha"])))
                print query
                cur.execute(query)               
    except db.Error, e:
        print "Error %s:" % e.args[0] 

def descargarListaAccion(listaIds):
    urlTemplate = "http://201.116.60.82/DCPdata/{id}_LastHoursData.xml"   
    for i in listaIds:
        url = urlTemplate.format(id = i)
        print url
        descargaTexto(url,"salida_id.xml")      
        datos = leerArchivoXML("salida_id.xml")      
        datosToBaseDatos(datos,i)
        print "--------------------------"

if __name__ == '__main__':
    print " "*5 + "inicio descarga_datos"
    listaIds = []
    
    with open("lista_estaciones.csv") as f:
        for l in f.readlines():
            listaIds.append(l.rstrip().lstrip())
    descargarListaAccion(listaIds)

    print " "*5 + "fin descarga_datos"
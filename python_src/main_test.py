# -*- coding: utf-8 -*-
import os
import threading
import datetime
import subprocess

qgisPath = r"/usr"
raiz = os.path.join(os.path.dirname(__file__),"capas")
copiarA = os.path.join(os.path.dirname(__file__),"..","web_src")
nombreBD = "dbMonitoreo.sqlite"
bDatosPath = os.path.join(os.path.dirname(__file__),"..","bd",nombreBD)

class myThread (threading.Thread):
    def __init__(self, threadID,name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
    def run(self):
        print "Iniciando " + self.name + " " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        stdout, stderr = subprocess.Popen('python descarga_datos_test.py',shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE).communicate()        
        
        archivoStdout = open(os.path.join(raiz,"procesos/stdout_descarga_datos.txt"),"w")
        archivoStdout.write(stdout)    
        archivoStdout.close()
        
        archivoStderr = open(os.path.join(raiz,"procesos/stderr_descarga_datos.txt"),"w")
        archivoStderr.write(stderr)    
        archivoStderr.close()

        if stderr=="":
            stdout, stderr = subprocess.Popen('python crear_capas_test.py',shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE).communicate()
            
            if stderr=="":
                archivoStdout = open(os.path.join(raiz,"procesos/stdout_crear_capas.txt"),"w")
                archivoStdout.write(stdout)    
                archivoStdout.close()
                
                archivoStderr = open(os.path.join(raiz,"procesos/stderr_crear_capas.txt"),"w")
                archivoStderr.write(stderr)    
                archivoStderr.close()
            else:
                print "error en bajar datos ver ",os.path.join(raiz,"procesos/stderr_crear_capas.txt")
        else:
            print "error en bajar datos ver ",os.path.join(raiz,"procesos/stderr_descarga_datos.txt")        
        print "Terminando " + self.name + " " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
if __name__ == '__main__':
    print "inicio"
    
    primeraVez = True
    minutos = 10
    tiempoInicial = datetime.datetime.now()

    while True:          
        try:
            tiempoActual = datetime.datetime.now()
            if(tiempoInicial + datetime.timedelta(minutes = minutos))< tiempoActual or primeraVez:
                primeraVez = False
                tiempoInicial = tiempoActual
                threadProcesos = myThread(1,"Thread #1")
                threadProcesos.start()
                while True:
                    if threadProcesos.is_alive()==False:                    
                        raise Exception("a finalizado")                      
        except KeyboardInterrupt:                
            print "\n...CERRANDO..."
            break                    
        except Exception as e:
            print "thread:",e
            print "ctrl + c para cerrar"
    print "fin..........."
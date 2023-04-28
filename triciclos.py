from pyspark import SparkContext
import sys


def filtro1(linea):
    dos_nodos=linea.split(',')
    n1=dos_nodos[0]
    n2=dos_nodos[1]
    if n1<n2:
        return (n1,n2)
    elif n2>n1:
        return (n2,n1)
    else:
        pass
    
def f1(nodo_lista):
    nodo=nodo_lista[0]
    lista=nodo_lista[1]
    tricilos=[]
    for i in range(len(lista)):
        n=lista[i]
        tricilos.append(((nodo,n),'exists'))
        for j in range(i+1, len(lista)):
            tricilos.append(((n,lista[j]),('pending', nodo)))
    return tricilos


def main(sc, filename): 
    grafo=sc.textFile(filename)
    gb=grafo.map(filtro1).distinct().filter(lambda x: x!=None)
    adyacentes=gb.groupByKey().map(lambda x: (x[0],sorted(list(x[1])))).sortByKey()
    triciclos=adyacentes.flatMap(f1).groupByKey()
    lista_triciclos=[]
    for pareja, mensajes in triciclos.collect():
        mens=list(mensajes)
        if len(mens)>1 and 'exists' in mens:
            for m in mens:
                if m!='exists': 
                    lista_triciclos.append((m[1],pareja[0],pareja[1]))
    print("Lista de triciclos", sorted(lista_triciclos))

if __name__=="__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        with SparkContext() as sc:
            sc.setLogLevel("ERROR")
            main(sc, sys.argv[1])
from multiprocessing import Process
from multiprocessing import BoundedSemaphore, Semaphore, Lock
from multiprocessing import current_process
from multiprocessing import Value, Array
from time import sleep
from random import random


N = 20
NPROD = 3
K = 1

def delay(factor = 3):
    sleep(random()/factor)
"""
Añade, con exclusión mutua, datos a la variable compartida en el storage

"""
def minimo(lista):
    new_list = []
    for i in lista:
        if i >= 0:
            new_list.append(i)
    return min(new_list)

def add_data(storage, index, data, mutex):
    mutex.acquire()
    try:
        storage[index.value] = data
        index.value += 1
        print(f"dato añadido: {data}")
        delay()
    finally:
        mutex.release()

"""
Toma datos de storage
"""
def get_data(storage, index, mutex):
    mutex.acquire()
    index.value = index.value - 1
    minim = storage[0]
    pos = 0 
    try:
        """
        Hallamos el minimo de los elementos en el storage
        """
        for i in range(index.value):
            if minim % 1000 > storage[i] % 1000:
                minim  = storage[i]
                pos = i
        
        temp = storage[index.value]
        storage[index.value] = -2
        storage[pos] = temp
        
        data = minim % 1000
        pid = minim // 1000
            
    finally:
        mutex.release()
    return data, pid


def producer(storage, index, empty, non_empty, mutex):
    pid = int(current_process().name.split('_')[1])

    for v in range(N):
        print(f"proceso {pid} produciendo")
        empty.acquire()
        add_data(storage, index, 1000*pid + v, mutex)
        print(f"proceso {pid} ha producido")
        non_empty.release()
    empty.acquire()
    add_data(storage, index, -1, mutex)
    non_empty.release()


def merge(storage, index, index2, non_empty, empty, mutex, result):
    for i in range(NPROD):
        non_empty[i].acquire()
    for v in range(N * NPROD):
        dato, pid = get_data(storage, index, mutex)
        result[index2.value] = dato
        index2.value += 1
        empty[pid].release()
        non_empty[pid].acquire()
        delay()
        
        

def main():
    result = Array('i', NPROD * N) 
    storage = Array('i', NPROD)
    index2 = Value('i', 0)
    index = Value('i', 0)
    for i in range(NPROD * N):
        result[i] = -2
    for i in range(NPROD):
        storage[i] = -2
        
    empty = [0]*NPROD
    non_empty = [0]*NPROD
    
    for i in range(NPROD):
        non_empty[i] = Semaphore(0)
        empty[i] = BoundedSemaphore(K)
    mutex = Lock()

    prodlst = [ Process(target=producer,
                        name=f'prod_{i}',
                        args=(storage, index, empty[i], non_empty[i], mutex))
                for i in range(NPROD) ]
    merge_process = [ Process(target=merge,
                              name=f'merge',
                              args=(storage, index, index2, non_empty, empty, mutex, result))]

    for p in prodlst + merge_process:
        p.start()

    for p in prodlst + merge_process:
        p.join()
    print(list(result))


if __name__ == '__main__':
    main()

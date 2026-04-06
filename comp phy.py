import threading
import random

LOWER_NUM = 1
UPPER_NUM = 10000
BUFFER_SIZE = 100
MAX_COUNT = 10000

buffer = []

# Synchronization
mutex = threading.Lock()
empty = threading.Semaphore(BUFFER_SIZE)
full = threading.Semaphore(0)

produced_count = 0
consumed_count = 0

# Files
all_file = open("all.txt", "w")
even_file = open("even.txt", "w")
odd_file = open("odd.txt", "w")

def producer():
    global produced_count
    for _ in range(MAX_COUNT):
        num = random.randint(LOWER_NUM, UPPER_NUM)

        empty.acquire()
        mutex.acquire()

        buffer.append(num)
        all_file.write(str(num) + "\n")
        produced_count += 1

        mutex.release()
        full.release()

def even_consumer():
    global consumed_count
    while True:
        full.acquire()
        mutex.acquire()

        if len(buffer) > 0:
            num = buffer[-1]  # peek top
            if num % 2 == 0:
                buffer.pop()
                even_file.write(str(num) + "\n")
                consumed_count += 1
                mutex.release()
                empty.release()
            else:
                mutex.release()
                full.release()
        else:
            mutex.release()
            full.release()

        if consumed_count >= MAX_COUNT:
            break

def odd_consumer():
    global consumed_count
    while True:
        full.acquire()
        mutex.acquire()

        if len(buffer) > 0:
            num = buffer[-1]  # peek top
            if num % 2 == 1:
                buffer.pop()
                odd_file.write(str(num) + "\n")
                consumed_count += 1
                mutex.release()
                empty.release()
            else:
                mutex.release()
                full.release()
        else:
            mutex.release()
            full.release()

        if consumed_count >= MAX_COUNT:
            break


# Threads
t1 = threading.Thread(target=producer)
t2 = threading.Thread(target=even_consumer)
t3 = threading.Thread(target=odd_consumer)

t1.start()
t2.start()
t3.start()

t1.join()
t2.join()
t3.join()

all_file.close()
even_file.close()
odd_file.close()

print("Program finished.")
from fasttask import FastTask
from multiprocessing import Queue
app = FastTask()

q = Queue()
app.addBroker('queue', q)

e = Queue()
app.addBroker('queuee', e)

back = Queue()
app.addBackend('queue', back)

q.put({'a': 6, 'b': 7})
q.put((1, 2))
q.put((2, 3))
q.put((3, 4))
q.put((4, 5))
q.put((5, 6))
q.put(('finall', '!!!'))

e.put({'a': 6, 'b': 7})
e.put((1, 2))
e.put((2, 3))
e.put((3, 4))
e.put((4, 5))
e.put((5, 6))


@app.task('queue', 'queue', 2)
def test(a, b):
    return a + b


@app.task('queuee', 'queue', 3)
def test2(a, b):
    return a - b


@app.task('queue', 'queue', 1)
def test3(a, b):
    return a == b

"task run"
app(max_workers=3, loop=asyncio.get_event_loop())

"native run"
# while q.qsize() > 0:
#     param = q.get()
#     if isinstance(param, tuple):
#         res = test(*param)
#     if isinstance(param, dict):
#         res = test(**param)
#     print('res', res)



# app.join()
# print("app block")

print("app not block")

import time
print("sleep 2 sec")
time.sleep(2)

print("app terminated")
app.terminate()

while back.qsize() > 0:
    print('back res:', back.get())
import Queue
import sys
import threading

def pmap(num_threads, target, args):
    assert num_threads > 0
    inq = Queue.Queue(len(args))
    outq = Queue.Queue(len(args))

    def _worker():
        while True:
            try:
                (i, arg_tuple) = inq.get(block=False)
            except Queue.Empty:
                break
            try:
                result = target(*arg_tuple)
                outq.put((i, True, result))
            except:
                outq.put((i, False, sys.exc_info()[1]))
            finally:
                inq.task_done()

    for i, a in enumerate(args):
        inq.put((i, a))
    if num_threads > 1:
        for _ in xrange(num_threads):
            t = threading.Thread(target=_worker)
            t.start()
        inq.join()
    else:
        _worker()
    results = [outq.get() for _ in xrange(outq.qsize())]
    results.sort()
    for r in results:
        if not r[1]:
            raise r[2]
    return [r[2] for r in results]


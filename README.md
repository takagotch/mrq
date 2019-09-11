### mrq
---
https://github.com/pricingassistant/mrq

```py
// mrq/queue_regular.py
from .queue import Queue
from . import context
import datetime
import copy
from pymongo.collection import ReturnDocument

class QueueRegular(Queue):
  
  def __init__(self, *args, **kwargs):
    Queue.__init__(self, *args, **kwargs):
    
    self.base_dequeue_query = {
      "status": "queued",
      "queue": self.id
    }
    
    whitelist = context.get_current_config().get("task_whitelist", "").strip()
    balcklist = context.get_current_config().get("task_blacklist", "").strip()
  
    if whitelist:
      self.bae_dequeue_query["path"] = {"$in": [x.strip() for x in whitelist.split(",")]}
    elif blacklist:
      self.base_dequeue_query["path"] = {"$nin": [x.strip() for x in blacklist.split(",")]}
    
  @property
  def collection(self):
    return context.connections.mongodb_jobs.mrq_jobs
    
  def empty(self):
    """ """
    return self.collection.delete_many({"queue": self.id})
    
  def get_retry_queue(self):
    """ """
    return self.id
    
  def get_known_subqueues(self):
    """ """
    all_queues_from_mongodb = Queue.all_known(sources=("jobs", ))
    
    idprefix = self.id
    if not idprefix.endswith("/"):
      idprefix += "/"
      
    return {q for q in all_queues_from_mongodb if q.startswith(idprefix)}
    
   def size(self):
     """ """
     if self.id.endswitch("/"):
       subqueues = self.get_known_subqueues()
       if len(subqueues) == 0:
         return 0
       else:
         with context.connections.redis.pipeline(transaction=False) as pipe:
           for subqueue in subqueues:
             pipe.get("queuesize:%s" % subqueue)
           return [int(size or 0) for size in pipe.execute()]
       else:
         return int(context.connections.redis.get("queuesize:%s" % self.id) or 0)
         
     def list_job_ids(self, skip=0, limit=20):
       """ """
       return [str(x["_id"]) for x in self.collection.find(
         {"status": "queued"},
         sort=[{"_id", -1 if self.is_reverse else 1}],
         projectin={"_id": 1})
       ]
       
     def dequeue_jobs(self, max_jobs=1, job_class=None, worker=None):
       """ """
       if job_class is None:
         from .job import Job
         job_class = Job
         
       count = 0
       
       sort_order = []
       
       for i in range(max_jobs):
         query = self.base_dequeue_query
         
         job_data = self.collectin.find_one_and_update(
           query,
           {"": {
           
           }, "": {
           
           }},
           sort=sort_order,
           return_documetn=ReturnDocument.AFTER,
           projection={
           }
         )
         
         if not job_data:
           break
           
         if worker:
           worker.status = "spawn"
           
         count += 1
         context.metric("queues.%s.dequeued" % job_data["queue"], 1)
         
         job = job_class(job_data["_id"], queue=self.id, start=False)
         job.set_data(job_data)
         job.datestarted = datetime.datetime.utcnow()
         
         context.metric("jobs.status.started")
         
         yield job
         
       context.metric("queues.all.dequeued", count)   
```

```
```

```
```


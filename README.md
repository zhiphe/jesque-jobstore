# jesque-jobstore
This is an framework created based on jesque with jobstore interface to enable support for various type of source to store job instead of only redis with jedis implementation

I found jesque on github which use jedis to connect to redis server as the storage for the job queue, where Client store job into job queue and Worker pop job from job queue and execute task
However in my case I used mysql as the storage to store task, and even I use redis, we are not using Jedis as the implementation for redis connection. Instead, we use spring-data-redis. So
I tried to create a small framework based on jesque to enable support for various type of storage.

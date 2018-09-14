# redis_batch_reader
read data in redis in batch with a pattern of key

```python
redis_reader = RedisReader("127.0.0.1")
res = redis_reader.values("*")
print(res)
```

PSLX stores the temporary data including:

1. Snapshots of operators and containers.
2. Log files.
3. RPC requests and responses.

and these temporary data are stored in the path defined py the environment variable `PSLX_DATABASE`, whose
default value is `database/`. To make sure these temporary files do not occupy too much disk, PSLX designs a
retention policy (ttl) for these files by putting everything `ttl=TIME`. We also run a separate ttl clearer to clean
 the files, and the criteria is that, if the last modified time for a file is already `TIME` away from the current time,
 the ttl cleaner will delete it.

The `TIME` in the ttl defined path could be an integer or a string. If this value is an integer, then the unit by default is
day. For string value time, one can specify the unit of `m` as minute, `h` as hour and `d` as day. For example, a `ttl=1m` folder
will guarantee that all the files in the folder that are not updated within the past 1 minute will be deleted if the ttl cleaner sees it.
 
The current ttl cleaner implementation exists in [ttl_cleaner](https://github.com/kfrancischen/pslx/blob/master/pslx/micro_service/ttl_cleaner/ttl_cleaner.py),
there is also an example about how to run the ttl cleaner in the [example](https://github.com/kfrancischen/pslx/blob/master/example/ttl_cleaner_example/ttl_cleaner.py) folder.

```python
from pslx.micro_service.ttl_cleaner.ttl_cleaner import TTLCleaner

if __name__ == "__main__":
    ttl_cleaner = TTLCleaner()
    ttl_cleaner.set_schedule(
        hour='*',
        minute='*/10'
    )
    ttl_cleaner.set_max_instances(max_instances=5)
    ttl_cleaner.execute()
```

Fundamentally, ttl cleaner is designed as an `CronBatchContainer` with container name `PSLX_TTL_CLEANER_OPERATOR`, and like regular
cron jobs, you can specify the hour and minute that the ttl cleaner runs. The above example shows the ttl cleaner runs every hour
at every 10 min. 

Also please make sure your `PSLX_DATABASE` is consistent across your application, because ttl cleaner looks at files
under this folder by default.

ttl cleaner also allows user to specify other folders in addition to `PSLX_DATABASE`. The way to do that is through the following function:

```python
watch_dir(dir_name)
```
* Description: Add other directory to watch.
* Arguments:
    1. dir_name: the directory that wants ttl cleaner to watch and clean.
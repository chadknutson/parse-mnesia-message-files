# parse-mnesia-message-files
python script for extracting messages from rqd or idx files.  Intended for use in python 3.  Not tested with python 2

This might be useful for handling corrupted rdq or idx files in RabbitMQ mnesia data

Options:
```
 -file :  Use full path to the file to be parsed
 -o    :  output is printed to terminal (don't need to set flag), or name of file for output (json data).  Don't provide a file extension here.
```

Example Usage
```
python3 parse_mnesia.py  -file /usr/local/var/lib/rabbitmq/mnesia/rabbit/msg_stores/vhosts/23GGHMUBU0M53JQSJA0D1RLN4/msg_store_persistent/0.rdq -o messagesLost
```

There is minimal error checking, so apologies if you get ugly error messages
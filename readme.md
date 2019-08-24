# peermq

message queue with peer networking

todo: verified hypercore session

# example

``` js
```

first, get the ID of the listening node:

```
$ node mq.js -d /tmp/abc id
4338729847d8d770b6c929ef005be1787da1e8f4b1e2c004c3b549df22eeaf5d
```

send a message to the listening node (that hasn't been started up yet):

```
$ node mq.js -d /tmp/xyz send -m hi \
  --to 4338729847d8d770b6c929ef005be1787da1e8f4b1e2c004c3b549df22eeaf5d
```

start up the listener and receive the message:

```
$ node mq.js -d /tmp/abc listen
MESSAGE: hi
```


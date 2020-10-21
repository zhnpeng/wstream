## notices
 
- support mac os only

## download & version

https://github.com/etcd-io/etcd/releases
```
etcd-v3.3.25-darwin-amd64.zip
```

## quick start etcd

start a single-member cluster of etcd:

 ```./etcd```
 
This will bring up etcd listening on port 2379 for client communication and on port 2380 for server-to-server communication.
                                                    
Next, let's set a single key, and then retrieve it:

```
etcdctl put mykey "this is awesome"
etcdctl get mykey
```

## start webui e3w

```cd webui```

```./e3w```
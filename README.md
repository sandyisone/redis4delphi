# redis4delphi

Delphi Redis Client is compatible with Delphi2010.


Redis

```Delphi
  FRedisHandle := TRedisHandle.Create();
  FRedisHandle.Ip := '127.0.0.1';
  FRedisHandle.Port := 6379;
  FRedisHandle.Password := '123456';
  FRedisHandle.Db := 1;

  FRedisHandle.StringSet('akey', 'aValue');
  FRedisHandle.StringGet('akey');
```


Redis Cluster

```Delphi
  FRedisClusterHandle := TRedisClusterHandle.Create;

  FRedisClusterHandle.Password := '123456';
  FRedisClusterHandle.AddNode('192.168.1.80', 6379);
  FRedisClusterHandle.AddNode('192.168.1.81', 6379);

  FRedisClusterHandle.StringSet('akey', 'aValue');
  FRedisClusterHandle.StringGet('akey');
```


feature is unit tested.

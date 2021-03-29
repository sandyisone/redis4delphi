# redis4delphi

Delphi Redis Client is compatible with Delphi2010.

```Delphi

  FRedisHandle := TRedisHandle.Create();
  FRedisHandle.Ip := '127.0.0.1';
  FRedisHandle.Port := 6379;
  FRedisHandle.Password := '123456';
  FRedisHandle.Db := 1;

  FRedisHandle.Connection := True;
  FRedisHandle.RedisAuth;
  FRedisHandle.RedisSelect;

```

feature is unit tested.

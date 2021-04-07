{
  redis for delphi
}

unit uRedisHandle;

interface

uses
  Classes, IdTCPClient, SysUtils, StrUtils, IdException;


type

  RedisException = class(Exception);

  TRedisHandle = class
  private
    FTcpClient: TIdTCPClient;
    FPort: Integer;
    FPassword: string;
    FIp: string;
    FDb: Integer;
    FReadTimeout: Integer;
    procedure SetIp(const Value: string);
    procedure SetPassword(const Value: string);
    procedure SetPort(const Value: Integer);
    procedure SetDb(const Value: Integer);
    procedure SetReadTimeout(const Value: Integer);

  protected
    FCmdList: TStringList;
    FResponseList: TStringList;

    function GetConnection: Boolean;
    procedure SetConnection(const Value: Boolean);
    //异常
    procedure RaiseErr(aErr: string);

    //组装并发送命令
    procedure SendCmds(aCmdList: TStringList);
    //读取应答并解析
    procedure ReadAndParseResponse(var aResponseList: TStringList);
    //获取String应答，无返回空
    function ReadStringResponse(): string;
    //获取Integer应答，无返回0
    function ReadIntegerResponse(): Integer;

    procedure NewTcpClient;
  public
    constructor Create(aReadTimeOut: Integer = 5000);
    destructor Destroy; override;

    //连接
    property Connection: Boolean read GetConnection write SetConnection;
    //应答
    property ResponseList: TStringList read FResponseList;


    ////////////////////////////////////////////////////////////////////////////
    ///                     命令
    //使用Password认证
    procedure RedisAuth();
    //选择Db数据库，默认使用0号数据库
    procedure RedisSelect();


    ////////////////////////////////////////////////////////////////////////////
    ///               String
    //Redis DEL 命令用于删除已存在的键。不存在的 key 会被忽略。
    procedure KeyDelete(aKey: String);
    //Redis EXISTS 命令用于检查给定 key 是否存在。
    function KeyExist(aKey: String): Boolean;
    //Redis Expire 命令用于设置 key 的过期时间，key 过期后将不再可用。单位以秒计。
    procedure KeySetExpire(aKey: String; aExpireSec: Integer);
    ////////////////////////////////////////////////////////////////////////////


    ////////////////////////////////////////////////////////////////////////////
    ///               String
    //Redis Get 命令用于获取指定 key 的值。如果 key 不存在，返回 nil 。如果key 储存的值不是字符串类型，返回一个错误。
    function StringGet(aKey: string): string;
    //Redis SET 命令用于设置给定 key 的值。如果 key 已经存储其他值， SET 就覆写旧值，且无视类型。
    procedure StringSet(aKey, aValue: String); overload;
    //Redis Getset 命令用于设置指定 key 的值，并返回 key 的旧值。
    function StringGetSet(aKey, aValue: String): String;

    //set 带超时（秒）
    procedure StringSet(aKey, aValue: String; aExpireSec: Int64); overload;
    ////////////////////////////////////////////////////////////////////////////



    ////////////////////////////////////////////////////////////////////////////
    ///               List
    //list队尾插入值，返回新的list长度
    //Redis Rpush 命令用于将一个或多个值插入到列表的尾部(最右边)。
    //如果列表不存在,一个空列表会被创建并执行RPUSH 操作。当列表存在但不是列表类型时返回一个错误
    //Insert all the specified values at the tail of the list stored at key.
    //If key does not exist, it is created as empty list before performing the push operation.
    //When key holds a value that is not a list, an error is returned.
    function ListRPush(aKey, aValue: string): Integer; overload;
    function ListRPush(aKey: string; aValues: array of string): Integer; overload;

    //Redis Lpush 命令将一个或多个值插入到列表头部。 如果 key 不存在，
    //一个空列表会被创建并执行 LPUSH 操作。 当 key 存在但不是列表类型时，返回一个错误。
    function ListLPush(aKey, aValue: string): Integer; overload;
    function ListLPush(aKey: string; aValues: array of string): Integer; overload;


    //Redis Rpop 命令用于移除列表的最后一个元素，返回值为移除的元素，无数据返回空。
    function ListRPop(aKey: string): string;
    //Redis Lpop 命令用于移除并返回列表的第一个元素，无数据返回空。
    function ListLPop(aKey: string): string;

    //获取list大小
    function ListLen(aKey: string): Integer;
    //获取list范围数据,获取后数据并不会被删除
    function ListRange(aKey: string; aBegin, aEnd: Integer; var aRetValues: TStringList): Integer;

    //Redis Lrem 根据参数 COUNT 的值，移除列表中与参数 VALUE 相等的元素,返回移除数据个数
    //COUNT 的值可以是以下几种：
    //count > 0 : 从表头开始向表尾搜索，移除与 VALUE 相等的元素，数量为 COUNT 。
    //count < 0 : 从表尾开始向表头搜索，移除与 VALUE 相等的元素，数量为 COUNT 的绝对值。
    //count = 0 : 移除表中所有与 VALUE 相等的值。
    //Removes the first count occurrences of elements equal to element from the list stored at key
    //The count argument influences the operation in the following ways:
    //count > 0: Remove elements equal to element moving from head to tail.
    //count < 0: Remove elements equal to element moving from tail to head.
    //count = 0: Remove all elements equal to element.
    function ListRemove(aKey, aValue: string; aCount: Integer): Integer;
    ////////////////////////////////////////////////////////////////////////////



    ////////////////////////////////////////////////////////////////////////////
    //redis 服务ip
    property Ip: string read FIp write SetIp;
    //redis 端口
    property Port: Integer read FPort write SetPort;
    //redis 密码
    property Password: string read FPassword write SetPassword;
    //redis 数据库
    property Db: Integer read FDb write SetDb;
    //
    property ReadTimeout: Integer read FReadTimeout write SetReadTimeout;
  end;


const
  //回车换行
  C_CRLF = #$0D#$0A;



implementation

{ TRedisHandle }

constructor TRedisHandle.Create(aReadTimeOut: Integer);
begin
  FIp := '127.0.0.1';
  FPort := 6379;
  FPassword := '';
  FDb := 0;
  FReadTimeout := aReadTimeOut;

  NewTcpClient;

  FCmdList := TStringList.Create;
  FResponseList := TStringList.Create;

end;

destructor TRedisHandle.Destroy;
begin
  FTcpClient.Free;

  FCmdList.Free;
  FResponseList.Free;

  inherited;
end;

procedure TRedisHandle.RaiseErr(aErr: string);
begin
  raise RedisException.Create(aErr);
end;

procedure TRedisHandle.ReadAndParseResponse(var aResponseList: TStringList);
var
  aRetType: string;
  aLen: Integer;
  aBuff: TBytes;
  i, aBegin: Integer;
begin

//  aRetType := FTcpClient.IOHandler.ReadByte;
//
//  if aRetType = $2B then
//  begin
//    //状态回复（status reply）的第一个字节是 "+"
//  end
//  else if aRetType = $3A then
//  begin
//    //整数回复（integer reply）的第一个字节是 ":"
//  end
//  else if aRetType = $24 then
//  begin
//    //批量回复（bulk reply）的第一个字节是 "$"
//  end
//  else if aRetType = $2A then
//  begin
//    //多条批量回复（multi bulk reply）的第一个字节是 "*"
//  end;


  FTcpClient.IOHandler.ReadBytes(aBuff, -1, False);

  aLen := Length(aBuff);
  if aLen = 0 then RaiseErr('无应答');


  if aLen <= 2 then
  begin
    Sleep(100);
    FTcpClient.IOHandler.ReadBytes(aBuff, -1, True);

    aLen := Length(aBuff);
  end;

  if aLen <= 2 then
    RaiseErr('未知应答:' + TEncoding.UTF8.GetString(aBuff));


  //数据类型
  aRetType := TEncoding.UTF8.GetString(aBuff, 0, 1);

  if aRetType = '-' then
  begin
    //错误回复（error reply）的第一个字节是 "-"
    RaiseErr(TEncoding.UTF8.GetString(aBuff, 1, aLen - 2));
  end;

  aResponseList.Clear;
  aResponseList.Add(aRetType);
  //应答数据
  aBegin := 1;
  for i := 2 to aLen - 2 do
  begin
    if (aBuff[i] = $0D) and (aBuff[i + 1] = $0A) then
    begin
      aResponseList.Add(TEncoding.UTF8.GetString(aBuff, aBegin, i - aBegin));
      aBegin := i + 2;
    end;
  end;

  if aResponseList.Count < 2 then RaiseErr('应答数据缺失');


end;



function TRedisHandle.ReadIntegerResponse: Integer;
begin
  //读取应答并解析
  ReadAndParseResponse(FResponseList);

  Result := StrToInt(FResponseList.Strings[1]);
end;

function TRedisHandle.ReadStringResponse: string;
begin
  //读取应答并解析
  ReadAndParseResponse(FResponseList);

  if StrToInt(FResponseList.Strings[1]) <= 0 then Exit('');

  Result := FResponseList.Strings[2];
end;

procedure TRedisHandle.RedisAuth;
begin
  Connection := True;

  //AUTH <password>
  FCmdList.Clear;
  FCmdList.Add('AUTH');
  FCmdList.Add(FPassword);

  //发送名称
  SendCmds(FCmdList);

  //读取应答并解析
  ReadAndParseResponse(FResponseList);

end;


function TRedisHandle.StringGet(aKey: string): string;
begin
  Connection := True;

  FCmdList.Clear;
  FCmdList.Add('GET');
  FCmdList.Add(aKey);

  //发送名称
  SendCmds(FCmdList);

  //读取应答
  Result := ReadStringResponse();

end;


function TRedisHandle.StringGetSet(aKey, aValue: String): String;
begin
  Connection := True;

  FCmdList.Clear;
  FCmdList.Add('GETSET');
  FCmdList.Add(aKey);
  FCmdList.Add(aValue);

  //发送名称
  SendCmds(FCmdList);

  //读取应答
  Result := ReadStringResponse();

end;



{
  Set key to hold the string value. If key already holds a value, it is overwritten,
  regardless of its type. Any previous time to live associated with the key
  is discarded on successful SET operation.

  Options
  The SET command supports a set of options that modify its behavior:

  EX seconds -- Set the specified expire time, in seconds.
  PX milliseconds -- Set the specified expire time, in milliseconds.
  EXAT timestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds.
  PXAT timestamp-milliseconds -- Set the specified Unix time at which the key will expire, in milliseconds.
  NX -- Only set the key if it does not already exist.
  XX -- Only set the key if it already exist.
  KEEPTTL -- Retain the time to live associated with the key.
  GET -- Return the old value stored at key, or nil when key did not exist.
  Note: Since the SET command options can replace SETNX, SETEX, PSETEX, GETSET,
  it is possible that in future versions of Redis these commands will be deprecated and finally removed.

  Return value
  Simple string reply: OK if SET was executed correctly.
  Bulk string reply: when GET option is set, the old value stored at key,
  or nil when key did not exist.
  Null reply: a Null Bulk Reply is returned if the SET operation was not
  performed because the user specified the NX or XX option but the condition
  was not met, or if the user specified the GET option and there was no previous
  value for the key.
}
procedure TRedisHandle.StringSet(aKey, aValue: String);
begin
  StringSet(aKey, aValue, -1);
end;

procedure TRedisHandle.StringSet(aKey, aValue: String; aExpireSec: Int64);
begin
  Connection := True;

  FCmdList.Clear;
  FCmdList.Add('SET');
  FCmdList.Add(aKey);
  FCmdList.Add(aValue);
  if aExpireSec > 0 then
  begin
    FCmdList.Add('EX');
    FCmdList.Add(IntToStr(aExpireSec));
  end;

  //发送名称
  SendCmds(FCmdList);

  //读取应答并解析
  ReadAndParseResponse(FResponseList);

end;


procedure TRedisHandle.RedisSelect();
begin
  Connection := True;

  //SELECT index
  FCmdList.Clear;
  FCmdList.Add('SELECT');
  FCmdList.Add(IntToStr(FDb));

  //发送名称
  SendCmds(FCmdList);

  //读取应答并解析
  ReadAndParseResponse(FResponseList);

end;


function TRedisHandle.GetConnection: Boolean;
begin
  Result := Assigned(FTcpClient) and FTcpClient.Connected;
end;


procedure TRedisHandle.KeyDelete(aKey: String);
begin
  Connection := True;

  FCmdList.Clear;
  FCmdList.Add('DEL');
  FCmdList.Add(aKey);

  //发送名称
  SendCmds(FCmdList);

  //读取应答并解析
  ReadAndParseResponse(FResponseList);
end;

function TRedisHandle.KeyExist(aKey: String): Boolean;
begin
  Connection := True;

  FCmdList.Clear;
  FCmdList.Add('EXISTS');
  FCmdList.Add(aKey);

  //发送名称
  SendCmds(FCmdList);

  //读取应答并解析
  Result := ReadIntegerResponse() <> 0;

end;

procedure TRedisHandle.KeySetExpire(aKey: String; aExpireSec: Integer);
begin
  Connection := True;

  FCmdList.Clear;
  FCmdList.Add('EXPIRE');
  FCmdList.Add(aKey);
  FCmdList.Add(IntToStr(aExpireSec));

  //发送名称
  SendCmds(FCmdList);

  //读取应答并解析
  ReadAndParseResponse(FResponseList);



end;

function TRedisHandle.ListRange(aKey: string; aBegin, aEnd: Integer;
  var aRetValues: TStringList): Integer;
var
  i: Integer;
begin
  Connection := True;

  FCmdList.Clear;
  FCmdList.Add('LRANGE');
  FCmdList.Add(aKey);
  FCmdList.Add(IntToStr(aBegin));
  FCmdList.Add(IntToStr(aEnd));

  //发送名称
  SendCmds(FCmdList);

  //读取应答并解析
  ReadAndParseResponse(FResponseList);

  Result := StrToInt(FResponseList.Strings[1]);

  aRetValues.Clear;

  if Result <= 0 then Exit;

  for i := 0 to Result - 1 do
  begin
    aRetValues.Add(FResponseList.Strings[3 + i * 2]);
  end;

end;

function TRedisHandle.ListRemove(aKey, aValue: string; aCount: Integer): Integer;
begin
  Connection := True;

  FCmdList.Clear;
  FCmdList.Add('LREM');
  FCmdList.Add(aKey);
  FCmdList.Add(IntToStr(aCount));
  FCmdList.Add(aValue);

  //发送名称
  SendCmds(FCmdList);

  //读取应答并解析
  ReadAndParseResponse(FResponseList);

  Result := StrToInt(FResponseList.Strings[1]);
end;



function TRedisHandle.ListLen(aKey: string): Integer;
begin
  Connection := True;

  FCmdList.Clear;
  FCmdList.Add('LLEN');
  FCmdList.Add(aKey);

  //发送名称
  SendCmds(FCmdList);

  //读取应答并解析
  Result := ReadIntegerResponse();

end;


function TRedisHandle.ListLPop(aKey: string): string;
begin
  Connection := True;

  FCmdList.Clear;
  FCmdList.Add('LPOP');
  FCmdList.Add(aKey);

  //发送名称
  SendCmds(FCmdList);

  //读取应答
  Result := ReadStringResponse();

end;

function TRedisHandle.ListLPush(aKey, aValue: string): Integer;
begin
  Result := ListLPush(aKey, [aValue]);
end;

function TRedisHandle.ListLPush(aKey: string;
  aValues: array of string): Integer;
var
  i: Integer;
begin
  if Length(aValues) <= 0 then RaiseErr('无数据');

  Connection := True;

  FCmdList.Clear;
  FCmdList.Add('LPUSH');
  FCmdList.Add(aKey);
  for i := 0 to Length(aValues) - 1 do
    FCmdList.Add(aValues[i]);

  //发送名称
  SendCmds(FCmdList);

  //读取应答并解析
  Result := ReadIntegerResponse();

end;

function TRedisHandle.ListRPop(aKey: string): string;
begin
  Connection := True;

  FCmdList.Clear;
  FCmdList.Add('RPOP');
  FCmdList.Add(aKey);

  //发送名称
  SendCmds(FCmdList);

  //读取应答
  Result := ReadStringResponse();
end;




function TRedisHandle.ListRPush(aKey, aValue: string): Integer;
begin
  Result := ListRPush(aKey, [aValue]);
end;


function TRedisHandle.ListRPush(aKey: string;
  aValues: array of string): Integer;
var
  i: Integer;
begin
  if Length(aValues) <= 0 then RaiseErr('无数据');

  Connection := True;

  FCmdList.Clear;
  FCmdList.Add('RPUSH');
  FCmdList.Add(aKey);
  for i := 0 to Length(aValues) - 1 do
    FCmdList.Add(aValues[i]);

  //发送名称
  SendCmds(FCmdList);

  //读取应答并解析
  Result := ReadIntegerResponse();

end;




procedure TRedisHandle.NewTcpClient;
begin
  if Assigned(FTcpClient) then
  begin
    try
      FreeAndNil(FTcpClient);
    except
      on E: Exception do
      begin
      end;
    end;
  end;

  FTcpClient := TIdTCPClient.Create(nil);
  FTcpClient.ReadTimeout := FReadTimeout;
end;

{
发送命令：
  *<参数数量> CR LF
  $<参数 1 的字节数量> CR LF
  <参数 1 的数据> CR LF
  ...
  $<参数 N 的字节数量> CR LF
  <参数 N 的数据> CR LF
}
procedure TRedisHandle.SendCmds(aCmdList: TStringList);
var
  aCmd: string;
  aBuff: TBytes;
  i: Integer;
begin

  //参数个数
  aCmd := '*' + IntToStr(aCmdList.Count) + C_CRLF;
  //参数
  for i := 0 to aCmdList.Count - 1 do
  begin
    aCmd := aCmd + '$' + IntToStr(TEncoding.UTF8.GetByteCount(aCmdList.Strings[i])) + C_CRLF
      + aCmdList.Strings[i] + C_CRLF;
  end;
  aBuff := TEncoding.UTF8.GetBytes(aCmd);

  FTcpClient.IOHandler.Write(aBuff);

end;

procedure TRedisHandle.SetConnection(const Value: Boolean);
begin
  if Value = GetConnection then Exit;

  try
    if Value then
    begin
      if FIp = '' then FIp := '127.0.0.1';
      if FPort = 0 then FPort := 6379;

      FTcpClient.Host := FIp;
      FTcpClient.Port := FPort;
      FTcpClient.Connect;

      if Password <> '' then RedisAuth;
      if Db <> 0 then RedisSelect;

    end
    else
    begin
      FTcpClient.Disconnect;
    end;
  except
    on E: EIdException do
    begin
      NewTcpClient;
      raise e;
    end;
  end;



end;

procedure TRedisHandle.SetDb(const Value: Integer);
begin
  FDb := Value;
end;

procedure TRedisHandle.SetIp(const Value: string);
begin
  FIp := Value;
end;

procedure TRedisHandle.SetPassword(const Value: string);
begin
  FPassword := Value;
end;

procedure TRedisHandle.SetPort(const Value: Integer);
begin
  FPort := Value;
end;

procedure TRedisHandle.SetReadTimeout(const Value: Integer);
begin
  FReadTimeout := Value;
end;

end.

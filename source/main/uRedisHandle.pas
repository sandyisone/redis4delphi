{
  redis for delphi
}

unit uRedisHandle;

interface

uses
  Classes, IdTCPClient, SysUtils, StrUtils, IdException, uRedisCommand,
  uRedisCommon;


type

  //redis handle exception
  ERedisException = class(Exception);
  //redis应答错误信息
  ERedisErrorReply = class(Exception);

  //当redis应答错误时，回调此函数，如isHandled返回true，则提示成功，否则弹异常
  TOnGetRedisError = procedure(aRedisCommand: TRedisCommand;
      var aResponseList: TStringList; aErr: string; var isHandled: Boolean) of object;

  //
  TRedisHandle = class
  private
    FPassword: string;
    FDb: Integer;
    FPort: Integer;
    FIp: string;
    FReadTimeout: Integer;
    FOnGetRedisError: TOnGetRedisError;
    procedure SetPassword(const Value: string);
    procedure SetDb(const Value: Integer);
    procedure SetIp(const Value: string);
    procedure SetPort(const Value: Integer);
    procedure SetReadTimeout(const Value: Integer);
    procedure SetOnGetRedisError(const Value: TOnGetRedisError);
  protected
    //命令行
    FRedisCommand: TRedisCommand;
    FResponseList: TStringList;
    //tcp
    FTcpClient: TIdTCPClient;

    function GetConnection: Boolean;
    procedure SetConnection(const Value: Boolean);
    //tcp
    procedure NewTcpClient;

    //异常
    procedure RaiseErr(aErr: string);

    //组装并发送命令 无数据返回
    procedure SendCommandWithNoResponse(aRedisCommand: TRedisCommand);
    //获取String应答，无返回空
    function SendCommandWithStrResponse(aRedisCommand: TRedisCommand): string;
    //获取Integer应答，无返回0
    function SendCommandWithIntResponse(aRedisCommand: TRedisCommand): Integer;



    //组装并发送命令
    procedure SendCommand(aRedisCommand: TRedisCommand);
    //读取应答并解析
    procedure ReadAndParseResponse(var aResponseList: TStringList);

  public
    constructor Create(); virtual;
    destructor Destroy; override;

    //连接
    property Connection: Boolean read GetConnection write SetConnection;
    //应答
    property ResponseList: TStringList read FResponseList;

    //发送命令解析应答
    procedure SendCommandAndGetResponse(aRedisCommand: TRedisCommand;
      var aResponseList: TStringList);

    ////////////////////////////////////////////////////////////////////////////
    ///                     命令
    //使用Password认证
    procedure RedisAuth();
    //选择Db数据库，默认使用0号数据库
    procedure RedisSelect();

    ////////////////////////////////////////////////////////////////////////////
    ///                     Key
    //Redis DEL 命令用于删除已存在的键。不存在的 key 会被忽略。
    procedure KeyDelete(aKey: String);
    //Redis EXISTS 命令用于检查给定 key 是否存在。
    function KeyExist(aKey: String): Boolean;
    //Redis Expire 命令用于设置 key 的过期时间，key 过期后将不再可用。单位以秒计。
    procedure KeySetExpire(aKey: String; aExpireSec: Integer);
    ////////////////////////////////////////////////////////////////////////////


    ////////////////////////////////////////////////////////////////////////////
    ///                     String
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
    ///                     List
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
    //redis ip
    property Ip: string read FIp write SetIp;
    //redis port
    property Port: Integer read FPort write SetPort;
    //redis 读超时
    property ReadTimeout: Integer read FReadTimeout write SetReadTimeout;

    //redis 密码
    property Password: string read FPassword write SetPassword;
    //redis 数据库
    property Db: Integer read FDb write SetDb;
    //当redis应答错误时，回调此函数，如isHandled返回true，则提示成功，否则弹异常
    property OnGetRedisError: TOnGetRedisError read FOnGetRedisError write SetOnGetRedisError;
  end;


implementation

{ TRedisHandle }

constructor TRedisHandle.Create();
begin
  FIp := Redis_Default_Ip;
  FPort := Redis_Default_Port;
  FPassword := '';
  FDb := 0;

  FRedisCommand := TRedisCommand.Create;
  FResponseList := TStringList.Create;

  NewTcpClient;
end;

destructor TRedisHandle.Destroy;
begin
  FTcpClient.Free;
  FRedisCommand.Free;
  FResponseList.Free;

  inherited;
end;


procedure TRedisHandle.RaiseErr(aErr: string);
begin
  raise ERedisException.Create(aErr);
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
    raise ERedisErrorReply.Create(TEncoding.UTF8.GetString(aBuff, 1, aLen - 2));
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

procedure TRedisHandle.RedisAuth;
var
  aCommand: TRedisCommand;
begin

  aCommand := TRedisCommand.Create;
  try
    //AUTH <password>
    aCommand.Clear.Add('AUTH').Add(FPassword);
    //发送,读取应答并解析
    SendCommandWithNoResponse(aCommand);
  finally
    aCommand.Free;
  end;

end;


function TRedisHandle.StringGet(aKey: string): string;
begin
  FRedisCommand.Clear.Add('GET').Add(aKey);
  //发送,读取应答并解析
  Result := SendCommandWithStrResponse(FRedisCommand);
end;


function TRedisHandle.StringGetSet(aKey, aValue: String): String;
begin
  FRedisCommand.Clear.Add('GETSET').Add(aKey).Add(aValue);
  //发送,读取应答并解析
  Result := SendCommandWithStrResponse(FRedisCommand);
end;


procedure TRedisHandle.StringSet(aKey, aValue: String);
begin
  StringSet(aKey, aValue, -1);
end;

procedure TRedisHandle.StringSet(aKey, aValue: String; aExpireSec: Int64);
begin
  FRedisCommand.Clear.Add('SET').Add(aKey).Add(aValue);
  if aExpireSec > 0 then
  begin
    FRedisCommand.Add('EX').Add(IntToStr(aExpireSec));
  end;

  //发送,读取应答并解析
  SendCommandWithNoResponse(FRedisCommand);

end;


procedure TRedisHandle.RedisSelect();
var
  aCommand: TRedisCommand;
begin
  aCommand := TRedisCommand.Create;
  try
    //SELECT index
    aCommand.Clear.Add('SELECT').Add(IntToStr(FDb));
    //发送,读取应答并解析
    SendCommandWithNoResponse(aCommand);
  finally
    aCommand.Free;
  end;
end;


function TRedisHandle.GetConnection: Boolean;
begin
  Result := Assigned(FTcpClient) and FTcpClient.Connected;
end;


procedure TRedisHandle.KeyDelete(aKey: String);
begin
  FRedisCommand.Clear.Add('DEL').Add(aKey);
  //发送,读取应答并解析
  SendCommandWithNoResponse(FRedisCommand);
end;

function TRedisHandle.KeyExist(aKey: String): Boolean;
begin
  FRedisCommand.Clear.Add('EXISTS').Add(aKey);
  //发送,读取应答并解析
  Result := SendCommandWithIntResponse(FRedisCommand) <> 0;
end;

procedure TRedisHandle.KeySetExpire(aKey: String; aExpireSec: Integer);
begin
  FRedisCommand.Clear.Add('EXPIRE').Add(aKey).Add(IntToStr(aExpireSec));
  //发送,读取应答并解析
  SendCommandWithNoResponse(FRedisCommand);
end;

function TRedisHandle.ListRange(aKey: string; aBegin, aEnd: Integer;
  var aRetValues: TStringList): Integer;
var
  i: Integer;
begin
  FRedisCommand.Clear.Add('LRANGE').Add(aKey)
    .Add(IntToStr(aBegin)).Add(IntToStr(aEnd));

  //发送
  SendCommandAndGetResponse(FRedisCommand, FResponseList);

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
  FRedisCommand.Clear.Add('LREM').Add(aKey).Add(IntToStr(aCount)).Add(aValue);
  //发送
  Result := SendCommandWithIntResponse(FRedisCommand);
end;


function TRedisHandle.ListLen(aKey: string): Integer;
begin
  FRedisCommand.Clear.Add('LLEN').Add(aKey);
  //发送
  Result := SendCommandWithIntResponse(FRedisCommand);
end;


function TRedisHandle.ListLPop(aKey: string): string;
begin
  FRedisCommand.Clear.Add('LPOP').Add(aKey);
  //发送
  Result := SendCommandWithStrResponse(FRedisCommand);

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

  FRedisCommand.Clear.Add('LPUSH').Add(aKey);
  for i := 0 to Length(aValues) - 1 do
    FRedisCommand.Add(aValues[i]);

  //发送
  Result := SendCommandWithIntResponse(FRedisCommand);

end;

function TRedisHandle.ListRPop(aKey: string): string;
begin
  FRedisCommand.Clear.Add('RPOP').Add(aKey);
  //发送
  Result := SendCommandWithStrResponse(FRedisCommand);
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

  FRedisCommand.Clear.Add('RPUSH').Add(aKey);
  for i := 0 to Length(aValues) - 1 do
    FRedisCommand.Add(aValues[i]);

  //发送
  Result := SendCommandWithIntResponse(FRedisCommand);

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
end;

function TRedisHandle.SendCommandWithIntResponse(
  aRedisCommand: TRedisCommand): Integer;
begin
  SendCommandAndGetResponse(aRedisCommand, FResponseList);

  Result := StrToInt(FResponseList.Strings[1]);
end;

procedure TRedisHandle.SendCommandWithNoResponse(aRedisCommand: TRedisCommand);
begin
  SendCommandAndGetResponse(aRedisCommand, FResponseList);
end;

function TRedisHandle.SendCommandWithStrResponse(
  aRedisCommand: TRedisCommand): string;
begin
  SendCommandAndGetResponse(aRedisCommand, FResponseList);

  if StrToInt(FResponseList.Strings[1]) <= 0 then Exit('');

  Result := FResponseList.Strings[2];
end;



procedure TRedisHandle.SendCommand(aRedisCommand: TRedisCommand);
var
  aBuff: TBytes;
begin
  aBuff := aRedisCommand.ToRedisCommand;

  try
    FTcpClient.IOHandler.Write(aBuff);
  except
    on E: EIdException do
    begin
      NewTcpClient;
      raise e;
    end;
  end;

end;

procedure TRedisHandle.SendCommandAndGetResponse(aRedisCommand: TRedisCommand;
  var aResponseList: TStringList);
var
  isHandled: Boolean;
begin
  Connection := True;

  SendCommand(aRedisCommand);
  try
    ReadAndParseResponse(aResponseList);
  except
    on E: ERedisErrorReply do
    begin
      if Assigned(FOnGetRedisError) then
      begin
        FOnGetRedisError(aRedisCommand, aResponseList, e.Message, isHandled);
        if isHandled then Exit;
      end;

      raise e;

    end;
  end;

end;




procedure TRedisHandle.SetConnection(const Value: Boolean);
begin
  if Value = GetConnection then Exit;

  try
    if Value then
    begin
      if FIp = '' then FIp := Redis_Default_Ip;
      if FPort <= 0 then FPort := Redis_Default_Port;
      if FReadTimeout <= 0 then FReadTimeout := Redis_default_ReadTimeout;

      FTcpClient.Host := FIp;
      FTcpClient.Port := FPort;
      FTcpClient.ReadTimeout := FReadTimeout;
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

procedure TRedisHandle.SetOnGetRedisError(const Value: TOnGetRedisError);
begin
  FOnGetRedisError := Value;
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

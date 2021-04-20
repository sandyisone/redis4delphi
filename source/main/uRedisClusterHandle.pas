{
  support redis cluter
}
unit uRedisClusterHandle;

interface

uses
  Classes, uRedisHandle, uRedisCommand, SysUtils, StrUtils, uRedisCommon,
  Contnrs;

type
  //reids cluster exception
  ERedisClusterException = class(Exception);

  TRedisClusterHandle = class
  private
    FRedisList: TObjectList;
    FReadTimeout: Integer;
    FPassword: string;
    procedure SetPassword(const Value: string);
    procedure SetReadTimeout(const Value: Integer);
  protected
    //异常
    procedure RaiseErr(aErr: string);

    //解析-MOVED 3999 127.0.0.1:6381中的ip和端口
    function ParseSlotIpPort(aStr: string; var aSlot: Integer;
      var aIp: string; var aPort: Integer): Boolean;

    procedure doMovedCommand(aRedisCommand: TRedisCommand;
      var aResponseList: TStringList; aMovedStr: string);

    //获取节点
    function GetNode(aIp: string; aPort: Integer): TRedisHandle;
    //新创建节点
    function NewNode(aIp: string; aPort: Integer): TRedisHandle;
    //获取一个节点，无异常
    function GetAndChekNode: TRedisHandle;

  protected
    //发送命令解析应答
    procedure SendCommandAndGetResponse(aRedisCommand: TRedisCommand;
      var aResponseList: TStringList);

  //当redis应答错误时，回调此函数，如isHandled返回true，则提示成功，否则弹异常
    procedure DoOnGetRedisError(aRedisCommand: TRedisCommand;
      var aResponseList: TStringList; aErr: string; var isHandled: Boolean);

  public
    constructor Create();
    destructor Destroy; override;

    //添加节点，处理重复ip和端口
    function AddNode(aIp: string; aPort: Integer): TRedisHandle;
    function GetNodeCount: Integer;


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













    //redis 读超时
    property ReadTimeout: Integer read FReadTimeout write SetReadTimeout;
    //redis 密码
    property Password: string read FPassword write SetPassword;

  end;



implementation

{ TRedisClusterHandle }

function TRedisClusterHandle.AddNode(aIp: string; aPort: Integer): TRedisHandle;
begin
  Result := GetNode(aIp, aPort);
  if Assigned(Result) then Exit;

  Result := NewNode(aIp, aPort);
  FRedisList.Add(Result);

end;

constructor TRedisClusterHandle.Create;
begin
  inherited;
  FRedisList := TObjectList.Create;
  FRedisList.OwnsObjects := True;

  FReadTimeout := Redis_default_ReadTimeout;
  FPassword := Redis_Default_Password;

end;

destructor TRedisClusterHandle.Destroy;
begin
  FRedisList.Free;
  inherited;
end;

procedure TRedisClusterHandle.doMovedCommand(aRedisCommand: TRedisCommand;
  var aResponseList: TStringList; aMovedStr: string);
var
 aSlot: Integer;
 aIp: string;
 aPort: Integer;
 aRedis: TRedisHandle;
begin
  if not ParseSlotIpPort(aMovedStr, aSlot, aIp, aPort) then
    RaiseErr('未知的MOVED应答：' + aMovedStr);

  aRedis := AddNode(aIp, aPort);

  aRedis.SendCommandAndGetResponse(aRedisCommand, aResponseList);

end;

procedure TRedisClusterHandle.DoOnGetRedisError(aRedisCommand: TRedisCommand;
  var aResponseList: TStringList; aErr: string; var isHandled: Boolean);
begin
  isHandled := False;

  aErr := Trim(aErr);
  //-MOVED 3999 127.0.0.1:6381
  if StartsText('MOVED', aErr) then
  begin
    //处理moved
    doMovedCommand(aRedisCommand, aResponseList, aErr);
    isHandled := True;
  end;

end;



procedure TRedisClusterHandle.SendCommandAndGetResponse(
  aRedisCommand: TRedisCommand; var aResponseList: TStringList);
var
  aStr: string;
begin
  try
    inherited;
  except
    on e: ERedisErrorReply do
    begin
      aStr := Trim(e.Message);
      //-MOVED 3999 127.0.0.1:6381
      if StartsText('MOVED', aStr) then
      begin
        //处理moved
        doMovedCommand(aRedisCommand, aResponseList, aStr);
      end;

      raise e;

    end;
  end;

end;

procedure TRedisClusterHandle.SetPassword(const Value: string);
begin
  FPassword := Value;
end;

procedure TRedisClusterHandle.SetReadTimeout(const Value: Integer);
begin
  FReadTimeout := Value;
end;

function TRedisClusterHandle.StringGet(aKey: string): string;
var
  aRedis: TRedisHandle;
begin
  aRedis := GetAndChekNode;
  Result := aRedis.StringGet(aKey);
end;

function TRedisClusterHandle.StringGetSet(aKey, aValue: String): String;
var
  aRedis: TRedisHandle;
begin
  aRedis := GetAndChekNode;
  Result := aRedis.StringGetSet(aKey, aValue);
end;

procedure TRedisClusterHandle.StringSet(aKey, aValue: String;
  aExpireSec: Int64);
var
  aRedis: TRedisHandle;
begin
  aRedis := GetAndChekNode;
  aRedis.StringSet(aKey, aValue, aExpireSec);
end;

procedure TRedisClusterHandle.StringSet(aKey, aValue: String);
var
  aRedis: TRedisHandle;
begin
  aRedis := GetAndChekNode;
  aRedis.StringSet(aKey, aValue);
end;

function TRedisClusterHandle.GetAndChekNode: TRedisHandle;
var
  i: Integer;
begin
  if FRedisList.Count <= 0 then
    RaiseErr('无Redis节点');

  //找个连上的，
  for i := 0 to FRedisList.Count - 1 do
  begin
    if TRedisHandle(FRedisList.Items[0]).Connection then
    begin
      Exit(TRedisHandle(FRedisList.Items[0]));
    end;
  end;
  //否则第一个
  Result := TRedisHandle(FRedisList.Items[0]);

end;

function TRedisClusterHandle.GetNode(aIp: string;
  aPort: Integer): TRedisHandle;
var
  i: Integer;
  aRedis: TRedisHandle;
begin
  for i := 0 to FRedisList.Count - 1 do
  begin
    aRedis := TRedisHandle(FRedisList.Items[i]);
    if SameText(aRedis.Ip, aIp) and (aRedis.Port = aPort) then
    begin
      Exit(aRedis);
    end;
  end;

  Result := nil;

end;

function TRedisClusterHandle.GetNodeCount: Integer;
begin
  Result := FRedisList.Count;
end;

procedure TRedisClusterHandle.KeyDelete(aKey: String);
var
  aRedis: TRedisHandle;
begin
  aRedis := GetAndChekNode;
  aRedis.KeyDelete(aKey);
end;

function TRedisClusterHandle.KeyExist(aKey: String): Boolean;
var
  aRedis: TRedisHandle;
begin
  aRedis := GetAndChekNode;
  Result := aRedis.KeyExist(aKey);
end;

procedure TRedisClusterHandle.KeySetExpire(aKey: String; aExpireSec: Integer);
var
  aRedis: TRedisHandle;
begin
  aRedis := GetAndChekNode;
  aRedis.KeySetExpire(aKey, aExpireSec);
end;

function TRedisClusterHandle.NewNode(aIp: string; aPort: Integer): TRedisHandle;
begin
  Result := TRedisHandle.Create();

  Result.Password := FPassword;
  Result.Db := 0;

  Result.Ip := aIp;
  Result.Port := aPort;

  Result.OnGetRedisError := DoOnGetRedisError;
end;

function TRedisClusterHandle.ParseSlotIpPort(aStr: string; var aSlot: Integer;
  var aIp: string; var aPort: Integer): Boolean;
var
  i, aLen, aIndex, aIndex2, aIndex3: Integer;
  aSubStr: string;
begin
  //解析MOVED 3999 127.0.0.1:6381中的ip和端口
  aSubStr := '';

  aIndex := 0;
  aIndex2 := 0;
  aIndex3 := 0;
  aLen := Length(aStr);

  for i := 1 to aLen do
  begin
    if aStr[i] = ' ' then
    begin
      if aIndex = 0 then
        aIndex := i
      else
        aIndex2 := i;
    end;

    if aStr[i] = ':' then
      aIndex3 := i;

  end;

  if (aIndex = 0) or (aIndex2 = 0) or (aIndex3 = 0) then Exit(False);

  //slot
  aSubStr := Trim(MidStr(aStr, aIndex + 1, aIndex2 - aIndex - 1));
  if aSubStr = '' then Exit(False);
  aSlot := StrToInt(aSubStr);

  //ip
  aSubStr := Trim(MidStr(aStr, aIndex2 + 1, aIndex3 - aIndex2 - 1));
  if aSubStr = '' then Exit(False);
  aIp := aSubStr;

  //port
  aSubStr := Trim(MidStr(aStr, aIndex3 + 1, aLen - aIndex3));
  if aSubStr = '' then Exit(False);
  aPort := StrToInt(aSubStr);

  Result := True;

end;

procedure TRedisClusterHandle.RaiseErr(aErr: string);
begin
  raise ERedisClusterException.Create(aErr);
end;

end.

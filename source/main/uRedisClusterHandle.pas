{
  support redis cluter
}
unit uRedisClusterHandle;

interface

uses
  Classes, uRedisHandle, uRedisCommand, SysUtils, StrUtils, uRedisCommon,
  Contnrs, uRedisClusterCRC16, Generics.Collections;

type
  //reids cluster exception
  ERedisClusterException = class(Exception);

  TRedisClusterHandle = class
  private
    FSlotCache: TDictionary<Integer, TRedisHandle>;
    FRedisList: TObjectList;
    FReadTimeout: Integer;
    FPassword: string;
    procedure SetPassword(const Value: string);
    procedure SetReadTimeout(const Value: Integer);
  protected
    //�쳣
    procedure RaiseErr(aErr: string);

    //����-MOVED 3999 127.0.0.1:6381�е�ip�Ͷ˿�
    function ParseSlotIpPort(aStr: string; var aSlot: Integer;
      var aIp: string; var aPort: Integer): Boolean;

    procedure doMovedCommand(aRedisCommand: TRedisCommand;
      var aResponseList: TStringList; aMovedStr: string);

    //��ȡ�ڵ�
    function GetNode(aIp: string; aPort: Integer): TRedisHandle;
    //�´����ڵ�
    function NewNode(aIp: string; aPort: Integer): TRedisHandle;
    //��ȡһ���ڵ㣬���쳣
    function GetAndChekNode(aKey: string): TRedisHandle;

  protected

  //��redisӦ�����ʱ���ص��˺�������isHandled����true������ʾ�ɹ��������쳣
    procedure DoOnGetRedisError(aRedisCommand: TRedisCommand;
      var aResponseList: TStringList; aErr: string; var isHandled: Boolean);

  public
    constructor Create();
    destructor Destroy; override;

    //��ӽڵ㣬�����ظ�ip�Ͷ˿�
    function AddNode(aIp: string; aPort: Integer): TRedisHandle;
    function GetNodeCount: Integer;


    ////////////////////////////////////////////////////////////////////////////
    ///                     Key
    //Redis DEL ��������ɾ���Ѵ��ڵļ��������ڵ� key �ᱻ���ԡ�
    procedure KeyDelete(aKey: String);
    //Redis EXISTS �������ڼ����� key �Ƿ���ڡ�
    function KeyExist(aKey: String): Boolean;
    //Redis Expire ������������ key �Ĺ���ʱ�䣬key ���ں󽫲��ٿ��á���λ����ơ�
    procedure KeySetExpire(aKey: String; aExpireSec: Integer);
    ////////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////////
    ///                     String
    //Redis Get �������ڻ�ȡָ�� key ��ֵ����� key �����ڣ����� nil ��
    //���key �����ֵ�����ַ������ͣ�����һ������
    function StringGet(aKey: string): string;
    //Redis SET �����������ø��� key ��ֵ����� key �Ѿ��洢����ֵ��
    //SET �͸�д��ֵ�����������͡�
    procedure StringSet(aKey, aValue: String); overload;
    //Redis Getset ������������ָ�� key ��ֵ�������� key �ľ�ֵ��
    function StringGetSet(aKey, aValue: String): String;

    //set ����ʱ���룩
    procedure StringSet(aKey, aValue: String; aExpireSec: Int64); overload;
    ////////////////////////////////////////////////////////////////////////////
    //����
    property SlotCache: TDictionary<Integer, TRedisHandle> read FSlotCache;

    //redis ����ʱ
    property ReadTimeout: Integer read FReadTimeout write SetReadTimeout;
    //redis ����
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

  FSlotCache := TDictionary<Integer, TRedisHandle>.Create($10000);

end;

destructor TRedisClusterHandle.Destroy;
begin
  FRedisList.Free;
  FSlotCache.Free;
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
    RaiseErr('δ֪��MOVEDӦ��' + aMovedStr);

  //��ӻ򴴽�һ��
  aRedis := AddNode(aIp, aPort);

  //����
  FSlotCache.AddOrSetValue(aSlot, aRedis);

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
    //����moved
    doMovedCommand(aRedisCommand, aResponseList, aErr);
    isHandled := True;
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
  aRedis := GetAndChekNode(aKey);
  Result := aRedis.StringGet(aKey);
end;

function TRedisClusterHandle.StringGetSet(aKey, aValue: String): String;
var
  aRedis: TRedisHandle;
begin
  aRedis := GetAndChekNode(aKey);
  Result := aRedis.StringGetSet(aKey, aValue);
end;

procedure TRedisClusterHandle.StringSet(aKey, aValue: String;
  aExpireSec: Int64);
var
  aRedis: TRedisHandle;
begin
  aRedis := GetAndChekNode(aKey);
  aRedis.StringSet(aKey, aValue, aExpireSec);
end;

procedure TRedisClusterHandle.StringSet(aKey, aValue: String);
var
  aRedis: TRedisHandle;
begin
  aRedis := GetAndChekNode(aKey);
  aRedis.StringSet(aKey, aValue);
end;

function TRedisClusterHandle.GetAndChekNode(aKey: string): TRedisHandle;
var
  i: Integer;
  aSlot: Integer;
begin
  if FRedisList.Count <= 0 then
    RaiseErr('��Redis�ڵ�');

  //��cacheȡ
  aSlot := KeyToSlot(aKey);

  if FSlotCache.TryGetValue(aSlot, Result) then Exit;

  //�Ҹ����ϵģ�
  for i := 0 to FRedisList.Count - 1 do
  begin
    if TRedisHandle(FRedisList.Items[0]).Connection then
    begin
      Exit(TRedisHandle(FRedisList.Items[0]));
    end;
  end;
  //�����һ��
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
  aRedis := GetAndChekNode(aKey);
  aRedis.KeyDelete(aKey);
end;

function TRedisClusterHandle.KeyExist(aKey: String): Boolean;
var
  aRedis: TRedisHandle;
begin
  aRedis := GetAndChekNode(aKey);
  Result := aRedis.KeyExist(aKey);
end;

procedure TRedisClusterHandle.KeySetExpire(aKey: String; aExpireSec: Integer);
var
  aRedis: TRedisHandle;
begin
  aRedis := GetAndChekNode(aKey);
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
  //����MOVED 3999 127.0.0.1:6381�е�ip�Ͷ˿�
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

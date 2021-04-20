{
  redis command
}
unit uRedisCommand;

interface

uses
  Classes, SysUtils;

type
  TRedisCommand = class
  private
    FCommandList: TStrings;
  public
    constructor Create(); virtual;
    destructor Destroy; override;

    function Clear(): TRedisCommand;

    function Add(aValue: String): TRedisCommand; overload;
    function Add(aValue: Integer): TRedisCommand; overload;

    function ToRedisCommand: TBytes;
  end;

implementation

const
  //回车换行
  C_CRLF = #$0D#$0A;


{ TRedisCommand }

function TRedisCommand.Add(aValue: String): TRedisCommand;
begin
  FCommandList.Add(aValue);
  Result := Self;
end;

function TRedisCommand.Add(aValue: Integer): TRedisCommand;
begin
  FCommandList.Add(IntToStr(aValue));
  Result := Self;
end;

function TRedisCommand.Clear: TRedisCommand;
begin
  FCommandList.Clear;
  Result := Self;
end;

constructor TRedisCommand.Create;
begin
  inherited;
  FCommandList := TStringList.Create;
end;

destructor TRedisCommand.Destroy;
begin
  FCommandList.Free;
  inherited;
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
function TRedisCommand.ToRedisCommand: TBytes;
var
  aCmd: string;
  i, aLen: Integer;
begin

  //参数个数
  aCmd := '*' + IntToStr(FCommandList.Count) + C_CRLF;

  //params
  for i := 0 to FCommandList.Count - 1 do
  begin
    //string len
    aLen := TEncoding.UTF8.GetByteCount(FCommandList.Strings[i]);

    aCmd := aCmd + '$' + IntToStr(aLen) + C_CRLF
      + FCommandList.Strings[i] + C_CRLF;

  end;

  Result := TEncoding.UTF8.GetBytes(aCmd);

end;

end.

program RedisTest;
{

  Delphi DUnit Test Project
  -------------------------
  This project contains the DUnit test framework and the GUI/Console test runners.
  Add "CONSOLE_TESTRUNNER" to the conditional defines entry in the project options
  to use the console test runner.  Otherwise the GUI test runner will be used by
  default.

}

{$IFDEF CONSOLE_TESTRUNNER}
{$APPTYPE CONSOLE}
{$ENDIF}


uses
  Forms,
  TestFramework,
  GUITestRunner,
  TextTestRunner,
  uRedisHandle in '..\main\uRedisHandle.pas',
  TestuRedisHandle in 'TestuRedisHandle.pas',
  uRedisClusterHandle in '..\main\uRedisClusterHandle.pas',
  uRedisCommand in '..\main\uRedisCommand.pas',
  uRedisCommon in '..\main\uRedisCommon.pas',
  TestuRedisClusterHandle in 'TestuRedisClusterHandle.pas',
  uRedisClusterCRC16 in '..\main\uRedisClusterCRC16.pas';

{$R *.RES}

begin
  Application.Initialize;
  if IsConsole then
    with TextTestRunner.RunRegisteredTests do
      Free
  else
    GUITestRunner.RunRegisteredTests;
end.


set logger.type=window
call "%~dp0set-env.cmd"

set working-path.simulations.folder=%CURRENT_DIR_NAME%\config\simulations
set working-path.complex-simulations.folder=%CURRENT_DIR_NAME%\config\simulations
::set tasks.max-parallel=10


if [%forceMaster%]==[true] (
	set firstWindowTile=SE Lottery simple simulator ^(master mode^)
	set secondWindowTile=SE Lottery complex simulator ^(master mode^)
) else (
	set firstWindowTile=SE Lottery simple simulator ^(slave mode^)
	set secondWindowTile=SE Lottery complex simulator ^(slave mode^)
	set logger.window.background-color=0,0,0
	set logger.window.text-color=255,255,255
)

set lottery.application.name=%firstWindowTile%
start "%firstWindowTile%" /D "%~dp0" "%JAVA_HOME%\bin\%JAVA_COMMAND%" -Xmx%XMX% -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.SELotterySimpleSimulator
set lottery.application.name=%secondWindowTile%
set logger.window.initial-x-position=120
set logger.window.initial-y-position=120
start "%secondWindowTile%" /D "%~dp0" "%JAVA_HOME%\bin\%JAVA_COMMAND%" -Xmx%XMX% -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.SELotteryComplexSimulator

::I comandi sottostanti anzichè aprire più finestre eseguono tutto nella finestra corrente
::set lottery.application.name=%firstWindowTile%
::start "%firstWindowTile%" /D "%~dp0" /b "%JAVA_HOME%\bin\%JAVA_COMMAND%" -Xmx%XMX% -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.SELotterySimpleSimulator
::set lottery.application.name=%secondWindowTile%
::set logger.window.initial-x-position=120
::set logger.window.initial-y-position=120
::start "%secondWindowTile%" /D "%~dp0" /b "%JAVA_HOME%\bin\%JAVA_COMMAND%" -Xmx%XMX% -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.SELotteryComplexSimulator
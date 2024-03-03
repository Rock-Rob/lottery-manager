@echo off

call "%~dp0set-env.cmd"

set working-path.generations.folder=%CURRENT_DIR_NAME%\config\generations
set report.detail.enabled=true

call "%JAVA_HOME%\bin\%JAVA_COMMAND%" -Xmx%XMX% -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.LotteryMatrixGenerator
echo: 
echo:
pause
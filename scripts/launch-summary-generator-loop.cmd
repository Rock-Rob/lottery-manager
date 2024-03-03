@echo off

call "%~dp0set-env.cmd"

set working-path.simulations.folder=Simulazioni

set waiting_time=900

:startLoop
call "%JAVA_HOME%\bin\%JAVA_COMMAND%" -Xmx%XMX% -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.SESimulationSummaryGenerator
echo:
echo Waiting 10 minutes before the next update
timeout /t 600 /NOBREAK > NUL
goto startLoop
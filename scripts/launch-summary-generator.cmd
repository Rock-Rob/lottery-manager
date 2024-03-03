@echo off

call "%~dp0set-env.cmd"

set working-path.simulations.folder=Simulazioni

call "%JAVA_HOME%\bin\%JAVA_COMMAND%" -Xmx%XMX% -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.SESimulationSummaryGenerator
echo:
echo:

start "" /D "%~dp0" /b "%lottery-util.working-path%\%working-path.simulations.folder%\Summary.xlsx"
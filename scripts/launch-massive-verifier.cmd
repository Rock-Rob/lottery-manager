@echo off

call "%~dp0set-env.cmd"

set startDate=01/01/2024
set endDate=next+1*1

call "%JAVA_HOME%\bin\%JAVA_COMMAND%" -Xmx%XMX% -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.SEMassiveVerifierAndQualityChecker
::start "" /B /D "%~dp0" "%JAVA_HOME%\bin\%JAVA_COMMAND%" -Xmx%XMX% -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.SEMassiveVerifierAndQualityChecker

echo: 
echo:

::timeout /t 9 /NOBREAK > NUL

set se-stats.force-loading-from-excel=true
set startDate=14/02/2023
set endDate=30/12/2023

call "%JAVA_HOME%\bin\%JAVA_COMMAND%" -Xmx%XMX% -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.SEMassiveVerifierAndQualityChecker
::start "" /B /D "%~dp0" "%JAVA_HOME%\bin\%JAVA_COMMAND%" -Xmx%XMX% -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.SEMassiveVerifierAndQualityChecker

if [%1]==[no-pause] (
	exit
)
pause
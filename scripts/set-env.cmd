set CURRENT_DIR=%~dp0
if "%CURRENT_DIR:~-1%" == "\" set "CURRENT_DIR_NAME_TEMP=%CURRENT_DIR:~0,-1%"

for %%f in ("%CURRENT_DIR_NAME_TEMP%") do set "CURRENT_DIR_NAME=%%~nxf"

set CURRENT_UNIT=%CURRENT_DIR:~0,2%
set JAVA_HOME=%CURRENT_DIR%jdk\21.0.2-GraalVM
set lottery-util.working-path=%CURRENT_DIR%..
set classPath="%CURRENT_DIR%bin"

if [%logger.type%]==[window] (
	set JAVA_COMMAND=javaw.exe
	set logger.window.max-number-of-characters=1048576
) else (
	set JAVA_COMMAND=java.exe
)

setLocal EnableDelayedExpansion
set LIBS="
for /R "%CURRENT_DIR%lib" %%a in (*.jar) do (
	set LIBS=!LIBS!;%%a
)
set LIBS=!LIBS!"
endlocal & (
  set "LIBS=%LIBS%"
)

set XMX=5g

set se-stats.force-loading-from-excel=false
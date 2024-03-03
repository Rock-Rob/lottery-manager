@echo off

call "%~dp0set-env.cmd"

::call  "%JAVA_HOME%\bin\%JAVA_COMMAND%" -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.SubscriptionExpirationDateUpdater Liberati-Claudio,4w;Carrazza-Alessandro,4w;Coletta-Antonello,4w;Coletta-Giuseppe,4w;
::call  "%JAVA_HOME%\bin\%JAVA_COMMAND%" -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.SubscriptionExpirationDateUpdater Bellacanzone-Emanuele,4w;
::call  "%JAVA_HOME%\bin\%JAVA_COMMAND%" -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.SubscriptionExpirationDateUpdater Fusi-Francesco,4w;
::call  "%JAVA_HOME%\bin\%JAVA_COMMAND%" -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.SubscriptionExpirationDateUpdater Corinti-Massimo,1w;
::call  "%JAVA_HOME%\bin\%JAVA_COMMAND%" -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.SubscriptionExpirationDateUpdater Berni-Valentina,6w;
::call  "%JAVA_HOME%\bin\%JAVA_COMMAND%" -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.SubscriptionExpirationDateUpdater Pistella-Maria-Anna,4w;
::call  "%JAVA_HOME%\bin\%JAVA_COMMAND%" -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.SubscriptionExpirationDateUpdater Ingegneri-Giuseppe,10w;
::call  "%JAVA_HOME%\bin\%JAVA_COMMAND%" -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.SubscriptionExpirationDateUpdater Mancini-Alessandro,20w;
::call  "%JAVA_HOME%\bin\%JAVA_COMMAND%" -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.SubscriptionExpirationDateUpdater Berni-Riccardo,8w;
::call  "%JAVA_HOME%\bin\%JAVA_COMMAND%" -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.SubscriptionExpirationDateUpdater Del-Giovine-Claudio,4w;
::call  "%JAVA_HOME%\bin\%JAVA_COMMAND%" -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.SubscriptionExpirationDateUpdater all,1d;
call  "%JAVA_HOME%\bin\%JAVA_COMMAND%" -Xmx%XMX% -cp %classPath%;%LIBS%;"%CURRENT_DIR%binaries.jar"; org.rg.game.lottery.application.SubscriptionExpirationDateUpdater
echo:
echo:
pause
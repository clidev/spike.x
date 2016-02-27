@if "%DEBUG%" == "" @echo off
@rem ##########################################################################
@rem
@rem  vertx startup script for Windows
@rem
@rem ##########################################################################

@rem Set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" setlocal

set DIRNAME=%~dp0
if "%DIRNAME%" == "" set DIRNAME=.
set APP_BASE_NAME=%~n0
set VERTX_HOME=%DIRNAME%..

@rem Add default JVM options here. You can also use JAVA_OPTS and VERTX_OPTS to pass JVM options to this script.
@rem set VERTX_OPTS=-Dvertx.serialiseBlockingActions=true -Dvertx.javaCompilerOptions=-Xlint:deprecation

set JVM_OPTS=-Xms64m -Xmx256m -Xss128k -XX:MaxMetaspaceSize=64M
set JVM_OPTS=%JVM_OPTS% -XX:+UseG1GC -XX:+ParallelRefProcEnabled -XX:+UseStringDeduplication
set JVM_OPTS=%JVM_OPTS% -verbose:gc -XX:+PrintAdaptiveSizePolicy -XX:+PrintGCDateStamps -XX:+PrintGCDetails -Xloggc:"%VERTX_HOME%\log\spikex-gc.log" -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10M
set JVM_OPTS=%JVM_OPTS% -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath="%VERTX_HOME%\log\spikex-dump.hprof"
set JVM_OPTS=%JVM_OPTS% -Dspikex.home="%VERTX_HOME%" -Dlogback.logdir="%VERTX_HOME%\log" -Djava.io.tmpdir="%VERTX_HOME%\tmp" -Dio.netty.tmpdir="%VERTX_HOME%\tmp" -Dio.netty.native.workdir="%VERTX_HOME%\tmp"
set JVM_OPTS=%JVM_OPTS% -Dorg.vertx.logger-delegate-factory-class-name=org.vertx.java.core.logging.impl.SLF4JLogDelegateFactory
set JVM_OPTS=%JVM_OPTS% -Dlogback.configurationFile=logback-console.xml
set JVM_OPTS=%JVM_OPTS% -Dsun.net.inetaddr.ttl=30
set JVM_OPTS=%JVM_OPTS% -Djava.awt.headless=true
set JVM_OPTS=%JVM_OPTS% -Dvisualvm.display.name=Spike.x
set JVM_OPTS=%JVM_OPTS% -Dio.netty.noUnsafe=true

set JMX_OPTS=
@rem To enable JMX uncomment the following
@rem set JMX_OPTS=-Dcom.sun.management.jmxremote -Dvertx.management.jmx=true -Dhazelcast.jmx=true
@rem set JMX_OPTS=%JMX_OPTS% -Dcom.sun.management.jmxremote.port=9099
@rem set JMX_OPTS=%JMX_OPTS% -Dcom.sun.management.jmxremote.ssl=false
@rem set JMX_OPTS=%JMX_OPTS% -Dcom.sun.management.jmxremote.authenticate=true
@rem set JMX_OPTS=%JMX_OPTS% -Dcom.sun.management.jmxremote.access.file="%VERTX_HOME%\conf\jmxremote.access
@rem set JMX_OPTS=%JMX_OPTS% -Dcom.sun.management.jmxremote.password.file="%VERTX_HOME%\conf\jmxremote.password

@rem Find java.exe
if defined JAVA_HOME goto findJavaFromJavaHome

set JAVA_EXE=java.exe
%JAVA_EXE% -version >NUL 2>&1
if "%ERRORLEVEL%" == "0" goto init

echo.
echo ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

goto fail

:findJavaFromJavaHome
set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%/bin/java.exe

if exist "%JAVA_EXE%" goto init

echo.
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME%
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

goto fail

:init
@rem Add module option to commandline, if VERTX_MODS was set
if not "%VERTX_MODS%" == "" set VERTX_MODULE_OPTS="-Dvertx.mods=%VERTX_MODS%"

@rem Configure JUL using custom properties file
if "%VERTX_JUL_CONFIG%" == "" set VERTX_JUL_CONFIG=%VERTX_HOME%\conf\logging.properties

@rem Specify ClusterManagerFactory
if "%VERTX_CLUSTERMANAGERFACTORY%" == "" set VERTX_CLUSTERMANAGERFACTORY=org.vertx.java.spi.cluster.impl.hazelcast.HazelcastClusterManagerFactory

@rem Get command-line arguments, handling Windowz variants

if not "%OS%" == "Windows_NT" goto win9xME_args
if "%@eval[2+2]" == "4" goto 4NT_args

:win9xME_args
@rem Slurp the command line arguments.
set CMD_LINE_ARGS=
set _SKIP=2

:win9xME_args_slurp
if "x%~1" == "x" goto execute

set CMD_LINE_ARGS=%*
goto execute

:4NT_args
@rem Get arguments from the 4NT Shell from JP Software
set CMD_LINE_ARGS=%$

:execute
@rem Setup the command line

set CLASSPATH=%CLASSPATH%;%VERTX_HOME%\conf;%VERTX_HOME%\lib\*

@rem Execute vertx
"%JAVA_EXE%" %JVM_OPTS% %JMX_OPTS% %JAVA_OPTS% %VERTX_OPTS% %VERTX_MODULE_OPTS% -Djava.util.logging.config.file="%VERTX_JUL_CONFIG%" -Dvertx.home="%VERTX_HOME%" -Dvertx.clusterManagerFactory="%VERTX_CLUSTERMANAGERFACTORY%" -classpath "%CLASSPATH%" io.spikex.core.Main %CMD_LINE_ARGS%

:end
@rem End local scope for the variables with windows NT shell
if "%ERRORLEVEL%"=="0" goto mainEnd

:fail
rem Set variable VERTX_EXIT_CONSOLE if you need the _script_ return code instead of
rem the _cmd.exe /c_ return code!
if  not "" == "%VERTX_EXIT_CONSOLE%" exit 1
exit /b 1

:mainEnd
if "%OS%"=="Windows_NT" endlocal

:omega

@echo off
set JAVA_HOME=C:\Program Files\Java\jdk-17
set PATH=%JAVA_HOME%\bin;%PATH%
cd /d C:\Users\AlejandroAmaya\Downloads\trasep
".venv\Scripts\python.exe" -m trasep seed
".venv\Scripts\python.exe" -m trasep batch

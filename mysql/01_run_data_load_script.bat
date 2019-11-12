@echo off

REM
REM First, copy the CSV data files to the secure
REM upload directory referred to by the MySQL
REM 'SHOW VARIABLES LIKE "secure_file_priv";'
REM command.
REM
REM You may also need to increase the MySQL
REM server parameter 'innodb_buffer_pool_size'
REM from its default value of 8 MB. Edit this 
REM setting in your server's my.ini file, and
REM then restart the server. Recommended: 64 MB.
REM Check the value with this command:
REM 'SELECT @@innodb_buffer_pool_size / 1024 / 1024;'
REM

copy /Y ^
  ..\imdb\csv\sampled\*.csv ^
  "C:\ProgramData\MySQL\MySQL Server 8.0\Uploads\"

"E:\MySQL\MySQL Workbench 6.3 CE\mysql.exe" ^
  --defaults-file="my_conf.cnf"  ^
  --user=imdb_user ^
  --host=localhost ^
  --protocol=tcp ^
  --port=3306 ^
  --default-character-set=utf8 ^
  --verbose ^
  --show-warnings ^
  --tee=output.txt ^
  "imdb" < mysql_imdb_create_and_load.sql

pause

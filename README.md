

## Running on Windows

* Copy "winutils.exe" and "hadoop.dll" from [winutils](https://github.com/steveloughran/winutils/tree/master/hadoop-2.7.1/bin) in a folder, for example d:\hadoop\bin. 
* Set the user variable %HADOOP_HOME$ to d:\hadoop and add d:\hadoop\bin to the system variable %PATH%.
* Use a temporary folder without spaces in the EmbeddedHdfsSpark.startHdfs. If the folder does not exist, it will be created

```code
 startHdfs(new File("c:\\tmp"))
```
* It is recommended to use either an absolute destination folder in EmbeddedHdfsSpark.copyFromLocal(source, destination). Otherwise, the Windows implementation uses the local user name and if that name contains spaces, it seems to go wrong. 

## Use Java 8 

For version Spark 2.3.x --> limited to Java 8
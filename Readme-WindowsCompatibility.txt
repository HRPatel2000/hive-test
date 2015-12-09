Using Hadoop compiled for windows:

    - Locate tar file from nexus under 'com.cat.ddsw:hadoopwinnative:${version}' for a particular ${version}
    Nexus Url for 2.5.0-cdh5.3.3:
        http://arlpscsp01.ecorp.cat.com:8081/nexus/service/local/repositories/thirdparty/content/com/cat/ddsw/hadoopwinnative/2.5.0-cdh5.3.3/hadoopwinnative-2.5.0-cdh5.3.3-2.5.0-cdh5.3.3..tar.gz

    - Untar to target directory and add path of target directory to new Environment Variable, HADOOP_HOME.
    - Add "HADDOP_HOME\bin" to the Windows Environment variable "Path".
    - Run HiveRunner tests in hive-test



Compiling Hadoop Source Code 2.5.0-cdh5.3.3 for Windows:
 - Find version of the artifact, "org.apache.hadoop:hadoop-common", used in the project i.e. "2.5.0-cdh5.3.3" from http://archive.cloudera.com/cdh5/cdh/5/ (i.e. hadoop-2.5.0-cdh5.3.3.tar.gz)
 - Acquire source code for this version of hadoop.
 - Extract source code to the root directory of your drive (Windows can have issues with long path names)
 - Check BUILD.txt to see if there are any changes to the required software and versions for newer versions of the hadoop src code and modify this document accordingly.
 - Start clean. Get rid of all Windows Visual c++ 2010 compilers, Visual studio's, and Windows SDK's.
 - All environment variables below (i.e. Path) are referring to Windows 'System Variables' and not 'User variables for logged-in user'
 - Requirements:
    1. Download Maven binaries from https://maven.apache.org/download.cgi
    2. Add 'bin' folder of maven to Path
    3. Download and install Cygwin
    4. Add 'bin' directory of Cygwin to Path
    5. Download and install CMake (Windows Installer) from http://www.cmake.org/download/
    6. Add CMake's bin directory to the path
    7. Download Google’s Protocol Buffers version 2.5.0 (no other version will work for hadoo-2.5.0) from https://github.com/google/protobuf/releases/download/v2.5.0/protoc-2.5.0-win32.zip
    8. Extract protoc and add the path of protoc.exe to an evironment variable HADOOP_PROTOC_CDH5_PATH (i.e. C:\hadoop\protocDir\protoc.exe)
    9. Add '%HADOOP_PROTOC_CDH5_PATH%' to the Path
   10. Download and install “Visual Studio 2010 Professional” (Trial is enough) from http://download.microsoft.com/download/D/B/C/DBC11267-9597-46FF-8377-E194A73970D6/vs_proweb.exe
   11. Add the location of newly installed MSBuild.exe (i.e. c:\Windows\Microsoft.NET\Framework64\v4.0.30319;) to the Path.
 - Open "Visual Studio x64 Command Prompt (2010)" in Administrator mode
 - Execute: “set Platform=x64” (assuming you want 64-bit version, otherwise use “set Platform=Win32”)
 - Navigate to the root directory of the hadoop source code
 - Execute maven build using 'native-win' profile
    Command: 'mvn package -Pdist,native-win -DskipTests -Dtar'
 - Find tar in ${HadoopSourceRoot}\hadoop-dist\target
 - Upload tar file to Nexus

 ** Other good resources (use "Visual Studio 2010" only and not a different version Visual Studio or Windows SDK):
      Windows 7: https://wiki.apache.org/hadoop/Hadoop2OnWindows (Sections 1 & 2)
      Static version included in file "Windows7HadoopCompile.pdf"

      OR

      Windows 8: http://mariuszprzydatek.com/2015/05/10/installing_hadoop_on_windows_8_or_8_1/
      Static version included in file "Windows8HadoopCompile.pdf"

 ** When compiling for windows there might be an issue or two that needs fixing in the source code.
 For hadoop2.5.0-cdh5.3.3 there was one issue that needed fixing that had to do with https://issues.apache.org/jira/browse/HADOOP-10925.
 The patch on that page was simple to apply and worked.
 Fix, Recompile, and Retry.



In the HiveRunner test cases:

 - Don't use windows directories in HQL files but opt for URI's as is done in reference implementation.
 - HiveRunner has been modified to add URI's as properties accessible from HQL.


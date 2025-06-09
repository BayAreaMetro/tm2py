# Set up server

## MTC's server

MTC is running tm2py on Intel(R) Xeon(R) Gold 6338 CPU @ 2.00GHz processors, 24 sockets, 48 cores.  
The servers have 512 GB of memory and run Windows Server 2019 Standard.

The C: drive has 80 GB, and an external E: drive has about 1 TB of disk space.

## Required Software

1. [OpenPaths/EMME (24.01) Advanced](http://softwaredownloads.bentley.com/)  
   From this link, search for OpenPaths, then install **OpenPaths 204 Update 1 (SES)**.

2. [Java 1.8.0 162](https://www.oracle.com/java/technologies/javase/javase8-archive-downloads.html#license-lightbox)  
   CTRAMP runs on Java. 

3. [Box](https://www.box.com/resources/downloads)  
   MTC gets its input files from Box, so this is required for now. We’ll need to figure out a long-term location for inputs.  

   To install Box, you also need the [Microsoft .NET Framework 4.8](https://dotnet.microsoft.com/en-us/download/dotnet-framework/net48), and you will need to restart your machine.  

   MTC staff run the model from the E: Box location. To configure the Box folder location, follow these instructions:  
   https://support.box.com/hc/en-us/articles/360043697454-Configuring-the-Default-Box-Drive-Folder-Location

4. Git/Git Bash and/or GitHub Desktop  
   You’ll need this to clone the tm2py repository. Just search online to install.

## Optional Software

1. Visual Studio Code  
   This can be used to debug Python and run Jupyter notebooks. Install the **Python**, **Java** and **Jupyter** extensions.and run jupyter notebooks. Install the python and jupyter notebook extensions.






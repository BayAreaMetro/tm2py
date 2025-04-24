# How to setup your server to run tm2py

## MTC's server
MTC is running tm2py Intel(R) Xeon(R) Gold 6338 CPU @ 2.00GHz processors, 24 sockets 48 cores.
The servers have 512 GB of memory. It's running Windows server 2019 standard.

The C: drive has 80 GB, and an external E: drive has about 1 TB of disk space.

## Required Software

1. [Open Paths/EMME (24.01) Advanced](http://softwaredownloads.bentley.com/)

From this link, search for OpenPaths, then Install OpenPaths 204 Update 1 (SES)

2. [Java 1.8.0 162](https://www.oracle.com/java/technologies/javase/javase8-archive-downloads.html#license-lightbox)

CTRAMP runs on java. You might eventually want a java IDE. You can use Eclipse if you want.

3. [Box](https://www.box.com/resources/downloads)

    MTC gets it's input files from Box, so this is requirement for now, we'll have to figured out long term where people will get inputs. 
    
    To install Box, you also need [Microsoft .NET framework 4.8](https://dotnet.microsoft.com/en-us/download/dotnet-framework/net48), and will need to restart.

    MTC staff run from the E: Box location, here is how to configure the box location: https://support.box.com/hc/en-us/articles/360043697454-Configuring-the-Default-Box-Drive-Folder-Location

4. Git/Git Bash and/or GitHub Desktop (just google it)

   So you can clone tm2py.

## Optional Software
1. Visual Studio Code

This can be used to debug python and run jupyter notebooks. Install the python and jupyter notebook extensions.






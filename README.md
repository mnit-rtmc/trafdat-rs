The trafdat module provides an http interface to access the traffic
data archives.  Since the .traffic files are very large and would take
considerable bacndwidth to transfer to the client analysis tools,
trafdat can locate the requested data within a traffic archive and
provide just the pertinent data set.  This module has nothing to do
with storing or zipping the archives... that is all handled by the 
IRIS server.  It also does no processing/parsing of the data.
It just sends the requested file to the client.

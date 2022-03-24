#!/bin/bash

# Installs SIGAR on MacOS
# @author Chris Phillipson
# @version 1.0.0
# @credit https://stackoverflow.com/questions/11266895/hyperic-sigar-mac-osx-error-no-library


# to find it later because you will need it ...
cd ~/Downloads/

# or whatever the latest one at the time of reading is ...
wget https://sourceforge.net/projects/sigar/files/latest/download -O hyperic-sigar-1.6.4.zip

# unpack the package to the tmp dir
sudo unzip -o $HOME/Downloads/hyperic-sigar-1.6.4.zip -d /tmp/

# copy the libsigar-universal64-macosx.dylib to your class path dir
sudo find /tmp/ -name libsigar-universal64-macosx.dylib \
   -exec cp -v {} /Library/Java/Extensions/ \;

# this cmd might be obsolete ...
# copy the sigar.jar to your class path dir
sudo find /tmp/ -name sigar*.jar \
   -exec cp -v {} /Library/Java/Extensions/ \;

# @see https://stackoverflow.com/questions/11266895/hyperic-sigar-mac-osx-error-no-library
echo "Add [ -Djava.library.path=$JAVA_LIBRARY_PATH:~/Library/Java/Extensions:/Library/Java/Extensions:/Network/Library/Java/Extensions:/System/Library/Java/Extensions:/usr/lib/java:. ] to the end of [ gradle build ]."
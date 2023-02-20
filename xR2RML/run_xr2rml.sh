#!/bin/bash

# -- Example script to run xR2RML --

# Set the variables below first
XR2RML=//home/happihappibill/xR2RML
mappingFile=mapping.ttl

java -jar ./morph-xr2rml-dist-1.3.2-jar-with-dependencies.jar --configDir ./ --configFile ./morph.properties --mappingFile ./mapping.ttl --output ./output.ttl

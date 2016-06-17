# BulkReanalysisDownload
Downloads a whole NCEP/NCAR Reanalysis 1 into single NetCDF file (one file for each variable)

usage: **BulkReanalysisDownload.exe \<layer\> \<variable\> \<start year\>**

layer - variable pair identifies the data to download

Available layer - varaibles combinations:

 * layer "surface":
   * "air"
   * "prate"

\<start year\> - the first year to begin download with. The data is downloaded until most recent entry

Exmaple: **BulkReanalysisDownload.exe surface air 1948**

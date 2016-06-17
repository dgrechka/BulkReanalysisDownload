open System.IO
open System.Net
open System.Text

type DataSet = Microsoft.Research.Science.Data.DataSet
type Variable = Microsoft.Research.Science.Data.Variable

let githubURL = "http://github.com/dgrechka/BulkReanalysisDownload/"

let printHelp() =
    printfn ""
    printfn "usage: BulkReanalysisDownload.exe <layer> <variable> <start year>"
    printfn ""
    printfn "layer - variable pair identifies the data to download"
    printfn ""
    printfn "Available layer - varaibles combinations:"
    printfn "\tlayer \"surface\":"
    printfn "\t\t\t  \"air\""
    printfn "\t\t\t  \"prate\""
    printfn ""
    printfn "<start year> - the first year to begin download with. The data is downloaded until most recent entry"
    printfn ""
    printfn "Exmaple: BuldReanalysisDownload.exe surface air 1948"


let ftpUrl layer var year =
    let baseUrl = "ftp://ftp.cdc.noaa.gov/Datasets/ncep.reanalysis"
    match layer with
    |   "surface"   ->
        match var with
        |   "air"   ->  Some(sprintf "%s/surface/air.sig995.%d.nc" baseUrl year)
        |   "prate" ->  Some(sprintf "%s/surface_gauss/prate.sfc.gauss.%d.nc" baseUrl year)
        |   _   ->  None
    |   _   -> None

let creds = NetworkCredential("anonymous","reanslysis@merger.com")

let bufferSize=1024*1024*256

let getSurfaceVariableFile layerName varName year =
    let rec copy (rs:Stream) (ws:Stream) buffer =
        let bytesRead = rs.Read(buffer,0,buffer.Length)
        if bytesRead = 0 then
            ()            
        else
            ws.Write(buffer, 0, bytesRead);
            copy rs ws buffer    
    try
        let addr = ftpUrl layerName varName year
        match addr with
        |   Some url    ->
            let request =
                WebRequest.Create(url)
                :?> (FtpWebRequest)
            request.Method <- WebRequestMethods.Ftp.DownloadFile
            request.Credentials <- creds :> ICredentials
            //request.UseBinary <- true;
            request.UsePassive <- true;
            request.KeepAlive <- true;
            printf "Downloading %s..." url
            let response = request.GetResponse()
            let response = response :?> FtpWebResponse 
            let filename = Path.GetRandomFileName()
            use rs = response.GetResponseStream()
            use ws = new FileStream(filename,FileMode.Create)
            let buffer = Array.zeroCreate<byte>(bufferSize)
            copy rs ws buffer            
            printfn "Done (%s)" response.StatusDescription
            Some(filename)
        |   None    ->
            printfn "The layer (%s) - variable (%s) combination is not supported yet. Add it yourself at %s" layerName varName githubURL
            None
    with        
    |   :? System.Net.WebException ->
        printfn "Not found";
        None    

let writeVariableSpecificMetadata (variable:Variable) =
    match variable.Name with
    |   "air"   ->
        variable.Metadata.["spatial_variogram_nugget"] <- 0.0
        variable.Metadata.["spatial_variogram_sill"] <- 1446.7854126267
        variable.Metadata.["spatial_variogram_range"] <- 6040.6671685173
        variable.Metadata.["spatial_variogram_family"] <- "Exponential"
        variable.Metadata.["spatial_variogram_createtime"] <- System.DateTime(2014,02,10)
        variable.Metadata.["2_variogram_nugget"] <- 1.75732521595007
        variable.Metadata.["2_variogram_sill"] <- 5.52220504709561
        variable.Metadata.["2_variogram_range"] <- 261.9008
        variable.Metadata.["2_variogram_family"] <- "Exponential"
        variable.Metadata.["2_variogram_createtime"] <- System.DateTime(2014,02,10)
    |   "prate"   ->
        variable.Metadata.["spatial_variogram_nugget"] <- 1.5635e-09
        variable.Metadata.["spatial_variogram_sill"] <- 9.598198e-09
        variable.Metadata.["spatial_variogram_range"] <- 3998.77904
        variable.Metadata.["spatial_variogram_family"] <- "Spherical"
        variable.Metadata.["spatial_variogram_createtime"] <- System.DateTime(2014,02,10)
        variable.Metadata.["2_variogram_nugget"] <- 2.58929197e-09
        variable.Metadata.["2_variogram_sill"] <- 7.074534e-09
        variable.Metadata.["2_variogram_range"] <- 18.81397006
        variable.Metadata.["2_variogram_family"] <- "Exponential"
        variable.Metadata.["2_variogram_createtime"] <- System.DateTime(2014,02,10)
    |   _   ->()

[<EntryPoint>]
let main argv = 
    if argv.Length<3 then
        printHelp()
        1
    else
        let layerName= argv.[0]
        let varName = argv.[1]
        let startYear = System.Int32.Parse(argv.[2])

        if (ftpUrl layerName varName startYear).IsNone then
            printfn "The layer (%s) - variable (%s) combination is not supported yet. Add it yourself at %s" layerName varName githubURL
            printHelp()
            2
        else
            printfn "Constructing NetCDF file %s_%s.nc for variable '%s' of '%s' layer" varName layerName varName layerName
            let datasetURL = sprintf "msds:nc?file=%s_%s.nc&openMode=create" varName layerName
            use dataSet = Microsoft.Research.Science.Data.DataSet.Open(datasetURL)    

            let variablesToBulkCopy = ["lat"; "lon"]

            let folder (dataSet:DataSet) file =
                let sourceDs = DataSet.Open(sprintf "msds:nc?file=%s&openMode=readOnly" file)
                let sourceVar,sourceTimeVar = sourceDs.Variables.[varName],sourceDs.Variables.["time"]
                let sourceData,timeData = sourceVar.GetData(),sourceTimeVar.GetData()
                let targetVar,targetTimeVar =
                    if dataSet.Variables.Contains(varName) then
                        dataSet.Variables.[varName],dataSet.Variables.["time"]
                    else
                        //first file in a series
                        dataSet.Metadata.["downloaded_with"] <- (sprintf "Bulk Reanalysis Downloader (%s)" githubURL)
                        dataSet.Metadata.["downloaded_on"] <- System.DateTime.UtcNow.ToLongDateString()

                        for bulkCopyVar in variablesToBulkCopy do
                            let sourceV = sourceDs.Variables.[bulkCopyVar]                    
                            let v = dataSet.AddVariable<System.Single>(bulkCopyVar,sourceV.GetData(),sourceV.Dimensions.AsNamesArray())
                            //metadata for 1D variables                    
                            for key in sourceV.Metadata.AsDictionary().Keys do
                                v.Metadata.[key] <- sourceV.Metadata.[key]

                        //global metadata
                        for key in sourceDs.Metadata.AsDictionary().Keys do
                            dataSet.Metadata.[key] <- sourceDs.Metadata.[key]
                        
                        //placeholders for incremental updates
                        let targetV = dataSet.AddVariable<System.Single>(varName,sourceVar.Dimensions.AsNamesArray()) :> Microsoft.Research.Science.Data.Variable
                        let targetTimeV = dataSet.AddVariable<float>("time",[|"time"|]) :> Microsoft.Research.Science.Data.Variable

                        //target var metadata
                        for key in sourceVar.Metadata.AsDictionary().Keys do
                                targetV.Metadata.[key] <- sourceVar.Metadata.[key]
                        writeVariableSpecificMetadata targetV
                        //time var metadat
                        for key in sourceTimeVar.Metadata.AsDictionary().Keys do
                                targetTimeV.Metadata.[key] <- sourceTimeVar.Metadata.[key]
                        targetV,targetTimeV
                targetVar.Append(sourceData,"time")
                targetTimeVar.Append(timeData)
                dataSet.Commit()
                sourceDs.Dispose()
                File.Delete file
                dataSet
                         
            Seq.initInfinite (fun i -> startYear+i)
                |> Seq.map (fun year -> getSurfaceVariableFile layerName varName year)    
                |> Seq.takeWhile (fun elem -> elem.IsSome)
                |> Seq.choose (fun elem -> elem)
                |> Seq.fold folder dataSet
                |> ignore
            printfn "Your dataset is ready"
            0 // return an integer exit code

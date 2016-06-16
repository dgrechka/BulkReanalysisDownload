open System.IO
open System.Net
open System.Text

type DataSet = Microsoft.Research.Science.Data.DataSet
type Variable = Microsoft.Research.Science.Data.Variable

let creds = NetworkCredential("anonymous","reanslysis@merger.com")

let bufferSize=1024*1024*256

let getSurfaceVariableFile varName year =
    let rec copy (rs:Stream) (ws:Stream) buffer =
        let bytesRead = rs.Read(buffer,0,buffer.Length)
        if bytesRead = 0 then
            ()            
        else
            ws.Write(buffer, 0, bytesRead);
            copy rs ws buffer    
    try
        let addr = sprintf "ftp://ftp.cdc.noaa.gov/Datasets/ncep.reanalysis/surface/%s.%d.nc" varName year
        let request =
            WebRequest.Create(addr)
            :?> (FtpWebRequest)
        request.Method <- WebRequestMethods.Ftp.DownloadFile
        request.Credentials <- creds :> ICredentials
        //request.UseBinary <- true;
        request.UsePassive <- true;
        request.KeepAlive <- true;
        printf "Downloading %s..." addr
        let response = request.GetResponse()
        let response = response :?> FtpWebResponse 
        let filename = Path.GetRandomFileName()
        use rs = response.GetResponseStream()
        use ws = new FileStream(filename,FileMode.Create)
        let buffer = Array.zeroCreate<byte>(bufferSize)
        copy rs ws buffer            
        printfn "Done (%s)" response.StatusDescription
        Some(filename)
    with        
    |   :? System.Net.WebException ->
        printfn "Not found";
        None    

[<EntryPoint>]
let main argv = 
    let startYear = 2015    
    let varName = "air"    
    let layerName = "sig995"

    let varFileName = sprintf "%s.%s" varName layerName

    let datasetURL = sprintf "msds:nc?file=%s.nc&openMode=create" varFileName
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
        |> Seq.map (fun year -> getSurfaceVariableFile varFileName year)    
        |> Seq.takeWhile (fun elem -> elem.IsSome)
        |> Seq.choose (fun elem -> elem)
        |> Seq.fold folder dataSet
        |> ignore
    0 // return an integer exit code

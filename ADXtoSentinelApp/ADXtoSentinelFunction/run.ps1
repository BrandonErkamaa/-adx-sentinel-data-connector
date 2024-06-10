param($Timer)

Install-Module -Name Az.Accounts -Scope CurrentUser -Force -AllowClobber
Install-Module -Name Az.Kusto -Scope CurrentUser -Force -AllowClobber

# Environment variables
$ADX_CLUSTER = $env:ADX_CLUSTER
$ADX_DATABASE = $env:ADX_DATABASE
$TABLE_NAME = $env:TABLE_NAME
$STORAGE_CONNECTION_STRING = $env:AzureWebJobsStorage
$STATE_TABLE_NAME = "adxStateTable"
$STATE_PARTITION_KEY = "adxState"
$STATE_ROW_KEY = "lastProcessedRow"

# Function to get ADX token using Managed Identity
function GetAdxToken {
    Connect-AzAccount -Identity
    $resource = "https://smartaccessexplorer.centralus.kusto.windows.net"
    $token = (Get-AzAccessToken -ResourceUrl $resource).Token
    return $token
}

# Function to query ADX using the retrieved token
function QueryAdx {
    $token = GetAdxToken
    $headers = @{
        "Authorization" = "Bearer $token"
    }
    $uri = "https://$ADX_CLUSTER.centralus.kusto.windows.net/v2/rest/query"
    
    # Query to take 10 rows from the table
    $query = "['$TABLE_NAME'] | take 10"

    $body = @{
        "db"  = $ADX_DATABASE
        "csl" = $query
    } | ConvertTo-Json

    try {
        $response = Invoke-RestMethod -Method Post -Uri $uri -Headers $headers -ContentType "application/json" -Body $body
        return $response
    }
    catch {
        Write-Error "Error querying ADX: $_"
        throw
    }
}

# Timer-triggered function execution
try {
    # Get new data from ADX
    $results = QueryAdx
    Write-Output "Query Results:"
    Write-Output $results
}
catch {
    Write-Error "Error during function execution: $_"
}
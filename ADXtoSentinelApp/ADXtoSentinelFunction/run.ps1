param($Timer)

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
    $tokenEndpoint = "http://169.254.169.254/metadata/identity/oauth2/token"
    $params = @{
        "api-version" = "2018-02-01"
        "resource"    = "https://kusto.kusto.windows.net"
    }
    $headers = @{
        "Metadata" = "true"
    }
    try {
        # Constructing URI by concatenating token endpoint and parameters
        $uri = $tokenEndpoint + "?" + ($params.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join '&'
        $response = Invoke-RestMethod -Method Get -Uri $uri -Headers $headers
        return $response.access_token
    }
    catch {
        Write-Error "Error getting ADX token: $_"
        throw
    }
}

# Function to query ADX using the retrieved token
function QueryAdx {
    $token = GetAdxToken
    $headers = @{
        "Authorization" = "Bearer $token"
    }
    $uri = "https://$ADX_CLUSTER.kusto.windows.net/v2/rest/query"
    
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
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

$tenantId = "8f445392-4de8-4998-80f6-1f324068d229"
$SubscriptionId = "df87a0ba-c88a-4273-83f9-23338d08f3fc"
$ClientID = "5614d2cd-2239-43b3-8dc5-209512107993"



# Function to get ADX token using Managed Identity
function GetAdxToken {
    Write-Host "Logging in using User Managed Identity"
    Connect-AzAccount -Identity -AccountId $ClientId

    Write-Host "Getting token"
    $resource = "https://smartaccessexplorer.centralus.kusto.windows.net"
    $response = Get-AzAccessToken -ResourceUrl $resource
    Write-Host "response is $response"
    $token = $response.Token
    Write-Host "return value is $token"
    return $token
}

# Function to query ADX using the retrieved token
function QueryAdx {
    $token = GetAdxToken
    Write-Host "Token is , $token"
    Write-Host "Querying ADX"
    $headers = @{
        "Authorization" = "Bearer $token"
    }
    Write-Host "Headers are $headers"
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
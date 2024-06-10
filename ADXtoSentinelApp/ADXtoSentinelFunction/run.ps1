param($Timer)

# Install necessary modules
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
    Connect-AzAccount -Identity -AccountId $ClientID

    Write-Host "Getting token"
    $resource = "https://smartaccessexplorer.centralus.kusto.windows.net"
    $token = (Get-AzAccessToken -ResourceUrl $resource).Token
    
    Write-Host "Token retrieved: $token"  # Log first 50 chars for security
    return [string]$token
}

# Function to query ADX using the retrieved token
function QueryAdx {
    param (
        [Parameter(Mandatory = $true)]
        [string]$token
    )

    Write-Host "Token being used: $token"
    Write-Host "Querying ADX"

    # Check the token's validity
    if (-not $token) {
        throw "Token is null or empty. Aborting query."
    }

    $headers = @{
        "Authorization" = "Bearer $token"
    }

    Write-Host "Headers are @{Authorization=Bearer $token}"

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
    $token = GetAdxToken
    Write-Host "Token in main, $token"
    $results = QueryAdx -token $token
    Write-Output "Query Results:"
    Write-Output $results
}
catch {
    Write-Error "Error during function execution: $_"
}
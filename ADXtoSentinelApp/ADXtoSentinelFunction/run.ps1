param($Timer)

$currentUTCtime = (Get-Date).ToUniversalTime()

if ($Timer.IsPastDue) {
    Write-Host "PowerShell timer is running late! $($Timer.ScheduledStatus.Last)"
}

# Install necessary modules
Install-Module -Name Az.Accounts -Scope CurrentUser -Force -AllowClobber
Install-Module -Name Az.Kusto -Scope CurrentUser -Force -AllowClobber

# Environment variables
$ADX_CLUSTER = $env:ADX_CLUSTER
$ADX_DATABASE = $env:ADX_DATABASE
$TABLE_NAME = $env:TABLE_NAME
$SENTINEL_WORKSPACE_ID = $env:SENTINEL_WORKSPACE_ID
$logAnalyticsUri = "https://" + $SENTINEL_WORKSPACE_ID + ".ods.opinsights.azure.com"
$SENTINEL_SHARED_KEY = $env:SENTINEL_WORKSPACE_KEY
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
    $null = Connect-AzAccount -Identity -AccountId $ClientID

    Write-Host "Getting token"
    $resource = "https://smartaccessexplorer.centralus.kusto.windows.net"
    $token = (Get-AzAccessToken -ResourceUrl $resource).Token
    
    Write-Host "Token retrieved: $token"
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

# Function to build the signature for the request
Function Build-Signature {
    param (
        [string]$customerId,
        [string]$sharedKey,
        [string]$date,
        [int]$contentLength,
        [string]$method,
        [string]$contentType,
        [string]$resource
    )
    
    $xHeaders = "x-ms-date:" + $date
    $stringToHash = $method + "`n" + $contentLength + "`n" + $contentType + "`n" + $xHeaders + "`n" + $resource

    Write-Host "String to hash: $stringToHash"
    Write-Host "Shared key: $sharedKey"
    Write-Host "Customer ID: $customerId"

    $bytesToHash = [Text.Encoding]::UTF8.GetBytes($stringToHash)
    $keyBytes = [Convert]::FromBase64String($sharedKey)

    Write-Host "Bytes to hash: $($bytesToHash -join ',')"
    Write-Host "Key bytes: $($keyBytes -join ',')"   

    $sha256 = New-Object System.Security.Cryptography.HMACSHA256
    $sha256.Key = $keyBytes
    $calculatedHash = $sha256.ComputeHash($bytesToHash)
    $encodedHash = [Convert]::ToBase64String($calculatedHash)
    $authorization = 'SharedKey {0}:{1}' -f $customerId, $encodedHash
    Write-Host "Authorization: $authorization"
    
    # Dispose SHA256 from heap before return.
    $sha256.Dispose()

    return $authorization 
}

# Function to create and invoke an API POST request to the Log Analytics Data Connector API
Function Post-LogAnalyticsData {
    param (
        [string]$customerId,
        [string]$sharedKey,
        [string]$body,
        [string]$logType
    )

    $method = "POST"
    $contentType = "application/json"
    $resource = "/api/logs"
    $rfc1123date = [DateTime]::UtcNow.ToString("r")
    $contentLength = $body.Length
    $signature = Build-Signature `
        -customerId $customerId `
        -sharedKey $sharedKey `
        -date $rfc1123date `
        -contentLength $contentLength `
        -method $method `
        -contentType $contentType `
        -resource $resource
    
    $uri = $logAnalyticsUri + $resource + "?api-version=2016-04-01"

    $headers = @{
        "Authorization"        = $signature;
        "Log-Type"             = $logType;
        "x-ms-date"            = $rfc1123date;
        "time-generated-field" = "TimeGenerated";
    }
    Write-Host "Headers: $($headers.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" })"

    try {
        $response = Invoke-WebRequest -Uri $uri -Method $method -ContentType $contentType -Headers $headers -Body $body -UseBasicParsing
    }
    catch {
        Write-Error "Error during sending logs to Azure Sentinel: $_"
        throw $_  # Re-throwing to capture in the outer try-catch
    }
    if ($response.StatusCode -eq 200) {
        Write-Host "Logs have been successfully sent to Azure Sentinel."
    }
    else {
        Write-Host "Error during sending logs to Azure Sentinel. Response code : $response.StatusCode"
    }

    return $response.StatusCode
}

# Timer-triggered function execution
try {
    # Get new data from ADX
    $token = GetAdxToken
    Write-Host "Token in main: $token"
    $results = QueryAdx -token $token
    Write-Output "Query Results:"
    Write-Output $results

    # Convert results to JSON
    $jsonBody = $results | ConvertTo-Json -Depth 10 -Compress
    write-host "JSON Body: $jsonBody"
    # Send the results to Sentinel
    $logName = "TestTable1"
    write-host "Sentinel_Workspace_ID: $SENTINEL_WORKSPACE_ID"
    write-host "Sentinel_Shared_Key: $SENTINEL_SHARED_KEY"

    $statusCode = Post-LogAnalyticsData -customerId $SENTINEL_WORKSPACE_ID -sharedKey $SENTINEL_SHARED_KEY -body $jsonBody -logType $logName
    Write-Host "Post-LogAnalyticsData returned status code: $statusCode"
}
catch {
    Write-Error "Error during function execution: $_"
}
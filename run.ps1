param($Timer)

# Environment variables
$ADX_CLUSTER = $env:ADX_CLUSTER
$ADX_DATABASE = $env:ADX_DATABASE
$TABLE_NAME = $env:TABLE_NAME
$SENTINEL_WORKSPACE_ID = $env:SENTINEL_WORKSPACE_ID
$SENTINEL_SHARED_KEY = $env:SENTINEL_SHARED_KEY
$STORAGE_CONNECTION_STRING = $env:AzureWebJobsStorage
$STATE_TABLE_NAME = "adxStateTable"
$STATE_PARTITION_KEY = "adxState"
$STATE_ROW_KEY = "lastProcessedRow"


function ConvertTo-QueryString {
    param ([hashtable]$hash)
    return ($hash.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join "&"
}

# Function to get ADX token using Managed Identity
function Get-AdxToken {
    $tokenEndpoint = "http://169.254.169.254/metadata/identity/oauth2/token"
    $params = @{
        "api-version" = "2018-02-01"
        "resource"    = "https://kusto.kusto.windows.net/"
    }
    $headers = @{
        "Metadata" = "true"
    }
    try {
        $response = Invoke-RestMethod -Method Get -Uri "$tokenEndpoint?$(ConvertTo-QueryString $params)" -Headers $headers
        return $response.access_token
    }
    catch {
        Write-Error "Error getting ADX token: $_"
        throw
    }
}

# Function to get the last processed row
function Get-LastProcessedRow {
    $tableClient = [Microsoft.Azure.Cosmos.Table.CloudStorageAccount]::Parse($STORAGE_CONNECTION_STRING).CreateCloudTableClient()
    $table = $tableClient.GetTableReference($STATE_TABLE_NAME)
    $retrieveOperation = [Microsoft.Azure.Cosmos.Table.TableOperation]::Retrieve($STATE_PARTITION_KEY, $STATE_ROW_KEY)
    try {
        $result = $table.Execute($retrieveOperation)
        if ($result.Result) {
            return $result.Result.Properties['LastProcessedRow'].StringValue
        }
        else {
            Write-Warning "No previous state found in $STATE_TABLE_NAME."
            return $null
        }
    }
    catch {
        Write-Error "Error getting last processed row: $_"
        throw
    }
}

# Function to update the last processed row
function Update-LastProcessedRow($rowKey) {
    $tableClient = [Microsoft.Azure.Cosmos.Table.CloudStorageAccount]::Parse($STORAGE_CONNECTION_STRING).CreateCloudTableClient()
    $table = $tableClient.GetTableReference($STATE_TABLE_NAME)
    $entity = New-Object Microsoft.Azure.Cosmos.Table.DynamicTableEntity($STATE_PARTITION_KEY, $STATE_ROW_KEY)
    $entity.Properties['LastProcessedRow'] = New-Object Microsoft.Azure.Cosmos.Table.EntityProperty $rowKey
    $upsertOperation = [Microsoft.Azure.Cosmos.Table.TableOperation]::InsertOrReplace($entity)
    try {
        $table.Execute($upsertOperation)
    }
    catch {
        Write-Error "Error updating last processed row: $_"
        throw
    }
}

# Function to query ADX
function Query-Adx($token, $lastProcessedRow) {
    $url = "$ADX_CLUSTER/v1/rest/query"
    $headers = @{
        "Authorization" = "Bearer $token"
        "Content-Type"  = "application/json"
    }
    $csl = if ($lastProcessedRow) {
        "$TABLE_NAME | where Timestamp > datetime($lastProcessedRow)"
    }
    else {
        "$TABLE_NAME"
    }
    $body = @{
        db  = $ADX_DATABASE
        csl = $csl
    } | ConvertTo-Json
    try {
        $response = Invoke-RestMethod -Uri $url -Headers $headers -Method Post -Body $body
        return $response
    }
    catch {
        Write-Error "Error querying ADX: $_"
        throw
    }
}

# Function to send data to Sentinel
function Send-ToSentinel($data) {
    $url = "https://$SENTINEL_WORKSPACE_ID.ods.opinsights.azure.com/api/logs?api-version=2016-04-01&logType=$TABLE_NAME"
    $headers = @{
        "Authorization" = "SharedKey ${SENTINEL_WORKSPACE_ID}:${SENTINEL_SHARED_KEY}"
        "Content-Type"  = "application/json"
    }
    foreach ($record in $data) {
        $body = @{
            time = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
            data = $record
        } | ConvertTo-Json
        try {
            Invoke-RestMethod -Uri $url -Headers $headers -Method Post -Body $body
        }
        catch {
            Write-Error "Error sending data to Sentinel: $_"
            throw
        }
    }
}

# Main function
try {
    Write-Host "PowerShell timer trigger function started."

    Write-Host "Getting ADX token..."
    $token = Get-AdxToken
    Write-Host "Got ADX token."

    Write-Host "Getting last processed row..."
    $lastProcessedRow = Get-LastProcessedRow
    Write-Host "Got last processed row: $lastProcessedRow"

    Write-Host "Querying ADX..."
    $adxData = Query-Adx -token $token -lastProcessedRow $lastProcessedRow
    Write-Host "Queried ADX."

    if ($adxData.Tables[0].Rows.Count -gt 0) {
        Write-Host "Sending data to Sentinel..."
        Send-ToSentinel -data $adxData.Tables[0].Rows
        Write-Host "Sent data to Sentinel."

        $newLastProcessedRow = $adxData.Tables[0].Rows[-1][0]  # Assuming the first column is the timestamp
        Write-Host "Updating last processed row..."
        Update-LastProcessedRow -rowKey $newLastProcessedRow
        Write-Host "Updated last processed row."
    }
    else {
        Write-Host "No new data to process."
    }

}
catch {
    Write-Error "Error in main function: $_"
}

Write-Host "PowerShell timer trigger function ended."
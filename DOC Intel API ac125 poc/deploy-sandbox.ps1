param(
    # Azure subscription that will own the sandbox deployment.
    [Parameter(Mandatory = $false)]
    [string]$SubscriptionId = "<your-subscription-id>",

    # Short environment label used in generated resource names.
    [Parameter(Mandatory = $false)]
    [string]$Environment = "sbx",

    # Azure region for the resource group, App Service plan, and web app.
    [Parameter(Mandatory = $false)]
    [string]$Location = "eastus",

    # Base workload name used to generate Azure resource names.
    [Parameter(Mandatory = $false)]
    [string]$WorkloadName = "sample-api",

    # Container image repository name inside Azure Container Registry.
    [Parameter(Mandatory = $false)]
    [string]$ImageName = "sample-api",

    # Optional image tag. If omitted, the script generates a timestamp tag.
    [Parameter(Mandatory = $false)]
    [string]$ImageTag,

    # Existing Azure AI resources to read connection details from.
    [Parameter(Mandatory = $false)]
    [string]$DocIntelResourceGroup = "<doc-intel-resource-group>",

    [Parameter(Mandatory = $false)]
    [string]$DocIntelAccountName = "<doc-intel-account-name>",

    [Parameter(Mandatory = $false)]
    [string]$OpenAIResourceGroup = "<openai-resource-group>",

    [Parameter(Mandatory = $false)]
    [string]$OpenAIAccountName = "<openai-account-name>",

    [Parameter(Mandatory = $false)]
    [string]$OpenAIDeployment = "gpt-4.1",

    [Parameter(Mandatory = $false)]
    [string]$OpenAIApiVersion = "2024-10-21"
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $false

function Require-Command {
    param([string]$Name)

    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command not found: $Name"
    }
}

function Read-DotEnv {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        throw ".env file not found at $Path"
    }

    $values = @{}
    foreach ($line in Get-Content $Path) {
        if ([string]::IsNullOrWhiteSpace($line)) {
            continue
        }

        $trimmed = $line.Trim()
        if ($trimmed.StartsWith("#")) {
            continue
        }

        $parts = $trimmed -split "=", 2
        if ($parts.Count -ne 2) {
            continue
        }

        $values[$parts[0].Trim()] = $parts[1].Trim()
    }

    return $values
}

function Ensure-AzureLogin {
    az account show | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Azure CLI is not logged in. Run 'az login' first."
    }
}

function Assert-PlaceholderReplaced {
    param(
        [string]$Value,
        [string]$Name
    )

    if ([string]::IsNullOrWhiteSpace($Value) -or $Value.StartsWith("<")) {
        throw "Set $Name before running this example script."
    }
}

function Ensure-ResourceGroup {
    param(
        [string]$ResourceGroupName,
        [string]$ResourceLocation
    )

    az group create --name $ResourceGroupName --location $ResourceLocation --output none
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create or verify resource group $ResourceGroupName"
    }
}

function Ensure-Acr {
    param(
        [string]$ResourceGroupName,
        [string]$RegistryName
    )

    $existingAcr = az acr list --resource-group $ResourceGroupName --query "[?name=='$RegistryName'].name | [0]" -o tsv
    if ([string]::IsNullOrWhiteSpace($existingAcr)) {
        az acr create --resource-group $ResourceGroupName --name $RegistryName --sku Basic --admin-enabled true --output none
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to create ACR $RegistryName"
        }
    }

    az acr update --resource-group $ResourceGroupName --name $RegistryName --admin-enabled true --output none
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to enable ACR admin credentials on $RegistryName"
    }
}

function Ensure-AppServicePlan {
    param(
        [string]$ResourceGroupName,
        [string]$PlanName
    )

    $existingPlan = az appservice plan list --resource-group $ResourceGroupName --query "[?name=='$PlanName'].name | [0]" -o tsv
    if ([string]::IsNullOrWhiteSpace($existingPlan)) {
        az appservice plan create --resource-group $ResourceGroupName --name $PlanName --is-linux --sku B1 --output none
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to create App Service plan $PlanName"
        }
    }
}

function Ensure-WebApp {
    param(
        [string]$ResourceGroupName,
        [string]$PlanName,
        [string]$WebAppName,
        [string]$ImageReference
    )

    $existingWebApp = az webapp list --resource-group $ResourceGroupName --query "[?name=='$WebAppName'].name | [0]" -o tsv
    if ([string]::IsNullOrWhiteSpace($existingWebApp)) {
        az webapp create --resource-group $ResourceGroupName --plan $PlanName --name $WebAppName --deployment-container-image-name $ImageReference --output none
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to create Web App $WebAppName"
        }
    }
}

Require-Command az

if (-not $ImageTag) {
    $ImageTag = "v$(Get-Date -Format yyyyMMddHHmmss)"
}

# Basic guardrails so the example is not run unchanged by accident.
Assert-PlaceholderReplaced -Value $SubscriptionId -Name "SubscriptionId"
Assert-PlaceholderReplaced -Value $DocIntelResourceGroup -Name "DocIntelResourceGroup"
Assert-PlaceholderReplaced -Value $DocIntelAccountName -Name "DocIntelAccountName"
Assert-PlaceholderReplaced -Value $OpenAIResourceGroup -Name "OpenAIResourceGroup"
Assert-PlaceholderReplaced -Value $OpenAIAccountName -Name "OpenAIAccountName"

# Read app-level runtime settings from the local .env file.
$dotenvPath = Join-Path $PSScriptRoot ".env"
$envFile = Read-DotEnv -Path $dotenvPath

$apiSharedSecret = $envFile["API_SHARED_SECRET"]
if ([string]::IsNullOrWhiteSpace($apiSharedSecret)) {
    throw "API_SHARED_SECRET must be set in .env before deployment."
}

# Step 1: make sure Azure CLI is authenticated and pointed to the right subscription.
Ensure-AzureLogin
Write-Host "Selecting subscription $SubscriptionId..."
az account set --subscription $SubscriptionId
if ($LASTEXITCODE -ne 0) {
    throw "Failed to select subscription $SubscriptionId"
}

# Step 2: build consistent, editable Azure resource names from simple inputs.
$subscriptionSuffix = ($SubscriptionId -replace "-", "").Substring(0, 6).ToLower()
$safeWorkload = ($WorkloadName.ToLower() -replace "[^a-z0-9-]", "")
$safeImageName = ($ImageName.ToLower() -replace "[^a-z0-9]", "")

if ([string]::IsNullOrWhiteSpace($safeImageName)) {
    $safeImageName = "sampleapi"
}

$resourceGroup = "rg-$safeWorkload-$Environment"
$planName = "$safeWorkload-$Environment-plan"
$webAppName = "$safeWorkload-$Environment-$subscriptionSuffix"
if ($webAppName.Length -gt 60) {
    $webAppName = $webAppName.Substring(0, 60)
}

$acrName = ($safeImageName + $Environment + $subscriptionSuffix)
if ($acrName.Length -lt 5) {
    $acrName = ($acrName + "build").Substring(0, 5)
}
if ($acrName.Length -gt 50) {
    $acrName = $acrName.Substring(0, 50)
}

# Step 3: create or reuse the resource group that will hold the sandbox app resources.
Write-Host "Using resource group $resourceGroup"
Ensure-ResourceGroup -ResourceGroupName $resourceGroup -ResourceLocation $Location

# Step 4: read the external AI service endpoints and keys from existing Azure resources.
Write-Host "Fetching AI endpoints and keys from existing Azure resources..."
$docIntelEndpoint = az cognitiveservices account show --resource-group $DocIntelResourceGroup --name $DocIntelAccountName --query properties.endpoint -o tsv
$docIntelKey = az cognitiveservices account keys list --resource-group $DocIntelResourceGroup --name $DocIntelAccountName --query key1 -o tsv
$openAIEndpoint = az cognitiveservices account show --resource-group $OpenAIResourceGroup --name $OpenAIAccountName --query properties.endpoint -o tsv
$openAIKey = az cognitiveservices account keys list --resource-group $OpenAIResourceGroup --name $OpenAIAccountName --query key1 -o tsv

if ([string]::IsNullOrWhiteSpace($docIntelEndpoint) -or [string]::IsNullOrWhiteSpace($docIntelKey)) {
    throw "Failed to retrieve Document Intelligence endpoint or key."
}

if ([string]::IsNullOrWhiteSpace($openAIEndpoint) -or [string]::IsNullOrWhiteSpace($openAIKey)) {
    throw "Failed to retrieve Azure OpenAI endpoint or key."
}

# Step 5: create or reuse Azure Container Registry and capture the admin credentials.
Write-Host "Ensuring container registry $acrName..."
Ensure-Acr -ResourceGroupName $resourceGroup -RegistryName $acrName

$acrLoginServer = az acr show --resource-group $resourceGroup --name $acrName --query loginServer -o tsv
$acrUser = az acr credential show --resource-group $resourceGroup --name $acrName --query username -o tsv
$acrPass = az acr credential show --resource-group $resourceGroup --name $acrName --query "passwords[0].value" -o tsv

if ([string]::IsNullOrWhiteSpace($acrLoginServer) -or [string]::IsNullOrWhiteSpace($acrUser) -or [string]::IsNullOrWhiteSpace($acrPass)) {
    throw "Failed to retrieve ACR login details."
}

$imageRef = "${acrLoginServer}/${ImageName}:$ImageTag"

# Step 6: build the container image in ACR so Docker is not required locally.
Write-Host "Building image $imageRef in Azure Container Registry..."
Push-Location $PSScriptRoot
try {
    az acr build --registry $acrName --image "${ImageName}:$ImageTag" .
    if ($LASTEXITCODE -ne 0) {
        throw "ACR build failed."
    }
}
finally {
    Pop-Location
}

# Step 7: create or reuse the Linux App Service plan and web app.
Write-Host "Ensuring App Service plan and web app..."
Ensure-AppServicePlan -ResourceGroupName $resourceGroup -PlanName $planName
Ensure-WebApp -ResourceGroupName $resourceGroup -PlanName $planName -WebAppName $webAppName -ImageReference $imageRef

# Step 8: point the web app at the new container image.
Write-Host "Configuring container pull from ACR admin credentials..."
az webapp config container set `
    --resource-group $resourceGroup `
    --name $webAppName `
    --container-image-name $imageRef `
    --container-registry-url "https://${acrLoginServer}" `
    --container-registry-user $acrUser `
    --container-registry-password $acrPass `
    --output none

if ($LASTEXITCODE -ne 0) {
    throw "Failed to configure Web App container settings."
}

# Step 9: inject runtime settings into the web app.
Write-Host "Applying application settings..."
az webapp config appsettings set `
    --resource-group $resourceGroup `
    --name $webAppName `
    --settings `
        "AZURE_DOC_INTEL_ENDPOINT=$docIntelEndpoint" `
        "AZURE_DOC_INTEL_KEY=$docIntelKey" `
        "AZURE_OPENAI_ENDPOINT=$openAIEndpoint" `
        "AZURE_OPENAI_KEY=$openAIKey" `
        "AZURE_OPENAI_DEPLOYMENT=$OpenAIDeployment" `
        "AZURE_OPENAI_API_VERSION=$OpenAIApiVersion" `
        "API_SHARED_SECRET=$apiSharedSecret" `
        "MAX_UPLOAD_SIZE_MB=25" `
        "LOG_LEVEL=INFO" `
        "WEBSITES_PORT=8000" `
        "WEBSITES_ENABLE_APP_SERVICE_STORAGE=false" `
        "SCM_DO_BUILD_DURING_DEPLOYMENT=false" `
    --output none

if ($LASTEXITCODE -ne 0) {
    throw "Failed to set Web App application settings."
}

# Step 10: restart the site and write a small deployment summary file.
Write-Host "Restarting web app..."
az webapp restart --resource-group $resourceGroup --name $webAppName --output none
if ($LASTEXITCODE -ne 0) {
    throw "Failed to restart Web App $webAppName"
}

$url = "https://$webAppName.azurewebsites.net"
$state = [ordered]@{
    subscriptionId = $SubscriptionId
    resourceGroup = $resourceGroup
    planName = $planName
    webAppName = $webAppName
    acrName = $acrName
    imageRef = $imageRef
    url = $url
    extractUrl = "$url/api/extract"
    healthUrl = "$url/health"
}

$statePath = Join-Path $PSScriptRoot "deploy-state.example.json"
$state | ConvertTo-Json | Set-Content -Path $statePath

Write-Host "Example deployment complete."
Write-Host "Application URL: $url"
Write-Host "Extract endpoint: $url/api/extract"
Write-Host "Health endpoint: $url/health"
Write-Host "Image: $imageRef"
Write-Host "Deployment state saved to $statePath"
#!/bin/bash
# Azure Infrastructure Setup Script
# Usage: ./setup_azure.sh
# Make sure you are logged in using `az login` and have an active subscription selected.

set -e # Exit immediately if a command exits with a non-zero status

# --- Configurations ---
RESOURCE_GROUP="rg-vn30-pipeline"
LOCATION="southeastasia"
# Storage account names must be globally unique, 3-24 chars, lowercase and numbers only
RANDOM_STRING=$(head /dev/urandom | tr -dc a-z0-9 | head -c 6)
STORAGE_ACCOUNT="savn30pipeline$RANDOM_STRING"
# SQL Server names must be globally unique
SQL_SERVER="sql-vn30-pipeline-$RANDOM_STRING"
SQL_DB="sqldb-vn30"
SQL_ADMIN="vn30admin"

echo "Please enter a strong password for SQL Admin (min 8 chars, uppercase, lowercase, numbers, specials):"
read -s SQL_PASSWORD

echo "------------------------------------------------"
echo "Starting Azure Provisioning in $LOCATION..."
echo "Resource Group: $RESOURCE_GROUP"
echo "Storage Account: $STORAGE_ACCOUNT"
echo "SQL Server: $SQL_SERVER"
echo "------------------------------------------------"

# 1. Create Resource Group
echo "[1/4] Creating Resource Group: $RESOURCE_GROUP..."
az group create --name $RESOURCE_GROUP --location $LOCATION -o none

# 2. Create Storage Account (Data Lake Gen2)
echo "[2/4] Creating Storage Account: $STORAGE_ACCOUNT..."
az storage account create \
    --name $STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION \
    --sku Standard_LRS \
    --enable-hierarchical-namespace true \
    -o none

# Retrieve Storage Account Key
echo "Retrieving Storage Connection String..."
STORAGE_CONN_STR=$(az storage account show-connection-string -g $RESOURCE_GROUP -n $STORAGE_ACCOUNT -o tsv)

# Create Containers
echo "Creating containers: 'raw' and 'processed'..."
az storage container create --name raw --connection-string "$STORAGE_CONN_STR" -o none
az storage container create --name processed --connection-string "$STORAGE_CONN_STR" -o none

# 3. Create Azure SQL Server
echo "[3/4] Creating Azure SQL Server: $SQL_SERVER..."
az sql server create \
    --name $SQL_SERVER \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION \
    --admin-user $SQL_ADMIN \
    --admin-password "$SQL_PASSWORD" \
    -o none

# Create Firewall Rule (Allow local IP)
echo "Adding Firewall Rule to allow current IP..."
CURRENT_IP=$(curl -s ifconfig.me)
az sql server firewall-rule create \
    --resource-group $RESOURCE_GROUP \
    --server $SQL_SERVER \
    --name AllowLocalIP \
    --start-ip-address $CURRENT_IP \
    --end-ip-address $CURRENT_IP \
    -o none

# Allow Azure services to access server
az sql server firewall-rule create \
    --resource-group $RESOURCE_GROUP \
    --server $SQL_SERVER \
    --name AllowAzureServices \
    --start-ip-address 0.0.0.0 \
    --end-ip-address 0.0.0.0 \
    -o none

# 4. Create Azure SQL Database (Serverless Gen5_1 for cost saving)
echo "[4/4] Creating Azure SQL Database: $SQL_DB (Serverless)..."
az sql db create \
    --resource-group $RESOURCE_GROUP \
    --server $SQL_SERVER \
    --name $SQL_DB \
    --edition GeneralPurpose \
    --compute-model Serverless \
    --family Gen5 \
    --capacity 1 \
    --auto-pause-delay 60 \
    -o none

echo "------------------------------------------------"
echo "Provisioning Completed Successfully!"
echo "------------------------------------------------"
echo "Wait a moment... Retrieving credentials for your .env file..."
echo ""
echo "Please COPY the following values into your .env file:"
echo ""
echo "AZURE_STORAGE_CONNECTION_STRING=\"$STORAGE_CONN_STR\""
echo "AZURE_SQL_CONNECTION_STRING=\"Driver={ODBC Driver 18 for SQL Server};Server=tcp:$SQL_SERVER.database.windows.net,1433;Database=$SQL_DB;Uid=$SQL_ADMIN;Pwd=$SQL_PASSWORD;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;\""
echo ""
echo "SAVE these credentials securely!"

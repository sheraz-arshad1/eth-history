--Command to reset database
--mysql < Schema.sql
DROP DATABASE IF EXISTS Transfers;

CREATE DATABASE Transfers;

USE Transfers;

CREATE TABLE Transfer (
    txHash VARCHAR(100),
    fromAddress VARCHAR(100),
    toAddress VARCHAR(100),
    assetName VARCHAR(100),
    value VARCHAR(100),
    contractAddress VARCHAR(100),
    assetDecimal VARCHAR(10),
    blockNum VARCHAR(40),
    timestamp VARCHAR(40),
    gasCostInWei VARCHAR(100)
);

CREATE TABLE UserSyncedBlock (
    address VARCHAR(100) PRIMARY KEY,
    blockNum VARCHAR(40)
)
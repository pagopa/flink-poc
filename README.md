# Apache Flink Data Enrichment with Event Hub, PostgreSQL, Mongo, and Cosmos

## Table of Contents
- [Introduction](#introduction)
- [Project Overview](#project-overview)
- [Setup](#setup)
- [How It Works](#how-it-works)
- [Running the project](#running-the-project)

## Introduction

This repository is a proof of concept (PoC) project demonstrating data enrichment using Apache Flink. The goal of this project is to enrich data received from Azure Event Hub with information from various databases such as PostgreSQL, MongoDB, and Cosmos DB. By leveraging Apache Flink, we can seamlessly combine data from different sources and enrich the incoming messages.

## Project Overview

In this PoC, we create a data pipeline that listens for messages from an Azure Event Hub topic using the native Kafka source provided by Apache Flink. Upon receiving a message, we extract relevant data and perform a join operation with data from one of the three databases based on a common field. For example, if a message like this arrives on the Event Hub:

```json
{
    "id": "4444",
    "firstName": "Name",
    "lastName": "Surname",
    "dateOfBirth": "2023-09-13T09:46:42.927Z",
    "statusId": 1
}

We can execute a SQL query like "SELECT * FROM status" on one of the databases, obtaining data like this:

```json
{
    "id": 1,
    "value": "Active"
}

Subsequently, we perform a join operation using the "statusId" field to enrich the information, resulting in the following:

```json
{
    "id": "4444",
    "firstName": "Name",
    "lastName": "Surname",
    "dateOfBirth": "2023-09-13T09:46:42.927Z",
    "statusId": 1,
    "value": "Active"
}

## Setup

To set up this project, follow these steps:

- Clone this repository to your local machine.
- Install JDK 11 and set your JAVA_HOME variable.
- Set up your Azure Event Hub and obtain the necessary connection details (e.g., connection string, topic name).
- Create or use existing databases for PostgreSQL, MongoDB, and Cosmos DB, and populate them with relevant data.
- Copy the "src/main/resources/env.config.example" file and paste renaming it as "env.config" then populate it with the info needed.

## How It Works

The project uses the Apache Flink Kafka source to listen for messages from the Azure Event Hub.
When a message arrives, it is deserialized and relevant data is extracted.

A SQL query is executed on one of the three databases to fetch additional information based on the common field.

The Flink job performs a join operation to enrich the incoming message with the additional data.

The enriched data can be further processed or saved as needed.

## Running the Project

You can run the project in one of the following two modes:

### Mode 1: Running from Flink Cluster

1. First, create the JAR package using Maven:

```bash
mvn clean package

2. Then, launch the project from the Flink cluster in detached mode:

```bash
./bin/flink run --detached ./target/flink-poc-1.0-SNAPSHOT.jar

### Mode 2: Running with Maven

Execute the following Maven command to run the project directly:

```bash
mvn exec:java -Dexec.mainClass="com.pagopa.Main"

# cassandra-review-ingestion

Ingests and queries user review data from Amazon using AstraDB and Cassandra. Includes basic CQL modeling and data parsing with Java and JSON.

# Cassandra Review Ingestion – Big Data Systems Assignment

This code ingests a subset of Amazon review data into an AstraDB Cassandra database and implements queryable access through a provided command-line interface. The task involves writing a single class (`HW2StudentAnswer.java`) that connects to the database, processes the dataset, and supports the given CQL query patterns.

This assignment was completed as part of the Big Data Systems course at Tel Aviv University.

## Overview

- Connects to AstraDB using secure credentials and the Java driver.
- Loads product and review data from JSON files.
- Inserts records into appropriate Cassandra tables based on query requirements.
- Supports query execution through a CLI (`HW2CLI`) provided as part of the framework.
- Data ingestion is rate-limited and optimized using multi-threading and batching.
- JSON parsing is handled via the org.json package.

## Folder Structure

- `students.txt` – Contains student names and IDs.
- `astradb/`
  - `login-token.json` – AstraDB application token (admin role).
  - `secure-connect-hw2.zip` – Secure connect bundle for Cassandra driver.
- `src/` – Java source code including the student solution.
- `runme.jar` – Precompiled JAR that runs the CLI.
- `data/` – JSON dataset files containing Amazon product and review data (not included in repo).

## Requirements Implemented

- Ingest Amazon reviews and product metadata into AstraDB.
- Use correct partition and clustering keys to optimize CQL queries.
- Avoid inserting nulls by using `NOT_AVAILABLE_VALUE`.
- Apply proper data modeling to match the required query types.
- Avoid overloading the AstraDB free-tier instance by using thread throttling.
- Ensure `runme.jar` is self-contained and executable with provided CLI.

## Compilation and Packaging

Use Eclipse to export `runme.jar` including all dependent libraries (check "Extract required libraries into generated JAR").

Test the executable:
java -jar runme.jar /path/to/astradb /path/to/data


## Notes

- Only the file `HW2StudentAnswer.java` was modified.
- All other files under `bigdatacourse.hw2` were left unchanged as instructed.
- Application supports multiple queries as required through the CLI.
- Token-based filtering can be used to verify data ingestion counts if needed.

This implementation demonstrates basic use of Cassandra, data modeling, CQL programming, and JSON ingestion in a distributed database environment.

# API CLI for Airflow

<!--toc:start-->

- [API CLI for Airflow](#api-cli-for-airflow)

<!--toc:end-->

## Overview

This is a containerized Rust application designed to leverage the speed of Rust, the portability of Docker, and the orchestration capabilities of Apache Airflow. The CLI tool is built to perform efficient data extraction from REST APIs and can be easily deployed as Docker containers within Airflow DAGs for complex task scheduling.

## Why This Architecture?

This project takes advantage of three key technologies:

1. **Rust**: For maximum performance and memory safety when processing large amounts of data
2. **Docker**: For consistent, portable deployments across different environments
3. **Airflow**: For sophisticated workflow orchestration and scheduling capabilities

By combining these technologies, we can create highly efficient, reliable, and scalable data pipelines.

## Features

- _Extraction_
  - [x] REST API
    - [x] Query Params
    - [ ] Custom Headers
    - [ ] Basic Auth
    - [ ] Bearer Tokens
    - [x] Timeout retry
- _Transform_
  - [ ] TBD
- _Load_
  - [ ] DAL
  - [x] Write to local file system
  - [ ] Write to Object Store
    - [ ] S3
    - [ ] Minio
  - [ ] Write to Postgres

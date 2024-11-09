# Kleo Backend kinesis Stream Processor

A Python-based Kinesis Consumer Library (KCL) implementation for processing streaming data with user owned data. 

## Todo : Next Action Items
- [x] Setup Mongo Triggers to push data to this stream  
- [x] Test the following in production
- [ ] Create a measurement metrics for number of shards x size of instance x number of processes to run vs number of items / amount of time taken.
- [ ] How to measure time required and load on servers in a dynamic fashion
- [ ] How to scale better 

## Overview

This application processes records from Amazon Kinesis streams, tracking user activity data and storing it in a database. It includes features for:
- Calculating and tracking data quantities per user
- Activity classification and point system
- Periodic uploads of user data to Arweave Decentralised Storage 
- Automatic checkpointing and error handling

## Prerequisites

- Python 3.x
- Amazon KCL Python library (`amazon_kclpy`)
- Access to a Kinesis stream
- Database setup (refer to `tasks.db`)

## Key Components

- `RecordProcessor`: Main class that handles stream processing
- `tasks.classify`: Activity classification module
- `tasks.upload`: Arweave upload functionality
- `tasks.pii`: PII handling utilities
- `tasks.db`: Database interactions

## Features

- Processes records in real-time from Kinesis streams
- Tracks user activity and assigns points
- Implements automatic checkpointing every 60 seconds
- Handles stream processing errors with retries
- Uploads data to Arweave blockchain after every 50 history items
- Maintains user activity statistics and Kleo points

## Usage

Run the processor:

```bash
python processor.py
```

## Error Handling

The processor implements robust error handling for:
- Checkpoint failures (with retries)
- Throttling exceptions
- Invalid state exceptions
- General processing errors

## Configuration

Key configurations:
- Checkpoint frequency: 60 seconds
- Checkpoint retries: 5
- Sleep between retries: 5 seconds

## Notes

- Ensure proper AWS credentials are configured
- Monitor checkpoint frequency for production workloads
- Review database connection settings before deployment

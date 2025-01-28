# GDPRlogger 

## Overview

The **GDPRlogger** is a key component of the [GDPRruler](https://github.com/dimstav23/GDPRuler) project, which is designed to enforce compliance with the General Data Protection Regulation (GDPR) for key-value stores. This logging system acts as a tamper-evident, high-performance logging solution that tracks all transactions involving personal data. It ensures data processing activities are logged securely and in a way that supports auditing and reporting, with a focus on scalability, high write throughput, and data integrity.

## Features

- **High Write Throughput:** Focuses on fast write operations with minimal read requirements.
- **Tamper-Evident:** Cryptographic hashing of logs to detect any tampering.
- **Security:** Logs can be encrypted and protected with robust access control.
- **Scalable Concurrency:** Designed to handle high concurrency through dedicated writer threads and a thread-safe queue.
- **Occasional Export:** Logs are stored in append-only segments that can be exported or archived when needed.
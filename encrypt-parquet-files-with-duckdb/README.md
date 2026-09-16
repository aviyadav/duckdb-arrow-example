# Encrypt Parquet Files with DuckDB & Python

This project demonstrates how to secure your data pipeline by generating fake PII (Personally Identifiable Information) data and exporting it to an **AES-256 encrypted Parquet file** using [DuckDB](https://duckdb.org/) and Python. 

It is based on the concepts outlined in the article *[How to Encrypt Parquet Files With DuckDB](https://dean-joseph-murphy.medium.com/how-to-encrypt-parquet-files-with-duckdb-7b368716045d)* by Dean J. Murphy.

Instead of relying on a live PostgreSQL database, this adapted version uses a local DuckDB file as the source database to make the pipeline immediately runnable, while maintaining the exact same encryption, keyring, and Parquet modular encryption logic.

## 🛠️ Prerequisites

- **Python 3.9+**
- **[uv](https://docs.astral.sh/uv/)**: A fast Python package and project manager written in Rust.

## 📁 Project Structure

```text
.
├── README.md
├── pyproject.toml          # Managed by uv
├── generate_data.py        # Generates 1,000 fake customer records (PII)
├── generate_key.py         # Generates a 256-bit AES key and stores it in the OS Keyring
├── export_encrypted.py     # Exports the data to an AES-256 encrypted Parquet file
├── read_encrypted.py       # Decrypts and reads the Parquet file using the Keyring key
└── test_unencrypted.py     # Verifies that standard tools cannot read the file without the key
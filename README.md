# Invoice Parsing System using Google Document AI and BigQuery

## Overview

This project aims to create a system that automates the extraction of information from invoices in PDF format using Google Document AI and updates a BigQuery table with the parsed data. Two Python scripts have been developed for this purpose: `batch_upload.py` and `streaming_upload.py`.

## Technologies Used

- **Google Document AI**: Used for document processing and extraction of structured data from invoices.
- **BigQuery**: A fully managed, serverless data warehouse by Google Cloud Platform, used for storing and querying structured data.
- **Google Cloud Storage**: Used as an intermediary for storing input and output documents during processing.
- **Python**: Programming language used for scripting the data processing workflows.
- **Python libraries**:
  - `google-cloud-documentai`: Python client library for interacting with Google Document AI API.
  - `google-cloud-bigquery`: Python client library for interacting with BigQuery.
  - `google-cloud-storage`: Python client library for interacting with Google Cloud Storage.

## Project Structure

- **`batch_upload.py`**: This script provides functionality for batch uploading multiple invoices for processing. It takes a directory of PDF files as input, processes them in batches, and updates the BigQuery table with the extracted data. It utilizes asynchronous batch processing capabilities of Google Document AI.

- **`streaming_upload.py`**: Similar to `batch_upload.py`, this script allows for parsing and uploading invoices one at a time. It provides a simpler interface for processing individual documents by specifying the document location directly.

## Workflow

1. **Document Processing**: In both scripts, Google Document AI is utilized for processing PDF invoices. The Document AI processor is configured with a predefined set of rules for extracting specific fields such as invoice ID, date, supplier information, line items, etc.

2. **Data Extraction**: Extracted data is transformed into a structured format suitable for insertion into a BigQuery table. The `extract_data()` function within the scripts handles this transformation.

3. **Data Upload to BigQuery**: The parsed data is then uploaded to a designated BigQuery table using the `load_to_bigquery()` function. This function inserts the data into the specified BigQuery dataset and table.

4. **Error Handling**: Error handling mechanisms are implemented to handle exceptions such as internal server errors, retries, or unexpected errors gracefully. Error messages are logged for debugging and monitoring purposes.

## Usage

- **Batch Upload (`batch_upload.py`)**:
  ```bash
  python batch_upload.py 
  ```

- **Streaming Upload (`streaming_upload.py`)**:
  ```bash
  python streaming_upload.py 
  ```

## Configuration

**Environment Variables:**
- `project_id`: Google Cloud project ID.
- `location`: Location where the Document AI processor is deployed.
- `processor_id`: ID of the Document AI processor.
- `credentials_path`: Path to the service account credentials JSON file.
- `dataset_id`: BigQuery dataset ID.
- `table_id`: BigQuery table ID.
- `gcs_input_prefix`: Google Cloud Storage prefix for input documents.
- `gcs_output_uri`: Google Cloud Storage URI for output documents.

## Conclusion

The Invoice Parsing System provides a robust and scalable solution for automating the extraction of structured data from invoices and storing it in a centralized data warehouse. It leverages the capabilities of Google Document AI and BigQuery, offering flexibility in batch processing or individual document parsing as per the user's requirements.
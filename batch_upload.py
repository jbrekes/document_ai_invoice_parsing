import json
import os
import re
from typing import Optional
from dotenv import load_dotenv
from google.api_core.exceptions import InternalServerError, RetryError # type: ignore
from google.api_core.operation import Operation # type: ignore
from google.cloud import bigquery, documentai_v1 as documentai, storage
from google.oauth2 import service_account # type: ignore


# Load environment variables from .env file
load_dotenv()

# Constants
PROJECT_ID = os.getenv("project_id")
LOCATION = os.getenv("location")
PROCESSOR_ID = os.getenv("processor_id")
CREDENTIALS_PATH = os.getenv("credentials_path")
DATASET_ID = os.getenv("dataset_id")
TABLE_ID = os.getenv("table_id")
GCS_INPUT_PREFIX = os.getenv("gcs_input_prefix")
GCS_OUTPUT_URI = os.getenv("gcs_output_uri")
MIME_TYPE = 'application/pdf'

# Get environment variables
project_id = os.getenv("project_id")
location = os.getenv("location")
processor_id = os.getenv("processor_id")
credentials_path = os.getenv("credentials_path")
dataset_id = os.getenv("dataset_id")
table_id = os.getenv("table_id")
gcs_input_prefix = os.getenv("gcs_input_prefix")
gcs_output_uri = os.getenv("gcs_output_uri")

mime_type = 'application/pdf'

# Explicitly provide service account credentials to the client library
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)

def batch_process_documents(
    project_id: str,
    location: str,
    processor_id: str,
    gcs_output_uri: str,
    gcs_input_uri: Optional[str] = None,
    input_mime_type: Optional[str] = None,
    gcs_input_prefix: Optional[str] = None,
    field_mask: Optional[str] = None,
    timeout: int = 600,

) -> Operation:
    """
    Constructs a request to process a document using the Document AI Asynchronous API.
    """
    # You must set the api_endpoint if you use a location other than 'us', e.g.:
    opts = {}
    if location == "eu":
        opts = {"api_endpoint": "eu-documentai.googleapis.com"}

    # Instantiates a client
    documentai_client = documentai.DocumentProcessorServiceClient(credentials=credentials, client_options=opts)

    # projects/{project_id}/locations/{location}/processors/{processor_id}
    resource_name = documentai_client.processor_path(project_id, location, processor_id)

    # Specify specific GCS URIs to process individual documents, or a GCS URI Prefix to process an entire directory
    if gcs_input_uri:
        gcs_document = documentai.GcsDocument(
            gcs_uri=gcs_input_uri, mime_type=input_mime_type
        )
        # Load GCS Input URI into a List of document files
        gcs_documents = documentai.GcsDocuments(documents=[gcs_document])
        input_config = documentai.BatchDocumentsInputConfig(gcs_documents=gcs_documents)
    else:
        gcs_prefix = documentai.GcsPrefix(gcs_uri_prefix=gcs_input_prefix)
        input_config = documentai.BatchDocumentsInputConfig(gcs_prefix=gcs_prefix)

    # Cloud Storage URI for the Output Directory
    gcs_output_config = documentai.DocumentOutputConfig.GcsOutputConfig(
        gcs_uri=gcs_output_uri, field_mask=field_mask
    )

    # Where to write results
    output_config = documentai.DocumentOutputConfig(gcs_output_config=gcs_output_config)

    # Configure Process Request
    request = documentai.BatchProcessRequest(
        name=resource_name,
        input_documents=input_config,
        document_output_config=output_config,
    )

    # Future for long-running operations returned from Google Cloud APIs.
    operation = documentai_client.batch_process_documents(request)

    try:
        print(f"Waiting for operation {operation.operation.name} to complete...")
        operation.result(timeout=timeout)
    except (RetryError, InternalServerError) as e:
        print(e.message)
    
    # Use operation metadata to check if batch process succeded
    metadata = documentai.BatchProcessMetadata(operation.metadata)

    if metadata.state != documentai.BatchProcessMetadata.State.SUCCEEDED:
        raise ValueError(f"Batch Process Failed: {metadata.state_message}")

    return operation

def extract_data(doc):
    """
    Transforms Document AI Response to desired format.
    """
    # Define response structure
    invoice_data = {
        'invoice_id': None,
        'invoice_date': None, 
        'due_date': None,
        'invoice_type': None, 
        'supplier_name': None, 
        'receiver_name': None,
        'receiver_address': None,
        'net_amount': None,
        'total_tax_amount': None,
        'freight_amount': None,
        'total_amount': None,
        'line_items_list': None
    }

    invoice_line_items = []

    try:
        for entity in doc.entities:
            if entity.type_  in invoice_data.keys():

                value = entity.normalized_value.text if entity.normalized_value else entity.mention_text
                invoice_data[entity.type_] = value
            
            if entity.type_  == 'line_item':

                line_item_data = {
                    'amount': None,
                    'description': None,
                    'product_code': None,
                    'purchase_order': None,
                    'quantity': None,
                    'unit': None,
                    'unit_price': None
                }

                for prop in entity.properties:
                    field_name = prop.type_.split("/")[-1]
                    value = prop.normalized_value.text if prop.normalized_value else prop.mention_text
                    line_item_data[field_name] = value

                invoice_line_items.append(line_item_data)
        
        invoice_data['line_items_list'] = invoice_line_items

    except Exception as e:
        print(f'Failed to transform data. Error: {e}')
    
    return invoice_data

def get_documents_from_gcs(
    gcs_output_uri: str, operation_name: str
) -> [documentai.Document]: # type: ignore
    """
    Get the document output from GCS.
    """

    # The GCS API requires the bucket name and URI prefix separately
    match = re.match(r"gs://([^/]+)/(.+)", gcs_output_uri)
    output_bucket = match.group(1)
    prefix = match.group(2)

    # The output files will be in a new subdirectory with the Operation ID as the name
    operation_id = re.search("operations\/(\d+)", operation_name, re.IGNORECASE).group(1)
    output_directory = f"{prefix}/{operation_id}"
    storage_client = storage.Client(credentials=credentials)

    # List of all of the files in the directory `gs://gcs_output_uri/operation_id`
    blob_list = list(storage_client.list_blobs(output_bucket, prefix=output_directory))

    output_documents = []

    for blob in blob_list:
        if ".json" in blob.name:
            document = documentai.types.Document.from_json(blob.download_as_bytes())
            output_documents.append(document)
        else:
            print(f"Skipping non-supported file type {blob.name}")

    return output_documents

def load_to_bigquery(invoice_list):
    """
    Inserts data to BigQuery Table
    """

    client = bigquery.Client(credentials=credentials, project=project_id, location=location)

    table_ref = client.dataset(dataset_id).table(table_id)

    rows_to_insert = invoice_list

    errors = client.insert_rows_json(table_ref, rows_to_insert)

    if errors == []:
        print("Data inserted successfully.")
    else:
        print("Errors occurred while inserting data:", errors)

def main():
    """
    Main function to orchestrate the document processing workflow.
    """
    try:
        operation = batch_process_documents(
            project_id=project_id,
            location=location,
            processor_id=processor_id,
            gcs_input_prefix=gcs_input_prefix,
            input_mime_type=mime_type,
            gcs_output_uri=gcs_output_uri,
        )

        operation_name = operation.operation.name

        # Continually polls the operation until it is complete.
        print(f"Waiting for operation {operation_name} to complete...")
        result = operation.result(timeout=300)

        print("Document processing complete.")

        # Get the Document Objects from the Output Bucket
        document_list = get_documents_from_gcs(
            gcs_output_uri=gcs_output_uri, operation_name=operation_name
        )

        process_docs_data = []

        for document in document_list:
            data = extract_data(document)
            process_docs_data.append(data)
        
        # Insert invoice data into the BigQuery table
        load_to_bigquery(process_docs_data)

    except (RetryError, InternalServerError) as e:
        print(f"An error occurred: {str(e)}")
        
    except Exception as e:
        print(f"Unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    main()
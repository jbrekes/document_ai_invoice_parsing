import os
from dotenv import load_dotenv
from google.cloud import documentai_v1 as documentai
from google.cloud import bigquery
from google.oauth2 import service_account

# Load environment variables from .env file
load_dotenv()

# Get environment variables
project_id = os.getenv("project_id")
location = os.getenv("location")
processor_id = os.getenv("processor_id")
credentials_path = os.getenv("credentials_path")
dataset_id = os.getenv("dataset_id")
table_id = os.getenv("table_id")
file_path = os.getenv("file_path")

file_name = 'google_invoice.pdf'
file_path = os.getenv("file_path") + file_name

mime_type = 'application/pdf'

# Explicitly provide service account credentials to the client library
credentials = service_account.Credentials.from_service_account_file(credentials_path)

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

def process_document(project_id: str, location: str,
                     processor_id: str, file_path: str,
                     mime_type: str) -> documentai.Document:
    """
    Processes a document using the Document AI API.
    """

    # Instantiates a client
    documentai_client = documentai.DocumentProcessorServiceClient(credentials=credentials)

    resource_name = documentai_client.processor_path(project_id, location, processor_id)

    # Read the file into memory
    with open(file_path, "rb") as image:
        image_content = image.read()

        # Load Binary Data into Document AI RawDocument Object
        raw_document = documentai.RawDocument(
            content=image_content, mime_type=mime_type)

        # Configure the process request
        request = documentai.ProcessRequest(
            name=resource_name, raw_document=raw_document)

        # Use the Document AI client to process the sample form
        result = documentai_client.process_document(request=request)

        return result.document

def extract_data(doc):

    """
    Transforms Document AI Response to desired format.
    """

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

        print('Data transformed successfully')
    except Exception as e:
        print(f'Failed to transform data. Error: {e}')
    
    return invoice_data


def load_document(path, mimetype):
    project_id = os.getenv("project_id")
    location = os.getenv("location")
    processor_id = os.getenv("processor_id")

    file_path = path
    mime_type = mimetype
    # Refer to https://cloud.google.com/document-ai/docs/processors-list for the supported file types

    document = process_document(project_id=project_id, location=location,
                                processor_id=processor_id, file_path=file_path,
                                mime_type=mime_type)

    return document

def load_to_bigquery(data):
    """
    Inserts data to BigQuery Table
    """

    client = bigquery.Client(credentials=credentials, project=project_id, location=location)

    table_ref = client.dataset(dataset_id).table(table_id)

    rows_to_insert = [data]
    errors = client.insert_rows_json(table_ref, rows_to_insert)

    if errors == []:
        print("Data inserted successfully.")
    else:
        print("Errors occurred while inserting data:", errors)

def main(path, mimetype):

    document = load_document(path, mimetype)

    print("Document processing complete.")

    extracted_data = extract_data(document)

    # Insert invoice data into the BigQuery table
    load_to_bigquery(extracted_data)


main(file_path, mime_type)
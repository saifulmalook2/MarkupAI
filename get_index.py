from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SimpleField,
    SearchFieldDataType,
    SearchableField,
    SearchField,
    VectorSearch,
    HnswAlgorithmConfiguration,
    VectorSearchProfile,
    SemanticConfiguration,
    SemanticPrioritizedFields,
    SemanticField,
    SemanticSearch,
    SearchIndex,
    AzureOpenAIVectorizer,
    AzureOpenAIVectorizerParameters, 
    ComplexField
)
from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import ResourceNotFoundError
from azure.core.credentials import AzureKeyCredential
import os


# Function to create or retrieve an index based on client_id
def create_or_get_index(client_id, azure_openai_embedding_dimensions=1536):
    
    # Create a search index client
    index_client = SearchIndexClient(endpoint=os.getenv('AZURE_SEARCH_ENDPOINT'), credential=AzureKeyCredential(os.getenv('AZURE_SEARCH_KEY')))
    
    index_name = f"{client_id}-index"  # Generate a name based on client_id
    
    try:
        # Check if the index exists
        index = index_client.get_index(index_name)
        print(f"Index '{index_name}' already exists")
        return index  # Return the existing index
    except ResourceNotFoundError:
        # If the index does not exist, create it
        print(f"Index '{index_name}' does not exist. Creating a new index.")

        # Define the fields for the search index (based on the screenshot you provided)
        fields = [
            SimpleField(name="id", type=SearchFieldDataType.String, key=True, sortable=True, filterable=True, retrievable=True),
            SearchableField(name="content", type=SearchFieldDataType.String),  # content field
            SearchField(name="content_vector", type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                        searchable=True, vector_search_dimensions=azure_openai_embedding_dimensions, vector_search_profile_name="myHnswProfile"),
            ComplexField(
                name="metadata",  # Complex field for metadata
                fields=[
                    SimpleField(name="source", type=SearchFieldDataType.String, filterable=True, retrievable=True, searchable=True),
                    SimpleField(name="page", type=SearchFieldDataType.Int32, filterable=True, sortable=True),
                    SimpleField(name="sheet", type=SearchFieldDataType.String, filterable=True, retrievable=True, searchable=True)
                ]
            )
        ]
        
        # Configure vector search
        vector_search = VectorSearch(
            algorithms=[
                HnswAlgorithmConfiguration(name="myHnsw")
            ],
            profiles=[
                VectorSearchProfile(
                    name="myHnswProfile",
                    algorithm_configuration_name="myHnsw",
                    vectorizer="vector-profile-1724673280264"
                )
            ],
            vectorizers=[
                AzureOpenAIVectorizer(
                    vectorizer_name="vector-profile-1724673280264",
                    parameters=AzureOpenAIVectorizerParameters(
                        resource_url=os.getenv('AZURE_OPENAI_ENDPOINT_EMBEDDINGS'),
                        deployment_name=os.getenv('AZURE_OPENAI_DEPLOYMENT_EMBEDDINGS'),
                        model_name="text-embedding-ada-002",
                        api_key=os.getenv('AZURE_OPENAI_API_KEY_EMBEDDINGS')
                    )
                )
            ]
        )
        
        # Configure semantic search settings
        semantic_config = SemanticConfiguration(
            name="my-semantic-config",
            prioritized_fields=SemanticPrioritizedFields(
                content_fields=[SemanticField(field_name="content")]
            )
        )
        
        semantic_search = SemanticSearch(configurations=[semantic_config])
        
        # Create the search index
        index = SearchIndex(
            name=index_name,
            fields=fields,
            vector_search=vector_search,
            semantic_search=semantic_search
        )
        
        # Create or update the index
        result = index_client.create_or_update_index(index)
        print(f"Index '{result.name}' created successfully")
        return result

# Example Usage


client_id = "testing"
create_or_get_index(client_id)

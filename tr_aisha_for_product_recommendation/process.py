import datetime
import logging
import os
from typing import Iterator

import apache_beam as beam
from llama_index import (
    Document,
)
import numpy as np
from tr_aisha_for_product_recommendation.utils import (
    check_indices_exist,
    init_llamaindex_context,
    init_llamaindex_indices,
    load_indices,
    store_indices,
) 

logging.getLogger().setLevel(logging.DEBUG)


class Llamaindexing(beam.DoFn):
    def __init__(
            self, collection_name, metadata_dir, version,
            openai_api_key, qdrant_api_key, qdrant_api_url, mongo_uri,
            state_path
    ):
        self.config_path = os.path.join(
            metadata_dir, "configs/" + version + ".json"
        )
        self.scheme_path = os.path.join(
            metadata_dir, "schemes/" + version + ".json"
        )
        self.state_path = state_path
        self.collection_name = collection_name
        self.openai_api_key = openai_api_key
        self.qdrant_api_key = qdrant_api_key
        self.qdrant_api_url = qdrant_api_url
        self.mongo_uri = mongo_uri

        # initiate the indices to fill by the workers
        service_context, storage_context, _ = init_llamaindex_context(
            collection_name=self.collection_name,
            openai_api_key=self.openai_api_key,
            qdrant_api_key=self.qdrant_api_key,
            qdrant_api_url=self.qdrant_api_url,
            mongo_uri=self.mongo_uri,
            config_path=self.config_path,
            scheme_path=self.scheme_path,
        )
        if not check_indices_exist(self.state_path):
            mock_documents = [Document(
                text="mock document",
                metadata={
                    metadata["name"]: "mock metadata"
                    for metadata in self.scheme["input"]["metadata_info"]
                }
            )]
            mock_nodes = service_context.node_parser.get_nodes_from_documents(mock_documents, show_progress=True)
            indices = init_llamaindex_indices(storage_context, mock_nodes)
            store_indices(indices, self.state_path)

    def setup(self):
        self.service_context, self.storage_context, self.scheme = init_llamaindex_context(
            collection_name=self.collection_name,
            openai_api_key=self.openai_api_key,
            qdrant_api_key=self.qdrant_api_key,
            qdrant_api_url=self.qdrant_api_url,
            mongo_uri=self.mongo_uri,
            config_path=self.config_path,
            scheme_path=self.scheme_path,
        )
        assert check_indices_exist(self.state_path)
        indices = load_indices(self.state_path)
        self.indices = init_llamaindex_indices(self.storage_context, indices=indices)

    def process_batch(self, batch: np.ndarray) -> Iterator[np.ndarray]:
        documents = [
            Document(
                text=element[0],
                metadata=element[1]  # TODO: verify data with the scheme
            )
            for element in batch
        ]
        nodes = self.service_context.node_parser.get_nodes_from_documents(documents, show_progress=True)

        logging.info("Processing indices")
        self.indices["vector_index"].build_index_from_nodes(nodes)
        self.indices["keyword_table_index"].build_index_from_nodes(nodes)

    # Declare what the element-wise output type is
    def infer_output_type(self, input_element_type):
        return input_element_type

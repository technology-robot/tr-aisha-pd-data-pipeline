import os
import time


def parse_to_schema(x, schema):
    return schema(**{
        field: getattr(x, field)
        for field in schema._fields
    })


def retry(func, sleep_time=1, max_retry=5):
    curr_retry = 0
    err_to_reraise = None
    while curr_retry < max_retry:
        try:
            return func()
        except Exception as err:
            time.sleep(sleep_time)
            err_to_reraise = err
        curr_retry += 1
    raise err_to_reraise

def init_llamaindex_context(
        collection_name,
        openai_api_key,
        qdrant_api_url,
        qdrant_api_key,
        mongo_uri,
        config_path,
        scheme_path,
):
    import json

    import gcsfs
    from llama_index import (
        ServiceContext,
        OpenAIEmbedding,
        PromptHelper,
        StorageContext,
        set_global_service_context
    )
    from llama_index.llms import OpenAI
    from llama_index.text_splitter import SentenceSplitter
    from llama_index.storage.docstore import MongoDocumentStore
    from llama_index.storage.index_store import MongoIndexStore
    from llama_index.vector_stores.qdrant import QdrantVectorStore
    from qdrant_client import QdrantClient

    qdrant_client = QdrantClient(
        qdrant_api_url,
        api_key=qdrant_api_key,
    )
    gcs_fs = gcsfs.GCSFileSystem()

    with gcs_fs.open(config_path, 'r') as f_p:
        config = json.load(f_p)

    with gcs_fs.open(scheme_path, 'r') as f_p:
        scheme = json.load(f_p)

    os.environ["OPENAI_API_KEY"] = openai_api_key
    llm = OpenAI(model=str(config["openai_model"]), temperature=int(config["openai_temperature"]), max_tokens=int(config["openai_max_tokens"]))
    embed_model = OpenAIEmbedding(
        mode=str(config["openai_embedding_mode"]),
        model=str(config["openai_embedding_model"]),
        embed_batch_size=str(config["openai_embedding_embed_batch_size"])
    )
    text_splitter = SentenceSplitter(chunk_size=int(config["sentence_splitter_chunk_size"]), chunk_overlap=int(config["sentence_splitter_chunk_overlap"]))
    prompt_helper = PromptHelper(
        context_window=int(config["prompt_helper_context_window"]),
        num_output=int(config["prompt_helper_num_output"]),
        chunk_overlap_ratio=float(config["prompt_helper_chunk_overlap_ratio"]),
    )
    service_context = ServiceContext.from_defaults(
        llm=llm,
        embed_model=embed_model,
        text_splitter=text_splitter,
        prompt_helper=prompt_helper,
    )
    set_global_service_context(service_context)    

    mongodb_doc_store = MongoDocumentStore.from_uri(uri=mongo_uri, db_name=collection_name)
    mongodb_index_store = MongoIndexStore.from_uri(uri=mongo_uri, db_name=collection_name)
    qdrant_store = QdrantVectorStore(client=qdrant_client, collection_name=collection_name, prefer_grpc=True)

    storage_context = StorageContext.from_defaults(
        docstore=mongodb_doc_store,
        index_store=mongodb_index_store,
        vector_store=qdrant_store
    )

    return service_context, storage_context, scheme

def init_llamaindex_indices(
        storage_context,
        nodes = None,
        indices = None
):
    from llama_index import SimpleKeywordTableIndex, VectorStoreIndex
    from llama_index.indices.loading import load_index_from_storage

    if indices is None:
        vector_index = VectorStoreIndex(
            nodes,
            storage_context=storage_context,
            show_progress=True
        )
        keyword_table_index = SimpleKeywordTableIndex(
            nodes,
            storage_context=storage_context,
            show_progress=True
        )
    else:
        vector_index = load_index_from_storage(storage_context=storage_context, index_id=indices["vector_index"])
        keyword_table_index = load_index_from_storage(storage_context=storage_context, index_id=indices["keyword_table_index"])

    return {
        "vector_index": vector_index,
        "keyword_table_index": keyword_table_index,
    }

def store_indices(indices, state_path):
    import json
    import gcsfs
    gcs_fs = gcsfs.GCSFileSystem()
    with gcs_fs.open(state_path, 'w') as f_p:
        state = {
            "indices": {index_key: index_value.index_id for index_key, index_value in indices.items()}
        }
        json.dump(state, f_p)

def load_indices(state_path):
    import json
    import gcsfs
    gcs_fs = gcsfs.GCSFileSystem()
    with gcs_fs.open(state_path, 'r') as f_p:
        return json.load(f_p)["indices"]

def check_indices_exist(state_path):
    import gcsfs
    gcs_fs = gcsfs.GCSFileSystem()
    return gcs_fs.exists(state_path)

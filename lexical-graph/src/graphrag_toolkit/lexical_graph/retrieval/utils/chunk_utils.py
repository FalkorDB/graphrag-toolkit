# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import numpy as np
import threading
import logging
from typing import Dict, List
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import node_result
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore

logger = logging.getLogger(__name__)

def cosine_similarity(query_embedding, chunk_embeddings):
    
    if not chunk_embeddings:
        return np.array([]), []

    query_embedding = np.array(query_embedding)
    chunk_ids, chunk_embeddings = zip(*chunk_embeddings.items())
    chunk_embeddings = np.array(chunk_embeddings)

    dot_product = np.dot(chunk_embeddings, query_embedding)
    norms = np.linalg.norm(chunk_embeddings, axis=1) * np.linalg.norm(query_embedding)
    
    similarities = dot_product / norms
    return similarities, chunk_ids

def get_top_k(query_embedding, chunk_embeddings, top_k):
   
    logger.debug(f'chunk_embeddings: {chunk_embeddings}')

    if not chunk_embeddings:
        return []  
    
    similarities, chunk_ids = cosine_similarity(query_embedding, chunk_embeddings)

    logger.debug(f'similarities: {similarities}')
    
    if len(similarities) == 0:
        return []

    top_k = min(top_k, len(similarities))
    top_indices = np.argsort(similarities)[::-1][:top_k]

    top_chunk_ids = [chunk_ids[idx] for idx in top_indices]
    top_similarities = similarities[top_indices]
    return list(zip(top_similarities, top_chunk_ids))

def get_chunks_query(graph_store, chunk_ids):

    cypher = f'''
    MATCH (chunk:`__Chunk__`)-[:`__EXTRACTED_FROM__`]->(source:`__Source__`) WHERE {graph_store.node_id("chunk.chunkId")} in $chunk_ids
    RETURN {{
        source: {{
            sourceId: {graph_store.node_id("source.sourceId")},
            {node_result('source', key_name='metadata')}
        }},
        {node_result('chunk', graph_store.node_id("chunk.chunkId"))}
    }} AS result
    '''
    params = {'chunk_ids': chunk_ids}
    chunks = graph_store.execute_query(cypher, params)
    results = []
    for chunk_id in chunk_ids:
                for chunk in chunks:
                    if chunk['result']['chunk']['chunkId'] == chunk_id:
                        results.append(chunk)
    return results

class SharedChunkEmbeddingCache:

    def __init__(self, vector_store:VectorStore):
        self._cache: Dict[str, np.ndarray] = {}
        self._lock = threading.Lock()
        self.vector_store = vector_store

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),retry=retry_if_exception_type(Exception))
    def _fetch_embeddings(self, chunk_ids: List[str]) -> Dict[str, np.ndarray]:
       
        embeddings = self.vector_store.get_index('chunk').get_embeddings(chunk_ids)
        return {
            e['chunk']['chunkId']: np.array(e['embedding']) 
            for e in embeddings
        }

    def get_embeddings(self, chunk_ids: List[str]) -> Dict[str, np.ndarray]:

        missing_ids = []
        cached_embeddings = {}

        logger.debug(f'chunk_ids: {chunk_ids}')

        # Check cache first
        for sid in chunk_ids:
            if sid in self._cache:
                cached_embeddings[sid] = self._cache[sid]
            else:
                missing_ids.append(sid)

        logger.debug(f'missing_ids: {missing_ids}')

        # Fetch missing embeddings with retry
        if missing_ids:
            try:
                new_embeddings = self._fetch_embeddings(missing_ids)
                with self._lock:
                    self._cache.update(new_embeddings)
                    cached_embeddings.update(new_embeddings)
            except Exception as e:
                logger.error(f"Failed to fetch embeddings after retries: {e}")
                # Return what we have from cache
                logger.warning(f"Returning {len(cached_embeddings)} cached embeddings out of {len(chunk_ids)} requested")

        return cached_embeddings
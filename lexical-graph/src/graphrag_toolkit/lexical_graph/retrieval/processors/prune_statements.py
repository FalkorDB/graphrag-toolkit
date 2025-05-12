# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection, SearchResult, Topic

from llama_index.core.schema import QueryBundle

logger = logging.getLogger(__name__)

class PruneStatements(ProcessorBase):
    def __init__(self, args:ProcessorArgs, filter_config:FilterConfig):
        super().__init__(args, filter_config)

    def _process_results(self, search_results:SearchResultCollection, query:QueryBundle) -> SearchResultCollection:
        def prune_statements(topic:Topic):
            surviving_statements = [
                s 
                for s in topic.statements 
                if s.score >= self.args.statement_pruning_threshold
            ]
            topic.statements = surviving_statements
            return topic

        def prune_search_result(index:int, search_result:SearchResult):
            return self._apply_to_topics(search_result, prune_statements)
        
        return self._apply_to_search_results(search_results, prune_search_result)



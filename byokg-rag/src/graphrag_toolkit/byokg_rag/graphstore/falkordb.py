# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse

from .graphstore import GraphStore

logger = logging.getLogger(__name__)

try:
    import falkordb
    from falkordb import Graph
    from redis.exceptions import ResponseError, AuthenticationError, ConnectionError
except ImportError as e:
    raise ImportError(
        "FalkorDB and/or redis packages not found, install with 'pip install falkordb redis'"
    ) from e


class FalkorDBGraphStore(GraphStore):
    """
    Graph store for interacting with a FalkorDB database.
    
    Provides an implementation of the GraphStore interface for FalkorDB,
    using OpenCypher queries to interact with the graph database.
    """

    def __init__(self, endpoint_url=None, database="graphrag", username=None, password=None, ssl=False):
        """
        Create a FalkorDB backed GraphStore.

        :param endpoint_url: str. The endpoint URL of the FalkorDB instance in the format 'host:port'
        :param database: str. Name of the database/graph to connect to
        :param username: str. Username for authentication (optional)
        :param password: str. Password for authentication (optional)
        :param ssl: bool. Whether to use SSL for the connection
        """
        self.endpoint_url = endpoint_url
        self.database = database
        self.username = username
        self.password = password
        self.ssl = ssl
        self._client = None
        
    @property
    def client(self) -> Graph:
        """
        Establish and return a FalkorDB client instance.

        :return: A FalkorDB Graph instance.
        :raises ConnectionError: If the connection to FalkorDB fails.
        """
        if self._client is None:
            try:
                if self.endpoint_url:
                    # Parse endpoint URL
                    if '://' in self.endpoint_url:
                        parsed = urlparse(self.endpoint_url)
                        host = parsed.hostname or 'localhost'
                        port = parsed.port or 6379
                    else:
                        # Simple host:port format
                        parts = self.endpoint_url.split(':')
                        if len(parts) == 2:
                            host = parts[0]
                            port = int(parts[1])
                        else:
                            host = self.endpoint_url
                            port = 6379
                else:
                    host = "localhost"
                    port = 6379

                # Create FalkorDB connection
                self._client = falkordb.FalkorDB(
                    host=host,
                    port=port,
                    username=self.username,
                    password=self.password,
                    ssl=self.ssl,
                ).select_graph(self.database)
                
            except ConnectionError as e:
                logger.error(f"Failed to connect to FalkorDB: {e}")
                raise ConnectionError(f"Could not establish connection to FalkorDB: {e}") from e
            except AuthenticationError as e:
                logger.error(f"Authentication failed: {e}")
                raise ConnectionError(f"Authentication failed: {e}") from e
            except Exception as e:
                logger.error(f"Unexpected error while connecting to FalkorDB: {e}")
                raise ConnectionError(f"Unexpected error while connecting to FalkorDB: {e}") from e
                
        return self._client

    def execute_query(self, cypher, parameters=None):
        """
        Execute a Cypher query against the FalkorDB instance.
        
        :param cypher: str. The OpenCypher query to execute
        :param parameters: dict. Query parameters
        :return: List of result records
        """
        if parameters is None:
            parameters = {}
            
        logger.debug(f"Executing query: {cypher} with parameters: {parameters}")
        
        try:
            response = self.client.query(cypher, params=parameters)
            # Convert FalkorDB result to list of dictionaries
            results = []
            if response.result_set:
                header = [col[1] for col in response.header]
                for row in response.result_set:
                    result_dict = {}
                    for i, value in enumerate(row):
                        if i < len(header):
                            result_dict[header[i]] = value
                    results.append(result_dict)
            return results
        except ResponseError as e:
            logger.error(f"Query execution failed: {e}. Query: {cypher}, Parameters: {parameters}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during query execution: {e}. Query: {cypher}, Parameters: {parameters}")
            raise

    def get_schema(self):
        """
        Return the graph schema.
        
        :return: dict containing schema information
        """
        try:
            # Get node labels
            node_labels_query = "CALL db.labels() YIELD label RETURN label"
            node_labels_response = self.execute_query(node_labels_query)
            node_labels = [item["label"] for item in node_labels_response]
            
            # Get relationship types
            rel_types_query = "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType"
            rel_types_response = self.execute_query(rel_types_query)
            edge_labels = [item["relationshipType"] for item in rel_types_response]
            
            # Get detailed schema information for nodes
            node_label_details = {}
            for label in node_labels:
                prop_query = f"""MATCH (n:`{label}`)
                               WITH n LIMIT 100
                               UNWIND keys(n) AS key
                               RETURN COLLECT(DISTINCT key) AS properties"""
                try:
                    response = self.execute_query(prop_query)
                    if response:
                        node_label_details[label] = {"properties": response[0].get("properties", [])}
                    else:
                        node_label_details[label] = {"properties": []}
                except Exception as e:
                    logger.warning(f"Could not get properties for node label {label}: {e}")
                    node_label_details[label] = {"properties": []}
            
            # Get detailed schema information for edges
            edge_label_details = {}
            for edge_type in edge_labels:
                prop_query = f"""MATCH ()-[r:`{edge_type}`]->()
                               WITH r LIMIT 100
                               UNWIND keys(r) AS key
                               RETURN COLLECT(DISTINCT key) AS properties"""
                try:
                    response = self.execute_query(prop_query)
                    if response:
                        edge_label_details[edge_type] = {"properties": response[0].get("properties", [])}
                    else:
                        edge_label_details[edge_type] = {"properties": []}
                except Exception as e:
                    logger.warning(f"Could not get properties for edge type {edge_type}: {e}")
                    edge_label_details[edge_type] = {"properties": []}
            
            # Get label triples (relationships between node types)
            triples_query = """
                MATCH (x)-[r]->(y)
                RETURN DISTINCT head(labels(x)) AS `~from`, type(r) AS `~type`, head(labels(y)) AS `~to`
                LIMIT 1000
            """
            try:
                label_triples = self.execute_query(triples_query)
            except Exception as e:
                logger.warning(f"Could not get label triples: {e}")
                label_triples = []
            
            schema = {
                "graphSummary": {
                    "nodeLabels": node_labels,
                    "edgeLabels": edge_labels,
                    "nodeLabelDetails": node_label_details,
                    "edgeLabelDetails": edge_label_details,
                    "labelTriples": label_triples
                }
            }
            
            return schema
            
        except Exception as e:
            logger.error(f"Failed to get schema: {e}")
            # Return minimal schema on error
            return {
                "graphSummary": {
                    "nodeLabels": [],
                    "edgeLabels": [],
                    "nodeLabelDetails": {},
                    "edgeLabelDetails": {},
                    "labelTriples": []
                }
            }

    def nodes(self):
        """
        Return a list of all node_ids in the graph.

        :return: List[str] all node_ids in the graph
        """
        query = "MATCH (n) RETURN ID(n) AS node"
        response = self.execute_query(query)
        return [str(item["node"]) for item in response]

    def get_nodes(self, node_ids):
        """
        Return node details for given node ids.

        :param node_ids: List[str] node ids
        :return: Dict[node_id:str, Any] node details
        """
        if not node_ids:
            return {}
            
        # Convert string IDs to integers for FalkorDB
        try:
            int_node_ids = [int(node_id) for node_id in node_ids]
        except ValueError:
            # If conversion fails, treat as string IDs and search by property
            query = """MATCH (n)
                      WHERE n.id IN $node_ids OR ID(n) IN $node_ids
                      RETURN properties(n) as properties, ID(n) as node"""
            response = self.execute_query(query, parameters={'node_ids': node_ids})
        else:
            query = """MATCH (n)
                      WHERE ID(n) IN $node_ids
                      RETURN properties(n) as properties, ID(n) as node"""
            response = self.execute_query(query, parameters={'node_ids': int_node_ids})
        
        return {str(item["node"]): item["properties"] for item in response}

    def edges(self):
        """
        Return a list of all edge_ids in the graph.

        :return: List[str] all edge_ids in the graph
        """
        query = "MATCH ()-[e]-() RETURN ID(e) as edge"
        response = self.execute_query(query)
        return [str(item["edge"]) for item in response]

    def get_edges(self, edge_ids):
        """
        Return edge details for given edge ids.

        :param edge_ids: List[str] edge ids
        :return: Dict[edge_id:str, Any] edge details
        """
        if not edge_ids:
            return {}
            
        # Convert string IDs to integers for FalkorDB
        try:
            int_edge_ids = [int(edge_id) for edge_id in edge_ids]
        except ValueError:
            # If conversion fails, return empty dict
            logger.warning(f"Could not convert edge IDs to integers: {edge_ids}")
            return {}
        
        query = """MATCH ()-[e]->()
                  WHERE ID(e) IN $edge_ids
                  RETURN properties(e) as properties, ID(e) as edge"""
        response = self.execute_query(query, parameters={'edge_ids': int_edge_ids})
        return {str(item["edge"]): item["properties"] for item in response}

    def get_one_hop_edges(self, source_node_ids, return_triplets=False):
        """
        Return one hop edges given a set of source node ids.

        :param source_node_ids: List[str] the node ids to start traversal from
        :param return_triplets: whether to return edge_ids only or return triplets in the form (src_node_id, edge, dst_node_id)
        :return: Dict[node_id:str, Dict[edge_type:str, edge_ids:List[str]]] or Dict[node_id:str, Dict[edge_type:str, List[(node_id:str, edge_type:str, node_id:str)]]]
        """
        if not source_node_ids:
            return {}
            
        # Try to convert to integers, but also handle string-based lookups
        try:
            int_node_ids = [int(node_id) for node_id in source_node_ids]
            query = """MATCH (n)-[e]->(m)
                      WHERE ID(n) IN $node_ids
                      RETURN DISTINCT ID(n) as node, ID(e) as edge, type(e) as edge_type, ID(m) as dst_node"""
        except ValueError:
            # Handle string-based node IDs
            query = """MATCH (n)-[e]->(m)
                      WHERE n.id IN $node_ids OR ID(n) IN $node_ids
                      RETURN DISTINCT ID(n) as node, ID(e) as edge, type(e) as edge_type, ID(m) as dst_node"""
            int_node_ids = source_node_ids
        
        response = self.execute_query(query, parameters={'node_ids': int_node_ids})
        
        expanded_edges = {}
        for item in response:
            source_node = str(item["node"])
            if source_node not in expanded_edges:
                expanded_edges[source_node] = {}
            
            edge_type = item['edge_type']
            if edge_type not in expanded_edges[source_node]:
                expanded_edges[source_node][edge_type] = []
            
            if return_triplets:
                triplet = (source_node, edge_type, str(item["dst_node"]))
                expanded_edges[source_node][edge_type].append(triplet)
            else:
                expanded_edges[source_node][edge_type].append(str(item["edge"]))
        
        return expanded_edges

    def get_edge_destination_nodes(self, edge_ids):
        """
        Return destination nodes given a set of edge ids.

        :param edge_ids: List[str] the edge ids to select the destination nodes of
        :return: Dict[str, List[str]]
        """
        if not edge_ids:
            return {}
            
        # Convert string IDs to integers for FalkorDB
        try:
            int_edge_ids = [int(edge_id) for edge_id in edge_ids]
        except ValueError:
            logger.warning(f"Could not convert edge IDs to integers: {edge_ids}")
            return {}
        
        query = """MATCH ()-[e]->(m)
                  WHERE ID(e) IN $edge_ids
                  RETURN ID(e) as edge, ID(m) as dst_node"""
        response = self.execute_query(query, parameters={'edge_ids': int_edge_ids})
        
        result = {}
        for item in response:
            edge_id = str(item["edge"])
            dst_node = str(item["dst_node"])
            if edge_id not in result:
                result[edge_id] = []
            result[edge_id].append(dst_node)
        
        return result

    def get_linker_tasks(self):
        """
        Return supported linker tasks for FalkorDB.
        """
        return [
            "entity-extraction",
            "path-extraction", 
            "draft-answer-generation",
            "opencypher"
        ]
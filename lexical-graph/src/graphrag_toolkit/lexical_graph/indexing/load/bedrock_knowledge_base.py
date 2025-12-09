# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


import json
import os
import io
import uuid
import logging
import shutil
import copy
import base64
from pathlib import Path
from typing import Callable, Dict, Any, Optional
from os.path import join
from urllib.parse import urlparse

from graphrag_toolkit.lexical_graph.indexing import IdGenerator
from graphrag_toolkit.lexical_graph.indexing.load.file_based_chunks import FileBasedChunks
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
from graphrag_toolkit.lexical_graph.indexing.extract.id_rewriter import IdRewriter
from graphrag_toolkit.lexical_graph.indexing.utils.hash_utils import get_hash
from graphrag_toolkit.lexical_graph import GraphRAGConfig

from llama_index.core.schema import TextNode, Document
from llama_index.core.schema import NodeRelationship, RelatedNodeInfo

logger = logging.getLogger(__name__)

class TempFile():
    """
    Represents a temporary file wrapper for managing file operations and cleanup automatically.

    This class provides context management for opening, reading, and safely cleaning up temporary files.
    The file specified by the `filepath` is deleted after the context ends.

    Attributes:
        filepath (str): The path of the file to be managed as a temporary file.
        file (IO): The file object opened in the context.
    """
    def __init__(self, filepath):
        """
        Represents a file handler that initializes and manages a specific file path.

        Attributes:
            filepath: The path of the file to be managed.

        Args:
            filepath: The path to the target file.
        """
        self.filepath = filepath
        
    def __enter__(self):
        """
        Opens a file for the context of the object and returns the object itself.
        This method is intended to be used in a context manager where the file
        needs to be opened and closed automatically.

        Returns:
            The object itself with the opened file ready for use.
        """
        self.file = open(self.filepath)
        return self
    
    def __exit__(self, exception_type, exception_value, exception_traceback):
        """
        Manages cleanup operations when exiting a context, including closing a file and removing its associated
        file from the filesystem.

        This method is intended to be used within a context manager. It ensures that the file associated with
        the class instance is properly closed and deleted from the filesystem once the context is exited,
        whether the execution exits normally or due to an exception.

        Args:
            exception_type: The class of the exception when an exception is raised, otherwise None.
            exception_value: The instance of the exception when an exception is raised, otherwise None.
            exception_traceback: The traceback object when an exception is raised, otherwise None.
        """
        self.file.close()
        os.remove(self.filepath)
        
    def readline(self):
        """
        Reads a single line from the file associated with the current object.

        This method retrieves the next line from the file and advances the file's
        read pointer. It is typically used for iterating through lines in a file
        or processing input line by line.

        Returns:
            str: The next line from the file as a string. If the end of the file
                has been reached, an empty string is returned.
        """
        return self.file.readline()
    
class TempDir():
    """
    Handles creation and cleanup of a temporary directory.

    This class is used to manage a temporary directory within a context manager. It
    handles the creation of the directory when the context is entered and ensures
    the directory is cleaned up and removed when the context is exited, even if an
    exception occurs.

    Attributes:
        dir_path (str): Path to the temporary directory.
    """
    def __init__(self, dir_path):
        """
        This class provides functionality to work with a specified directory path. It allows storing the directory
        path for later use or for performing various directory-related operations.

        Attributes:
            dir_path (str): The path to the directory that this class will work with.
        """
        self.dir_path = dir_path
        
    def __enter__(self):
        """
        Manages a directory ensuring its existence for context management.

        The `__enter__` method checks whether the directory specified by `dir_path` exists.
        If it does not exist, the method creates the required directory structure. This
        enables the use of the instance as a context manager for managing directories.

        Returns:
            self: The current instance of the class to manage the directory within
                the context block.

        Raises:
            OSError: If an error occurs while creating the directory.
        """
        if not os.path.exists(self.dir_path):
            os.makedirs(self.dir_path)
        return self
    
    def __exit__(self, exception_type, exception_value, exception_traceback):
        """
        Handles the cleanup of temporary directories upon exiting a context.

        This method ensures that any temporary directory specified by the provided
        `dir_path` is removed when the context manager exits.

        Args:
            exception_type: The exception type raised during the execution of the
                context (if an exception occurred) or None if no exception occurred.
            exception_value: The exception value raised during the execution of the
                context (if an exception occurred) or None if no exception occurred.
            exception_traceback: The traceback object for the exception raised
                during the execution of the context (if an exception occurred) or
                None if no exception occurred.
        """
        if os.path.exists(self.dir_path):
            shutil.rmtree(self.dir_path)

class BedrockKnowledgeBaseExport():
    """
    Handles the export and processing of Amazon Bedrock Knowledge Base (KB) data.

    The primary purpose of this class is to work with Amazon Bedrock Knowledge Base
    exports by managing data retrieval, processing, and transformation tasks. This
    includes facilities for downloading and processing KB chunks, managing associated
    metadata, and handling source documents.

    Attributes:
        region (str): The AWS region where the S3 bucket resides.
        bucket_name (str): The name of the S3 bucket containing the KB export files.
        key_prefix (str): The prefix used to specify the KB export files in the S3 bucket.
        limit (int): The maximum number of documents to process. A value of -1 indicates no limit.
        output_dir (str): The directory in which output files are temporarily stored.
        metadata_fn (Callable[[str], Dict[str, Any]]): A function to process and retrieve metadata
            from the content. Defaults to None.
        include_embeddings (bool): Specifies whether to include embeddings in the output data.
            Defaults to True.
        include_source_doc (bool): Specifies whether to include the source document in the output.
            Defaults to False.
        tenant_id (str): The tenant ID used for generating unique document identifiers.
            Defaults to None.
        s3_client (object): The Amazon S3 client instance used for managing S3 operations.
        id_rewriter (object): The mechanism used to rewrite document IDs for support of tenant-specific
            operations.
    """
    def __init__(self, 
                 region:str, 
                 bucket_name:str, 
                 key_prefix:str, 
                 limit:int=-1, 
                 output_dir:str='output', 
                 metadata_fn:Callable[[str], Dict[str, Any]]=None,
                 include_embeddings:bool=True,
                 include_source_doc:bool=False,
                 tenant_id:str=None,
                 include_classification_in_entity_id:Optional[bool]=None,
                 **kwargs):
        """
        Initializes the instance with configuration for connecting to an S3 bucket and
        managing related properties. This includes parameters for specifying the S3
        region, bucket details, and various options for handling metadata, embeddings,
        and source documents.

        Args:
            region (str): The AWS region where the S3 bucket is located.
            bucket_name (str): The name of the S3 bucket.
            key_prefix (str): Prefix for the S3 object keys to filter objects in the
                bucket.
            limit (int): Maximum number of S3 objects to process. Defaults to -1,
                indicating no limit.
            output_dir (str): Directory path to store processed output. Defaults to
                'output'.
            metadata_fn (Callable[[str], Dict[str, Any]]): Callable function to
                generate metadata for a given input. Defaults to None.
            include_embeddings (bool): Flag to include embeddings in the output.
                Defaults to True.
            include_source_doc (bool): Flag to include the source document in the
                output. Defaults to False.
            tenant_id (str): Identifier for the tenant. Defaults to None.
            **kwargs: Additional keyword arguments for customization.
        """
        include_classification_in_entity_id = include_classification_in_entity_id or GraphRAGConfig.include_classification_in_entity_id
        id_generator=IdGenerator(tenant_id=tenant_id, include_classification_in_entity_id=include_classification_in_entity_id)

        self.bucket_name=bucket_name
        self.key_prefix=key_prefix
        self.region=region
        self.limit=limit
        self.output_dir = output_dir
        self.s3_client = GraphRAGConfig.s3
        self.id_rewriter = IdRewriter(id_generator=id_generator)
        self.metadata_fn=metadata_fn
        self.include_embeddings = include_embeddings
        self.include_source_doc = include_source_doc
        
    def _kb_chunks(self, kb_export_dir):
        """
        Generates and yields chunks of data from knowledge base export files stored in an Amazon S3
        bucket. Each chunk is read line by line from the files after downloading them temporarily.

        Args:
            kb_export_dir (str): The directory path where temporary files will be stored after
                downloading from the S3 bucket.

        Yields:
            dict: A dictionary representing a JSON object parsed from a single line of the knowledge
                base export file.
        """
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket_name, Prefix=self.key_prefix)

        keys = [
            obj['Key']
            for page in pages
            for obj in page['Contents'] 
        ]
        
        for key in keys:
        
            logger.info(f'Loading Amazon Bedrock Knowledge Base export file [bucket: {self.bucket_name}, key: {key}, region: {self.region}]')

            temp_filepath = join(kb_export_dir, f'{uuid.uuid4().hex}.json')
            self.s3_client.download_file(self.bucket_name, key, temp_filepath)

            with TempFile(temp_filepath) as f:
                while True:
                    line = f.readline()
                    if not line:
                        break
                    else:
                        yield json.loads(line)
                        
    def _parse_key(self, source):
        """
        Parses the provided source URL to extract and return the path without the leading
        forward slash.

        This function utilizes the `urlparse` method from Python's `urllib.parse` module to
        analyze the given URL. It removes certain components, such as fragments, and focuses
        on the path component by stripping the leading character. The output is the cleaned
        path section of the provided URL.

        Args:
            source: The URL to be parsed. It should be a string representation of a valid
                URL or URI.

        Returns:
            str: The cleaned path segment from the provided source URL, without the leading
                forward slash.
        """
        parsed = urlparse(source, allow_fragments=False)
        return parsed.path.lstrip('/')
    
    def _download_source_doc(self, source, doc_file_path):
        """
        Downloads a source document from an S3 bucket and processes it into a desired format
        based on its content type. The processed document is saved to a given file path in
        JSON format with metadata and content.

        This method supports processing PDF and text-based documents, and enriches the document
        with additional metadata provided by a callable function, if specified.

        Args:
            source (str): The identifier of the source document in the form of a key or URL.
            doc_file_path (str): The file path where the processed document will be saved.

        Returns:
            Document: The processed document object containing the text and metadata.
        """
        key = self._parse_key(source)

        logger.debug(f'Loading Amazon Bedrock Knowledge Base underlying source document [source: {source}, bucket: {self.bucket_name}, key: {key}, region: {self.region}]')
            
        object_metadata = self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
        content_type = object_metadata.get('ContentType', None)

        with io.BytesIO() as io_stream:
            self.s3_client.download_fileobj(self.bucket_name, key, io_stream)
        
            io_stream.seek(0)

            if content_type and content_type in ['application/pdf']:
                data = base64.b64encode(io_stream.read())
            else:
                data = io_stream.read().decode('utf-8')
            
        metadata = self.metadata_fn(data) if self.metadata_fn else {}

        if 'source' not in metadata:
            metadata['source'] = source
            
        doc = Document(
            text=data,
            metadata=metadata
        )
        
        doc = self.id_rewriter([doc])[0]
        
        with open(doc_file_path, 'w') as f:
                f.write(doc.to_json())
        
        return doc
    
    def _open_source_doc(self, doc_file_path):
        """
        Opens and loads a source document from the specified file path.

        This function reads a JSON file from the given file path, parses its
        content into a dictionary, and converts it into a `Document` object
        using the `from_dict` method.

        Args:
            doc_file_path: The path to the JSON file containing the document data.

        Returns:
            Document: An instance of the `Document` class created from the JSON data.
        """
        with open(doc_file_path) as f:
            data = json.load(f)
            return Document.from_dict(data)
    
    def _get_source_doc(self, source_docs_dir, source):
        """
        Retrieves the source document from a specified directory. If it does not exist,
        downloads and stores it in the designated path.

        Args:
            source_docs_dir: Base directory path where source documents are stored.
            source: Unique identifier or source content for which the corresponding
                document is being retrieved.

        Returns:
            The content of the source document.

        Raises:
            OSError: If there are issues creating or accessing the specified
                directories or files.
        """
        source_id = get_hash(source)
        doc_directory_path = join(source_docs_dir, source_id, 'document')
        doc_file_path = join(doc_directory_path, 'source_doc')
        
        if os.path.exists(doc_file_path):
            return self._open_source_doc(doc_file_path)
        else:
            if not os.path.exists(doc_directory_path):
                os.makedirs(doc_directory_path)
            return self._download_source_doc(source, doc_file_path)
            
    def _save_chunk(self, source_docs_dir, chunk, source):

        chunk = self.id_rewriter([chunk])[0]
                
        source_id = get_hash(source)
        chunks_directory_path = join(source_docs_dir, source_id, 'chunks')
        chunk_file_path = join(chunks_directory_path, chunk.id_)
        
        if not os.path.exists(chunks_directory_path):
            os.makedirs(chunks_directory_path)
            
        with open(chunk_file_path, 'w') as f:
                f.write(chunk.to_json())
    
    def _get_doc_count(self, source_docs_dir):
        """
        Counts the number of document files in the given directory excluding a specific file.

        This method calculates the total number of files in the specified directory while
        excluding a certain file if present (not explicitly specified in this method).
        The result is logged and also returned for further use.

        Args:
            source_docs_dir (str): Path to the directory containing document files.

        Returns:
            int: Total count of document files minus one specific file.

        """
        doc_count = len([name for name in os.listdir(source_docs_dir) if os.path.isfile(name)]) - 1
        logger.info(f'doc_count: {doc_count}')
        return doc_count
    
    def docs(self):
        return self
    
    def _with_page_number(self, metadata, page_number):
        """
        Copies and updates metadata with a page number, if provided.

        This function makes a deep copy of the provided metadata and updates it
        to include a specified `page_number`. If no `page_number` is provided,
        the original metadata is returned unchanged.

        Args:
            metadata (dict): The original metadata to be copied and updated.
            page_number (Union[int, None]): The page number to be added to the
                metadata if provided. Can be `None`.

        Returns:
            dict: A new dictionary containing the original metadata with the
            added page number, or the original metadata unmodified.
        """
        if page_number:
            metadata_copy = copy.deepcopy(metadata)
            metadata_copy['page_number'] = page_number
            return metadata_copy
        else:
            return metadata

    def __iter__(self):
        """
        Returns an iterator that processes and yields knowledge base chunks and corresponding
        source documents as `SourceDocument` objects. This method creates temporary directories
        for Amazon Bedrock Knowledge Base data and processes the data to build a structured
        output including metadata, source documents, and embeddings.

        Yields:
            SourceDocument: Each yielded object contains a reference to the source document and
            its associated chunks of text nodes.

        Raises:
            Any exception raised during directory creation, file I/O operations, or processing
            the knowledge base chunks will propagate through this method.

        Attributes:
            output_dir (str): Directory path where the knowledge base data should be processed.
            include_embeddings (bool): Determines whether to include chunk embeddings in the
                yielded results.
            include_source_doc (bool): Indicates if the source documents should be embedded
                in the output.
            limit (int): The maximum number of `SourceDocument` objects to yield. A value
                less than or equal to zero disables this limit.
        """
        job_dir = join(self.output_dir, 'bedrock-kb-export', f'{uuid.uuid4().hex}')
        
        bedrock_dir = join(job_dir, 'bedrock')
        llama_index_dir = join(job_dir, 'llama-index')
        
        logger.info(f'Creating Amazon Bedrock Knowledge Base temp directories [bedrock_dir: {bedrock_dir}, llama_index_dir: {llama_index_dir}]')

        count = 0
        
        with TempDir(job_dir) as j, TempDir(bedrock_dir) as k, TempDir(llama_index_dir) as s:
        
            for kb_chunk in self._kb_chunks(bedrock_dir):

                bedrock_id = kb_chunk['id']
                page_number = kb_chunk.get('x-amz-bedrock-kb-document-page-number', None)
                metadata = json.loads(kb_chunk['AMAZON_BEDROCK_METADATA'])
                source = metadata['source']
                
                source_doc = self._get_source_doc(llama_index_dir, source)
                
                chunk = TextNode()

                chunk.text = kb_chunk['AMAZON_BEDROCK_TEXT']
                chunk.metadata = metadata
                chunk.metadata['bedrock_id'] = bedrock_id
                if self.include_embeddings:
                    chunk.embedding = kb_chunk['bedrock-knowledge-base-default-vector']
                chunk.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(
                    node_id=source_doc.id_,
                    node_type=NodeRelationship.SOURCE,
                    metadata=source_doc.metadata,
                    hash=source_doc.hash
                )
                
                self._save_chunk(llama_index_dir, chunk, source)
                    
            for d in [d for d in Path(llama_index_dir).iterdir() if d.is_dir()]:
            
                document = None
                
                if self.include_source_doc:
                    source_doc_file_path = join(d, 'document', 'source_doc')
                    with open(source_doc_file_path) as f:
                        document = Document.from_json(f.read())
                 
                file_based_chunks = FileBasedChunks(str(d), 'chunks')
                chunks = [c for c in file_based_chunks.chunks()]
                
                yield SourceDocument(refNode=document, nodes=chunks)
                
                count += 1
                if self.limit > 0 and count >= self.limit:
                    break
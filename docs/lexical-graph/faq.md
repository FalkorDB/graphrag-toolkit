[[Home](./)]

## FAQ

  - [Errors and warnings](#errors-and-warnings)
    - [RuntimeError: Please use nest_asyncio.apply() to allow nested event loops](#runtimeerror-please-use-nest_asyncioapply-to-allow-nested-event-loops)
    - [ModelError: An error occurred (AccessDeniedException) when calling the InvokeModel operation: \<identity\> is not authorized to perform: bedrock:InvokeModel](#modelerror-an-error-occurred-accessdeniedexception-when-calling-the-invokemodel-operation-identity-is-not-authorized-to-perform-bedrockinvokemodel)
    - [ModelError: An error occurred (AccessDeniedException) when calling the InvokeModel operation: You don't have access to the model with the specified model ID](#modelerror-an-error-occurred-accessdeniedexception-when-calling-the-invokemodel-operation-you-dont-have-access-to-the-model-with-the-specified-model-id)
    - [WARNING:graph_store:Retrying query in x seconds because it raised ConcurrentModificationException](#warninggraph_storeretrying-query-in-x-seconds-because-it-raised-concurrentmodificationexception)

### Errors and warnings

#### RuntimeError: Please use nest_asyncio.apply() to allow nested event loops

`nest_asyncio.apply()` provides a convenient solution to enable nested event loops and make it easier to handle complex asynchronous programming situations in Python. All of the code examples in the documentation include `nest_asyncio.apply()`. However, the examples are formatted to be run in a Jupyter notebook. If you’re building an application with a main entry point, you may encounter this runtime error. To fix, put your application logic inside a method, and add an `if __name__ == '__main__'` block:

```python
import os

from graphrag_toolkit.lexical_graph import LexicalGraphIndex
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory

from llama_index.readers.web import SimpleWebPageReader

import nest_asyncio
nest_asyncio.apply()

def run_extract_and_build():

    graph_store = GraphStoreFactory.for_graph_store(
        'neptune-db://my-graph.cluster-abcdefghijkl.us-east-1.neptune.amazonaws.com'
    )
    
    vector_store = VectorStoreFactory.for_vector_store(
        'aoss://https://abcdefghijkl.us-east-1.aoss.amazonaws.com'
    )

    graph_index = LexicalGraphIndex(
        graph_store, 
        vector_store
    )

    doc_urls = [
        'https://docs.aws.amazon.com/neptune/latest/userguide/intro.html',
        'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/what-is-neptune-analytics.html',
        'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-features.html',
        'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-vs-neptune-database.html'
    ]

    docs = SimpleWebPageReader(
        html_to_text=True,
        metadata_fn=lambda url:{'url': url}
    ).load_data(doc_urls)

    graph_index.extract_and_build(docs, show_progress=True)

if __name__ == '__main__':
    run_extract_and_build()
```

---

#### ModelError: An error occurred (AccessDeniedException) when calling the InvokeModel operation: \<identity\> is not authorized to perform: bedrock:InvokeModel

If the AWS Identity and Access Management (IAM) identity under which your application is running does not have permission to invoke an Amazon Bedrock foundation model, you will get an error similar to the following:

```
graphrag_toolkit.errors.ModelError: An error occurred (AccessDeniedException) when calling the InvokeModel operation: <identity> is not authorized to perform: bedrock:InvokeModel on resource: arn:aws:bedrock:us-west-2::foundation-model/anthropic.claude-3-5-haiku-20241022-v1:0 because no identity-based policy allows the bedrock:InvokeModel action [Model config: {"system_prompt": null, "pydantic_program_mode": "default", "model": "anthropic.claude-3-5-haiku-20241022-v1:0", "temperature": 0.0, "max_tokens": 4096, "context_size": 200000, "profile_name": null, "max_retries": 10, "timeout": 60.0, "additional_kwargs": {}, "class_name": "Bedrock_LLM"}]
```

To fix, ensure you have [enabled access](https://docs.aws.amazon.com/bedrock/latest/userguide/model-access.html) to the appropriate foundation models in Amazon Bedrock, and then update the IAM policy associated with the identity:

```
{
    "Effect": "Allow",
    "Action": [
        "bedrock:InvokeModel"
    ],
    "Resource": [
        "arn:aws:bedrock:us-west-2::foundation-model/anthropic.claude-3-5-haiku-20241022-v1:0"
    ]
}
```

---

#### ModelError: An error occurred (AccessDeniedException) when calling the InvokeModel operation: You don't have access to the model with the specified model ID

Access to Amazon Bedrock foundation models isn't granted by default. If you have not enabled access to a foundation model, you will get an error similar to the following:

```
graphrag_toolkit.errors.ModelError: An error occurred (AccessDeniedException) when calling the InvokeModel operation: You don't have access to the model with the specified model ID. [Model config: {"system_prompt": null, "pydantic_program_mode":"default", "model": "anthropic.claude-3-7-sonnet-20250219-v1:0", "temperature": 0.0, "max_tokens": 4096, "context_size": 200000, "profile_name": null, "max_retries": 10, "timeout": 60.0, "additional_kwargs": {}, "class_name": "Bedrock_LLM"}]
```

To fix,  [enable access](https://docs.aws.amazon.com/bedrock/latest/userguide/model-access.html) to the appropriate foundation models in Amazon Bedrock, and then [grant IAM permissions to the model](#modelerror-an-error-occurred-accessdeniedexception-when-calling-the-invokemodel-operation-identity-is-not-authorized-to-perform-bedrockinvokemodel).

---

#### WARNING:graph_store:Retrying query in x seconds because it raised ConcurrentModificationException

While indexing data in Amazon Neptune Database, Neptune can sometimes issue a `ConcurrentModificationException`. This occurs because multiple workers are attempting to [update the same set of vertices](https://docs.aws.amazon.com/neptune/latest/userguide/transactions-exceptions.html). The GraphRAG Toolkit automatically retries transactionsb that are cancelled because of a `ConcurrentModificationException`. If the maximum number of retries is exceeded and the indexing fails, consider reducing the number of workers in the build stage using [`GraphRAGConfig.build_num_workers`](./configuration.md#graphragconfig).

---

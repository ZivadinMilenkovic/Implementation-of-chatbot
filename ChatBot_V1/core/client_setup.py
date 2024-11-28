from databricks.connect import DatabricksSession


def get_the_client(client, messages, max_token, temperature):
    completions_response = client.predict(
        endpoint="/serving-endpoints/databricks-meta-llama-3-1-70b-instruct",
        inputs={
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_token,
            "n": 1
        }
    )
    return completions_response

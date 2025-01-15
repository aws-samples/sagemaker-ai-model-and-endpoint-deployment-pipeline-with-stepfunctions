import boto3
import logging
import json
import os

# setting up logger
logger = logging.getLogger()
handler = logging.StreamHandler()
logger.addHandler(handler)
logger.setLevel(os.environ["LOG_LEVEL"])

# global
region = os.environ.get("region", "us-east-1")

# clients
ssm_client = boto3.client("ssm", region_name=region)


def list_ssm_parameters(prefix: str) -> dict:
    """
    This function lists all the ssm parameters in the given prefix.
    This will tell us if there are ssm parameters created for endpoints/models that have been removed from the dag, so we can remove them from the parameters.
    """
    response = ssm_client.get_parameters_by_path(
        Path=prefix, Recursive=True, WithDecryption=True
    )
    return response


def lambda_handler(event: dict, context: object) -> dict:
    """
    This function is the main handler for the lambda function.
    It is triggered when there are model or endpoint updates, the event is sent through a step function state machine.
    """
    logger.debug(f"event: {event}")
    execution_graph = event["execution_graph"]
    endpoint_params = []
    for key, value in execution_graph.items():
        endpoint_list = list_ssm_parameters(f"/{key}/")
        for end in value:
            logger.info(f"endpoint name: {end}")
            e_name = end["endpoint_name"]
            e_type = end["endpoint_type"]
            multi_container_endpoint = end.get(
                "multi_container_endpoint", False
            )
            logger.info(f"endpoint name: {e_name}")
            
            logger.info(f"endpoint type: {e_type}")
            logger.info(f"multi_container_endpoint: {multi_container_endpoint}")
            if e_type == "async" or (e_type == "real-time" and not multi_container_endpoint):
                endpoint_param = f"/{key}/{e_type}/{e_name}"
            else:
                c_name = end["container_name"]
                logger.info(f"container name: {c_name}")
                endpoint_param = f"/{key}/{e_type}/{e_name}/{c_name}"

            endpoint_params.append(endpoint_param)
        for endpoint in endpoint_list["Parameters"]:
            logger.info(f"endpoint: {endpoint}")
            logger.info(f"value: {value}")
            if endpoint["Name"] in endpoint_params:
                logger.info(f"endpoint {endpoint['Name']} exists in Model DAG")
            else:
                logger.info(f"endpoint {endpoint['Name']} does not exist in Model DAG")
                ssm_client.delete_parameter(Name=endpoint["Name"])
            
    return {"statusCode": 200, "body": "Model Dags Updated Successfully."}


if __name__ == "__main__":
    with open("src/build_sm_model/model_containers.json", "r") as f:
        json_data = json.load(f)

    response = lambda_handler(event=json_data, context=None)

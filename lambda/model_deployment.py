from datetime import datetime, timezone
import boto3
import logging
import json
import os

# setting up logger
logger = logging.getLogger()
handler = logging.StreamHandler()
logger.addHandler(handler)
logger.setLevel(os.environ["LOG_LEVEL"])

# globals
region = os.environ.get("region", "us-east-1")
model_metadata_bucket_name = os.environ["model_metadata_bucket_name"]
kms_key_arn = os.environ["kms_key_arn"]
sm_execution_role_arn = os.environ["sm_execution_role_arn"]

# clients
sagemaker_client = boto3.client("sagemaker", region_name=region)
ssm_client = boto3.client("ssm", region_name=region)
kms_client = boto3.client("kms", region_name=region)
s3_client = boto3.client("s3", region_name=region)


def get_model_card_json_s3(object_key: str) -> dict:
    """
    This method takes an S3 location as input, and returns the json object stored at that location.
    """
    logger.info(f"Getting model card json from S3: {object_key}")
    response = s3_client.get_object(Bucket=model_metadata_bucket_name, Key=object_key)
    model_card_json = json.loads(response["Body"].read().decode("utf-8"))
    return model_card_json


def describe_model_card(model_card_name: str) -> bool:
    """
    This method takes a model card name as input, and returns True if a model card with that name exists, False otherwise.
    """
    try:
        response = sagemaker_client.describe_model_card(ModelCardName=model_card_name)
        logger.debug(f"Model card {model_card_name} already exists: {response}")
        return True
    except sagemaker_client.exceptions.ResourceNotFound as e:
        logger.debug(
            f"Model card {model_card_name} does not exist, need to create a model card"
        )
        return False


def update_model_card_json(unique_model_name: str, model_card_json: dict) -> dict:
    """
    This method updates the model card json with the unique model name.
    """
    model_card_json["model_overview"]["model_name"] = unique_model_name
    return model_card_json


def create_update_model_card(
    model_name: str, model_card_json_key: str, unique_model_name: str
) -> str:
    """
    Creates or updates SM model card.
    Args:
        model_name: the name of the model
        model_card_json_key: the key of the model card json file in S3
        kms_key_arn: the ARN of the KMS key to use
        unique_model_name: the unique name of the model
    Returns:
        the string representation of the ARN
    """

    model_card_exists = describe_model_card(model_card_name=model_name)
    if model_card_exists:
        logger.debug(f"Model card {model_name} already exists, updating model card.")
        new_model_content = get_model_card_json_s3(object_key=model_card_json_key)
        updated_model_content = update_model_card_json(
            unique_model_name=unique_model_name, model_card_json=new_model_content
        )
        model_card_arn = sagemaker_client.update_model_card(
            ModelCardName=model_name,
            ModelCardStatus="Draft",
            Content=json.dumps(updated_model_content),
        )["ModelCardArn"]
        logger.info(f"Updated model card {model_card_arn}")
    else:
        logger.debug(
            f"Model card {model_name} does not exist, creating new model card."
        )
        new_model_content = get_model_card_json_s3(object_key=model_card_json_key)
        updated_model_content = update_model_card_json(
            unique_model_name=unique_model_name, model_card_json=new_model_content
        )
        model_card_arn = sagemaker_client.create_model_card(
            ModelCardName=model_name,
            SecurityConfig={"KmsKeyId": kms_key_arn},
            ModelCardStatus="Draft",
            Content=json.dumps(updated_model_content),
        )["ModelCardArn"]
        logger.info(f"Created model card {model_card_arn}")
    return model_card_arn


def create_model(
    model_name: str,
    container_list: list,
    role_arn: str,
    execution_type: str,
) -> tuple[str, str]:
    """
    Creates sagemaker model.
    Writes model ARN to SSM.
    Args:
        model_name: the name of the model
        container_list: a list of the containers in the inference pipeline
            [{
            "container_name": the container name,
            "container_image_url": the container image url,
            "s3_data_source_url": the s3 data source url [optional], (this URL can store additional artifacts needed for the model)
            },
            ...
            ]
        role_arn: the role arn to use for the model creation.
        execution_type: Either Serial or Direct, specifying if this model should be invoked as containers in serial as an
            inference pipeline, or directly as individual containers
    Returns:
        unique_model_name: the name of this specific model in sagemaker - appended with a timestamp
        model_arn: the arn of the created model.
    Raises:
        None.
    """

    date = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d-%H-%M-%S")
    unique_model_name = f"{model_name}-{date}"

    containers = []
    for i in container_list:
        try:
            i["s3_data_source_url"]
            containers.append(
                {
                    "ContainerHostname": i["container_name"],
                    "Image": i["container_image_url"],
                    "ModelDataUrl": i["s3_data_source_url"],
                }
            )
        except KeyError:
            containers.append(
                {
                    "ContainerHostname": i["container_name"],
                    "Image": i["container_image_url"],
                }
            )

    try:
        if execution_type == "None":
            model_arn = sagemaker_client.create_model(
                ModelName=unique_model_name,
                ExecutionRoleArn=role_arn,
                Containers=containers,
                EnableNetworkIsolation=True,  # no inbound or outbound network calls can be made to or from the container
            )["ModelArn"]
        else:
            model_arn = sagemaker_client.create_model(
                ModelName=unique_model_name,
                ExecutionRoleArn=role_arn,
                Containers=containers,
                InferenceExecutionConfig={
                    "Mode": execution_type
                },  # Specify the mode based on the endpoint's requirements
                EnableNetworkIsolation=True,  # no inbound or outbound network calls can be made to or from the container
            )["ModelArn"]

        return unique_model_name, model_arn
    except Exception as e:
        logger.error(f"error: {e}")
        raise Exception("SageMaker Model Not Created Successfully")


def write_model_name(model_name: str, unique_model_name: str) -> None:
    """
    Writes the unique model name to SSM. This is used to track the latest model.
    Args:
        model_name: the name of the model
        unique_model_name: the unique name of the model in sagemaker
    Returns:
        None.
    Raises:
        None.
    """
    ssm_client.put_parameter(
        Name=f"models-{model_name}",
        Type="String",
        Value=unique_model_name,
        Overwrite=True,
    )
    return None


def lambda_handler(event: dict, context: object) -> dict:
    """
    This handles model deployment for new models, and updating models.
    """
    logger.debug(f"event: {event}")
    model_name = event["model_name"]
    containers_list = event["container_list"]
    model_card_json_key = event["model_card_json_s3_object_key"]
    execution_type = event.get("execution_type", "None")
    endpoint_type = event.get("endpoint_type", "real-time").lower()
    if endpoint_type not in ["real-time", "async"]:
        raise ValueError("endpoint_type must be 'real-time' or 'async'")

    # create model
    unique_model_name, model_arn = create_model(
        model_name=model_name,
        container_list=containers_list,
        role_arn=sm_execution_role_arn,
        execution_type=execution_type,
    )
    logger.info(f"Sagemaker Model {unique_model_name} created with arn {model_arn}")
    # write model name to SSM
    write_model_name(model_name=model_name, unique_model_name=unique_model_name)
    logger.info(
        f"Wrote sagemaker model name and endpoint name to SSM as the latest version of the {model_name} model."
    )
    model_card_arn = create_update_model_card(
        model_name=model_name,
        model_card_json_key=model_card_json_key,
        unique_model_name=unique_model_name,
    )
    logger.info(
        f"Created or updated model card for {model_name} with arn {model_card_arn}"
    )

    return event


if __name__ == "__main__":
    with open("model_containers.json", "r") as f:
        json_data = json.load(f)

    response = lambda_handler(event=json_data, context=None)
    logger.debug(f"\nresponse: {response}")

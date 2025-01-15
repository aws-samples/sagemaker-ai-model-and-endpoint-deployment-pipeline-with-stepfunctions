from datetime import datetime, timezone
import boto3
import logging
import os

# setting up logger
logger = logging.getLogger()
handler = logging.StreamHandler()
logger.addHandler(handler)
logger.setLevel(os.environ["LOG_LEVEL"])


# globals
region = os.environ.get("region", "us-east-1")
model_output_bucket_name = os.environ.get("model_output_bucket_name")
kms_key_id = os.environ.get("kms_key_id")

# clients
sagemaker_client = boto3.client("sagemaker", region_name=region)
ssm_client = boto3.client("ssm", region_name=region)
appautoscaling_client = boto3.client("application-autoscaling", region_name=region)
kms_client = boto3.client("kms", region_name=region)


def describe_endpoint(endpoint_name: str) -> str:
    """
    This function returns the status of the given endpoint. Allows us to update an endpoint if it already exists, or create one if it does not exist
    """
    try:
        response = sagemaker_client.describe_endpoint(EndpointName=endpoint_name)
        logger.info(response["EndpointStatus"])
        return response["EndpointStatus"]
    except:
        logger.info("endpoint does not exist")
        return "DNE"  # does not exist


def describe_scalable_targets(model_item: dict) -> dict:
    """
    This function returns the scalable targets for the given endpoint.
    """
    prod_variants = model_item["variant_list"]
    production_variant = prod_variants[0]
    e_name = model_item["endpoint_name"]
    variant_name = production_variant["variant_name"]
    resource_id = f"endpoint/{e_name}/variant/{variant_name}"
    response = appautoscaling_client.describe_scalable_targets(
        ServiceNamespace="sagemaker",
        ResourceIds=[
            resource_id,
        ],
    )
    return response


def create_endpoint(
    model_name: str, model_item: dict, unique_endpoint_config_name: str
) -> dict:
    """
    This function creates an endpoint for the given model.
    """

    endpoint_name = model_item["endpoint_name"]
    logger.info(f"endpoint name: {endpoint_name}")

    status = describe_endpoint(endpoint_name)
    logger.info(f"status: {status}")

    if status == "InService":
        logger.info("updating endpoint")
        # before updating the endpoint, need to check if there are scalable targets previously registered to the endpoint variants, and deregister
        response = describe_scalable_targets(model_item)
        if "ScalableTargets" in response:
            for scalable_target in response["ScalableTargets"]:
                resource_id = scalable_target["ResourceId"]
                response = appautoscaling_client.deregister_scalable_target(
                    ServiceNamespace="sagemaker",
                    ResourceId=resource_id,
                    ScalableDimension="sagemaker:variant:DesiredInstanceCount",
                )
                logger.info(response)
                logger.debug(f"deregister_scalable_target response: {response}")
        response = sagemaker_client.update_endpoint(
            EndpointName=endpoint_name,
            EndpointConfigName=unique_endpoint_config_name,
        )
        logger.debug(f"update_endpoint response: {response}")
        return response
    elif status == "DNE":
        logger.info("creating endpoint")
        response = sagemaker_client.create_endpoint(
            EndpointName=endpoint_name,
            EndpointConfigName=unique_endpoint_config_name,
            Tags=[
                {
                    "Key": "Name",
                    "Value": f"SageMaker Endpoint for {model_name}",
                },
            ],
        )
        logger.debug(f"create_endpoint response: {response}")
        return response
    elif status == "Creating":
        raise Exception("endpoint is creating")
    elif status == "Updating":
        raise Exception("endpoint is still updating")
    else:
        raise Exception(f"endpoint status: {status}")


def get_latest_model_name(model_name: str) -> str:
    """
    This function returns the latest model name for the given model, from the stored ssm parameters
    """
    latest_model_name = ssm_client.get_parameter(
        Name=f"models-{model_name}", WithDecryption=True
    )["Parameter"]["Value"]
    return latest_model_name


def set_async_inf_config(model_name: str, model_item: dict) -> dict:
    """
    This functions sets the async inference config for the given endpoint.
    """
    prod_variants = model_item["variant_list"]
    production_variant = prod_variants[0]
    variant_name = production_variant["variant_name"]
    container_name = model_item["container_list"][0]["container_name"]
    s3_output_path = (
        f"s3://{model_output_bucket_name}/inferred/{container_name}/variants/{variant_name}"
    )
    logger.info(f"s3_output_path: {s3_output_path}")
    async_inference_config = {"OutputConfig": {"S3OutputPath": s3_output_path}}
    return async_inference_config


def get_unique_endpoint_config_name(endpoint_name: str) -> str:
    """
    This function returns a unique endpoint config name for the given endpoint.
    """
    date = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d-%H-%M-%S")
    unique_endpoint_config_name = f"{endpoint_name}-{date}"
    return unique_endpoint_config_name


def get_kms_key_id(sagemaker_key_alias: str) -> str:
    """
    This function returns the kms key id for the given sagemaker key alias.
    """

    logger.info(f"sagemaker_key_alias: {sagemaker_key_alias}")
    response = kms_client.describe_key(
        KeyId=sagemaker_key_alias,
    )
    return response["KeyMetadata"]["KeyId"]


def create_endpoint_config(model_name: str, model_item: dict, key_id: str) -> str:
    """
    This function creates the endpoint configuration that is needed to create the endpoint.
    """

    endpoint_type = model_item["endpoint_type"]
    logger.info(f"endpoint type: {endpoint_type}")
    logger.info(f"model_name: {model_name}")
    logger.info(f"model_item: {model_item}")
    endpoint_name = model_item["endpoint_name"]
    logger.info(f"endpoint name: {endpoint_name}")

    min_capacity = model_item["min_capacity"]

    unique_endpoint_config_name = get_unique_endpoint_config_name(endpoint_name)
    logger.info(f"unique endpoint config name: {unique_endpoint_config_name}")

    prod_variants = model_item["variant_list"]
    if endpoint_type == "async":
        # async endpoints can only have one prod variant, so just get first item in list
        if len(prod_variants) != 1:
            raise Exception(
                "Incorrect number of prod variants for an async endpoint, can only be 1 prod variant"
            )
        async_inference_config = set_async_inf_config(
            model_name=model_name,
            model_item=model_item,
        )

        production_variant = prod_variants[0]
        latest_model_name = get_latest_model_name(
            production_variant["variant_model_name"]
        )
        logger.info(f"latest model name: {latest_model_name}")

        production_variants = [
            {
                "VariantName": production_variant["variant_name"],
                "ModelName": latest_model_name,
                "InitialInstanceCount": production_variant["variant_instance_count"],
                "InitialVariantWeight": production_variant["variant_instance_weight"],
                "InstanceType": production_variant["variant_instance_type"],
            }
        ]
        response = sagemaker_client.create_endpoint_config(
            EndpointConfigName=unique_endpoint_config_name,
            ProductionVariants=production_variants,
            AsyncInferenceConfig=async_inference_config,
            KmsKeyId=key_id,
        )
    elif endpoint_type == "real-time":
        if min_capacity < 1:
            raise Exception(
                "min_capacity for a real time endpoint needs to be greater than or equal to 1"
            )
        production_variants = []
        if len(prod_variants) < 1 or len(prod_variants) > 10:
            raise Exception(
                "Incorrect number of prod variants for an real time endpoint, needs to be between 1 and 10"
            )
        for variant in prod_variants:
            latest_model_name = get_latest_model_name(variant["variant_model_name"])
            logger.info(f"latest model name: {latest_model_name}")
            production_variants.append(
                {
                    "VariantName": variant["variant_name"],
                    "ModelName": latest_model_name,
                    "InitialInstanceCount": variant["variant_instance_count"],
                    "InitialVariantWeight": variant["variant_instance_weight"],
                    "InstanceType": variant["variant_instance_type"],
                }
            )
        response = sagemaker_client.create_endpoint_config(
            EndpointConfigName=unique_endpoint_config_name,
            ProductionVariants=production_variants,
            KmsKeyId=key_id,
        )
    logger.debug(f"create_endpoint_config response: {response}")

    return unique_endpoint_config_name


def lambda_handler(event: dict, context: object) -> dict:
    """
    This function is triggered when there model updates, or endpoint config updates, it is triggered through a step functions state machine workflow
    It creates the endpoint for the given model.
    """
    logger.debug(f"event: {event}")
    model_item = event
    model_name = event["model_name"]
    unique_endpoint_config_name = create_endpoint_config(
        model_name=model_name, model_item=model_item, key_id=kms_key_id
    )
    create_endpoint(
        model_name=model_name,
        model_item=model_item,
        unique_endpoint_config_name=unique_endpoint_config_name,
    )

    return event

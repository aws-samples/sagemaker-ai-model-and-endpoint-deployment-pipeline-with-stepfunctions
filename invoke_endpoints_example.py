import boto3
import json

sagemaker = boto3.client("sagemaker-runtime")
ssm = boto3.client("ssm")
s3 = boto3.client("s3")


def list_ssm_parameters(prefix: str) -> dict:
    """
    This function lists the SSM parameters with the given prefix.
    Allows us to dynamically get which endpoints need to be invoked based on the step of the inference pipeline.
    """
    response = ssm.get_parameters_by_path(
        Path=prefix, Recursive=True, WithDecryption=True
    )
    return response


def sm_invoke_endpoint_async(
    endpoint_name: str, bucket_name: str, object_key: str
) -> dict:
    """
    This function invokes the given SageMaker endpoint asynchronously, and returns the response
    """
    response = sagemaker.invoke_endpoint_async(
        ContentType="application/json",
        EndpointName=endpoint_name,
        InputLocation=f"s3://{bucket_name}/{object_key}",
        InvocationTimeoutSeconds=3600,
    )
    return response


def sm_invoke_endpoint_real_time(
    endpoint_name: str,
    target_container_name: str,
    bucket_name: str,
    object_key: str,
) -> dict:
    """
    This function invokes the given real time SageMaker endpoint, and returns the response
    """
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    object_data = response["Body"].read().decode("utf-8")
    input_data = json.loads(object_data)
    if target_container_name == "None":
        # Invoke the endpoint without TargetContainerHostname, because this real-time endpoint only has one container
        response = sagemaker.invoke_endpoint(
            EndpointName=endpoint_name,
            ContentType="application/json",
            Body=json.dumps(input_data),
        )
    else:
        # this invokes endpoints that host multi container sagemaker models
        response = sagemaker.invoke_endpoint(
            ContentType="application/json",
            EndpointName=endpoint_name,
            Body=json.dumps(input_data),
            TargetContainerHostname=target_container_name,
        )
    return response


def invoke_endpoints(
    endpoint_list: dict, bucket_name: str, input_object_key: str
) -> None:
    """
    This function invokes the given SageMaker endpoints.
    """

    for endpoint in endpoint_list["Parameters"]:
        # get endpoint name from ssm
        endpoint_name_model_dependency = endpoint["Name"]
        parts = endpoint_name_model_dependency.split("/")
        if "async" in endpoint_name_model_dependency:
            # for async, there is no model container to invoke, so the endpoint name is the last part of ssm param after the dependency in the ssm param
            endpoint_name = parts[-1]
            response = sm_invoke_endpoint_async(
                endpoint_name=endpoint_name,
                bucket_name=bucket_name,
                object_key=input_object_key,
            )
            print(response)
        elif "real-time" in endpoint_name_model_dependency:
            if len(parts) == 4:
                # only one container on real-time endpoint, do not need to include container name in invoke api call
                container_name = "None"
                endpoint_name = parts[-1]
            else:
                # this is a multi-container real-time endpoint, so need to include container name in the api call
                endpoint_name = parts[-2]
                container_name = parts[-1]

            response = sm_invoke_endpoint_real_time(
                endpoint_name=endpoint_name,
                target_container_name=container_name,
                bucket_name=bucket_name,
                object_key=input_object_key,
            )
            print(response)


ssm_prefix = "/<Replace with your SSM Parameter Prefix>/"
endpoint_list = list_ssm_parameters(ssm_prefix)

bucket_name = "<Replace with the Name of the Bucket where the input data to your model is stored>"
object_key = "<Replace with the Object Key of the Input Data in the S3 Bucket>"
invoke_endpoints(
    endpoint_list=endpoint_list,
    bucket_name=bucket_name,
    input_object_key=object_key,
)
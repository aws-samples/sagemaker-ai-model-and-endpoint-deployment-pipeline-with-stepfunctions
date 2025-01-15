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

# clients
sagemaker_client = boto3.client("sagemaker", region_name=region)
ssm_client = boto3.client("ssm", region_name=region)
appautoscaling_client = boto3.client("application-autoscaling", region_name=region)
cloudwatch_client = boto3.client("cloudwatch", region_name=region)


def check_if_ssm_param_exists(name: str) -> bool:
    """
    This function checks if a ssm parameter exists.
    If it does not exist, it returns False.
    If it does exist, it returns True.
    This will allow us to create parameters for new endpoints
    """
    try:
        ssm_client.get_parameter(Name=name, WithDecryption=True)
        return True
    except:
        logger.info(f"ssm parameter {name} not found")
        return False


def create_ssm_parameter(name: str, value: str) -> None:
    """
    This function creates a ssm parameter. For new endpoints
    """
    try:
        ssm_client.put_parameter(Name=name, Value=value, Type="String")
    except Exception as e:
        logger.error(f"error creating ssm parameter {name}")
        logger.error("error: ", e)


def get_endpoint_exists(endpoint_name: str) -> bool:
    """
    This function checks if an endpoint exists.
    If it does not exist, or it is not InService, it returns False.
    If it does exist, and it is InService, it returns True.
    """
    try:
        response = sagemaker_client.describe_endpoint(EndpointName=endpoint_name)
        state = response["EndpointStatus"]
        if state == "InService":
            # endpoint is in service, which means a ssm parameter can be created for it
            return True
        else:
            # endpoint is not in service, which means a ssm parameter cannot be created for it
            ## TO DO: add functionality or error handling for endpoints that are not in service.
            ## there are a lot of states that the endpoint can be in, so we need to handle them
            ## for now, we will just return False
            return False
    except Exception as e:
        # endpoint does not exist, not ssm parameter can be created for it
        return False


def get_latest_model_name(model_name: str) -> str:
    """
    This function returns the latest model name for the given model, from the stored ssm parameters
    """
    latest_model_name = ssm_client.get_parameter(
        Name=f"models-{model_name}", WithDecryption=True
    )["Parameter"]["Value"]
    return latest_model_name


def describe_scaling_policies(resource_id: str) -> dict:
    """
    This function returns the scaling policies for the given resource id.
    """
    response = appautoscaling_client.describe_scaling_policies(
        ServiceNamespace="sagemaker", ResourceId=resource_id
    )
    return response


def delete_scaling_policy(
    resource_id: str, policy_name: str, scalable_dimension: str, namespace: str
) -> None:
    """
    This function deletes the scaling policy for the given resource id.
    """
    response = appautoscaling_client.delete_scaling_policy(
        PolicyName=policy_name,
        ServiceNamespace="sagemaker",
        ResourceId=resource_id,
        ScalableDimension=scalable_dimension,
    )
    logger.debug(f"delete_scaling_policy response: {response}")
    return


def create_auto_scaling_target(model_item: dict) -> str:
    """
    This function creates the auto scaling target for the given endpoint.
    """
    # the first variant should be the main variant, and this will become the autoscaling target, if endpoint has no variants, the deployment would have failed in the previous step
    prod_variants = model_item["variant_list"]
    production_variant = prod_variants[0]
    e_name = model_item["endpoint_name"]
    variant_name = production_variant["variant_name"]
    resource_id = f"endpoint/{e_name}/variant/{variant_name}"
    response = appautoscaling_client.register_scalable_target(
        ServiceNamespace="sagemaker",
        ResourceId=resource_id,
        ScalableDimension="sagemaker:variant:DesiredInstanceCount",
        MinCapacity=model_item["min_capacity"],
        MaxCapacity=model_item["max_capacity"],
    )
    logger.debug(f"create_auto_scaling_target response: {response}")
    return resource_id


def create_target_tracking_policy(model_item: dict, resource_id: str) -> None:
    """
    This function creates the target tracking policy for the given endpoint.
    """
    # check if endpoint already has a target tracking policy
    m_name = model_item["model_name"]
    policy_name = f"target-scaling-{m_name}"
    scaling_policies = describe_scaling_policies(resource_id=resource_id)
    logger.info("checking policies for matching")
    for policy in scaling_policies["ScalingPolicies"]:
        logger.info(f"policy: {policy}")
        if policy["PolicyName"] == policy_name:
            logger.info("target tracking policy already exists")
            scalableDimension = policy["ScalableDimension"]
            namespace = policy["ServiceNamespace"]
            delete_scaling_policy(
                resource_id=resource_id,
                policy_name=policy_name,
                scalable_dimension=scalableDimension,
                namespace=namespace,
            )
    # create new target tracking policy
    response = appautoscaling_client.put_scaling_policy(
        PolicyName=policy_name,
        ServiceNamespace="sagemaker",
        ResourceId=resource_id,
        ScalableDimension="sagemaker:variant:DesiredInstanceCount",
        PolicyType="TargetTrackingScaling",
        TargetTrackingScalingPolicyConfiguration={
            "TargetValue": 5,
            "CustomizedMetricSpecification": {
                "MetricName": "ApproximateBacklogSizePerInstance",
                "Namespace": "AWS/SageMaker",
                "Statistic": "Average",
                "Dimensions": [
                    {"Name": "EndpointName", "Value": model_item["endpoint_name"]}
                ],
            },
        },
    )
    logger.debug(f"create_target_tracking_policy response: {response}")
    return


def create_step_scaling_policy(model_item: dict, resource_id: str) -> str:
    """
    This function creates the step scaling policy for the given endpoint.
    """
    try:
        # check if endpoint already has a step scaling policy
        m_name = model_item["model_name"]
        policy_name = f"HasBacklogWithoutCapacity-{m_name}"
        scaling_policies = describe_scaling_policies(resource_id=resource_id)
        logger.info("checking policies for matching")
        for policy in scaling_policies["ScalingPolicies"]:
            logger.info(f"policy: {policy}")
            if policy["PolicyName"] == policy_name:
                logger.info("step scaling policy already exists")
                scalableDimension = policy["ScalableDimension"]
                namespace = policy["ServiceNamespace"]
                delete_scaling_policy(
                    resource_id=resource_id,
                    policy_name=policy_name,
                    scalable_dimension=scalableDimension,
                    namespace=namespace,
                )
        # create new step scaling policy
        response = appautoscaling_client.put_scaling_policy(
            PolicyName=policy_name,
            ServiceNamespace="sagemaker",
            ResourceId=resource_id,
            ScalableDimension="sagemaker:variant:DesiredInstanceCount",
            PolicyType="StepScaling",
            StepScalingPolicyConfiguration={
                "AdjustmentType": "ChangeInCapacity",
                "Cooldown": 300,
                "MetricAggregationType": "Average",
                "StepAdjustments": [
                    {"MetricIntervalLowerBound": 0, "ScalingAdjustment": 1}
                ],
            },
        )
        logger.debug(f"create_step_scaling_policy response: {response}")
        return response["PolicyARN"]
    except Exception as e:
        logger.error("error in create_step_scaling_policy: ", e)
        return "False"


def create_cloud_watch_alarm(model_item: dict, policy_arn: str) -> None:
    """
    This function creates the cloud watch alarm for the given endpoint.
    """
    # setting clients
    latest_model_name = get_latest_model_name(model_item["model_name"])
    response = cloudwatch_client.put_metric_alarm(
        AlarmName=f"sagemaker-step-scaling-{latest_model_name}",
        MetricName="HasBacklogWithoutCapacity",
        Namespace="AWS/SageMaker",
        Statistic="Average",
        EvaluationPeriods=2,
        DatapointsToAlarm=2,
        Threshold=1,
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        TreatMissingData="missing",
        Dimensions=[{"Name": "EndpointName", "Value": model_item["endpoint_name"]}],
        Period=60,
        AlarmActions=[policy_arn],
    )
    logger.debug(f"create_cloud_watch_alarm response: {response}")
    return


def lambda_handler(event: dict, context: object) -> dict:
    """
    This function is the main handler for the lambda function.
    It is triggered when there are model or endpoint updates, the event is sent through a step function state machine.
    This function is to add ssm parameter for each of the newly created endpoints, so the inference lambda can invoke them.
    This function also adds or updates the scaling of the endpoint based on the model configuration file.
    """
    logger.debug(f"event: {event}")
    if "statusCode" in event:
        status_code = event["statusCode"]
        if status_code == 500:
            raise Exception("Error from previous lambda")

    endpoint_type = event["endpoint_type"]
    logger.info(f"endpoint_type: {endpoint_type}")
    endpoint_name = event["endpoint_name"]
    logger.info(f"endpoint_name: {endpoint_name}")
    container_list = event["container_list"]
    for container_obj in container_list:
        container_name = container_obj["container_name"]
        dependency = container_obj["dependency"]
        logger.info(f"container_name: {container_name}")
        if endpoint_type == "async" or len(container_list) == 1:
            # if endpoint is asnychronous or real-time with only one container, do not need to specify container name
            ssm_param = f"/{dependency}/{endpoint_type}/{endpoint_name}"
        else:
            # if a real time endpoint with multiple containers, need to specify the target container when they are invoked, so we need to store the target container as an ssm parameter
            ssm_param = (
                f"/{dependency}/{endpoint_type}/{endpoint_name}/{container_name}"
            )
        logger.info(f"ssm_param: {ssm_param}")
        # check if the model exists in the dag i.e. there is a ssm param for it
        ssm_param_exists = check_if_ssm_param_exists(ssm_param)
        logger.info(f"ssm_param_exists: {ssm_param_exists}")
        if not ssm_param_exists:
            # first check if the endpoint exists in sagemaker
            endpoint_status = get_endpoint_exists(endpoint_name)
            logger.info(f"endpoint_status: {endpoint_status}")
            if endpoint_status:
                # if endpoint exists, create ssm param
                logger.info(f"creating ssm parameter {ssm_param}")
                create_ssm_parameter(ssm_param, endpoint_name)
            else:
                # if endpoint does not exist, or not in service, do not create ssm param
                # need to throw an error so this lambda can be retried
                raise Exception(
                    "Endpoint is not in service, need to wait and retry this lambda, so can add ssm parameter"
                )

        endpoint_status = get_endpoint_exists(endpoint_name)
        logger.info(f"endpoint_status: {endpoint_status}")
        model_item = event

        # if endpoint is in service, create auto scaling target, target tracking policy, step scaling policy, and cloud watch alarm
        if endpoint_status:
            # create auto scaling target
            try:
                logger.info("auto scaling target creation")
                resource_id = create_auto_scaling_target(model_item=model_item)
                logger.info("target tracking policy creation")
                create_target_tracking_policy(
                    model_item=model_item, resource_id=resource_id
                )
                if model_item["endpoint_type"] == "async":
                    # create step scaling policy, only for async, because this policy and alarm allow the async endpoints to scale up from zero instances
                    logger.info("step scaling policy creation")
                    policy_arn = create_step_scaling_policy(
                        model_item=model_item, resource_id=resource_id
                    )
                    if policy_arn != "False":
                        # create cloud watch alarm
                        logger.info("cloud watch alarm creation")
                        create_cloud_watch_alarm(
                            model_item=model_item, policy_arn=policy_arn
                        )
            except Exception as e:
                logger.error("error in lambda_handler adding auto scaling: ", e)
                raise Exception(e)

        # if endpoint is not in service, throw error so can retry this lambda and add autoscaling target
        else:
            raise Exception(
                "Endpoint is not in service, need to wait and retry this lambda so can add auto scaling"
            )

    return event

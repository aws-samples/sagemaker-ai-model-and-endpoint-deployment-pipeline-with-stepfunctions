import { Duration, Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as cdk from 'aws-cdk-lib';
import * as stepfunctions from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';

export class SagemakerModelAndEndpointDeploymentPipelineStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);
    // -------- GLOBALS
    const region = 'us-east-1'
    const LOG_LEVEL = 'INFO'
    //  ---------- ROLES
    const deploymentLambdaPolicies = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        "s3:GetObject",
        "s3:PutObject",
        "kms:Decrypt",
        "kms:GenerateDataKey",
        "kms:DescribeKey",
        "ssm:GetParameter",
        "ssm:PutParameter",
        "ssm:DeleteParameter",
        "ssm:GetParametersByPath",
      ],
      resources: ['*'],
    });
    const deploymentLambdaRole = new iam.Role(this, 'deploymentLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      inlinePolicies: {
        rekognitionPolicy: new iam.PolicyDocument({
          statements: [deploymentLambdaPolicies],
        }),
      },
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSageMakerFullAccess'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ]
    });
    deploymentLambdaRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')
    );
    const sagemakerRolePolicies = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        's3:*',
        'lambda:*',
        'cloudwatch:*',
        'iam:*',
        'kms:*',
        'sagemaker:*',
        'ecr:*',
        'iam:*',
        'application-autoscaling:*',
        'ssm:*'
      ],
      resources: ['*'],
    });
    const sagemakerRole = new iam.Role(this, 'sagemakerRole', {
      assumedBy: new iam.ServicePrincipal('sagemaker.amazonaws.com'),
      inlinePolicies: {
        rekognitionPolicy: new iam.PolicyDocument({
          statements: [sagemakerRolePolicies],
        }),
      },
    });

    const stateMachinePolicies = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        's3:*',
        'lambda:*',
        'cloudwatch:*',
        'iam:*',
        'kms:*',
        'sagemaker:*',
        'ecr:*'
      ],
      resources: ['*'],
    });
    const stateMachineRole = new iam.Role(this, 'stateMachineRole', {
      assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
      inlinePolicies: {
        rekognitionPolicy: new iam.PolicyDocument({
          statements: [stateMachinePolicies],
        }),
      },
    });
    // ---- S3 BUCKET FOR MODEL METADATA
    const model_metadata_bucket = new s3.Bucket(this, 'model_metadata_bucket', {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });
    // upload model card json to bucket
    new s3deploy.BucketDeployment(this, 'Model-Card_upload', {
      sources: [s3deploy.Source.asset("./model_card_json_files")],
      exclude: ['.DS_Store'],
      destinationBucket: model_metadata_bucket,
      destinationKeyPrefix: "model_card_json/"
    });
    // ---- S3 BUCKET FOR ASYNC ENDPOINT OUTPUT
    const sagemakerOutputBucket = new s3.Bucket(this, 'endpoint_output_bucket', {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });
    // --------- KMS KEY FOR SAGEMAKER ENCRYPTION
    const kmsKeyPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          principals: [new iam.AccountRootPrincipal()],
          actions: ['kms:*'],
          resources: ['*'],
        }),
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          principals: [
            new iam.ServicePrincipal('sagemaker.amazonaws.com'),
            new iam.ServicePrincipal('lambda.amazonaws.com'),
            new iam.ServicePrincipal('s3.amazonaws.com'),
          ],
          actions: [
            'kms:Encrypt',
            'kms:Decrypt',
            'kms:ReEncrypt*',
            'kms:GenerateDataKey*',
            'kms:DescribeKey',
          ],
          resources: ['*'],
        }),
      ],
    });
    const kmsKey = new kms.Key(this, 'kms_key', {
      enableKeyRotation: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      policy: kmsKeyPolicy
    });
    // ---------------- model deployment  ------------

    const modelDeploymentLambda = new lambda.Function(this, 'model-deployment-lambda', {
      runtime: lambda.Runtime.PYTHON_3_9,
      code: lambda.Code.fromAsset('lambda'),
      handler: 'model_deployment.lambda_handler',
      timeout: Duration.minutes(15),
      environment: {
        "model_metadata_bucket_name": model_metadata_bucket.bucketName,
        "kms_key_arn": kmsKey.keyArn,
        "sm_execution_role_arn": sagemakerRole.roleArn,
        "region": region,
        "LOG_LEVEL": LOG_LEVEL
      },
      description: 'Deploys SageMaker Models',
      role: deploymentLambdaRole,
    });


    // ---------------- endpoint deployment  ------------

    const endpointDeploymentLambda = new lambda.Function(this, 'endpoint-deployment-lambda', {
      runtime: lambda.Runtime.PYTHON_3_9,
      code: lambda.Code.fromAsset('lambda'),
      handler: 'endpoint_deployment.lambda_handler',
      timeout: Duration.minutes(15),
      environment: {
        "model_output_bucket_name": sagemakerOutputBucket.bucketName,
        "kms_key_id": kmsKey.keyId,
        "region": region,
        "LOG_LEVEL": LOG_LEVEL
      },
      description: 'Deploys SageMaker Endpoints and Endpoint Configurations',
      role: deploymentLambdaRole,
    });


    // ---------------- endpoint scaling and ssm ------------

    const endpointScalingAndSSMLambda = new lambda.Function(this, 'endpoint-scaling-and-ssm', {
      runtime: lambda.Runtime.PYTHON_3_9,
      code: lambda.Code.fromAsset('lambda'),
      handler: 'endpoint_scaling_and_ssm.lambda_handler',
      timeout: Duration.minutes(15),
      environment: {
        "region": region,
        "LOG_LEVEL": LOG_LEVEL
      },
      description: 'Deploys SageMaker Endpoints and Endpoint Configurations',
      role: deploymentLambdaRole,
    });


    // ---------------- update model dag deployment  ------------

    const updateModelDagLambda = new lambda.Function(this, 'update-model-dag', {
      runtime: lambda.Runtime.PYTHON_3_9,
      code: lambda.Code.fromAsset('lambda'),
      handler: 'update_model_dag.lambda_handler',
      timeout: Duration.minutes(15),
      environment: {
        "region": region,
        "LOG_LEVEL": LOG_LEVEL
      },
      description: 'Deploys SageMaker Endpoints and Endpoint Configurations',
      role: deploymentLambdaRole,
    });



    // ------------------ STEP FUNCTIONS -----------------------
    const modelDeploymentTask = new tasks.LambdaInvoke(this, 'modelDeploymentTask', {
      lambdaFunction: modelDeploymentLambda,
      outputPath: '$.Payload',
      retryOnServiceExceptions: true,
      taskTimeout: stepfunctions.Timeout.duration(cdk.Duration.minutes(60)),
    });
    modelDeploymentTask.addRetry({
      maxAttempts: 1,
      errors: ['States.ALL'],
    });
    const endpointDeploymentTask = new tasks.LambdaInvoke(this, 'endpointDeploymentTask', {
      lambdaFunction: endpointDeploymentLambda,
      outputPath: '$.Payload',
      retryOnServiceExceptions: true,
      taskTimeout: stepfunctions.Timeout.duration(cdk.Duration.minutes(60)),
    });
    endpointDeploymentTask.addRetry({
      maxAttempts: 8,
      interval: Duration.seconds(30),
      errors: ['States.ALL'],
    });
    const endpointScalingAndSSMTask = new tasks.LambdaInvoke(this, 'endpointScalingAndSSMTask', {
      lambdaFunction: endpointScalingAndSSMLambda,
      outputPath: '$.Payload',
      retryOnServiceExceptions: true,
      taskTimeout: stepfunctions.Timeout.duration(cdk.Duration.minutes(60)),
    });
    endpointScalingAndSSMTask.addRetry({
      maxAttempts: 8,
      interval: Duration.seconds(30),
      errors: ['States.ALL'],
    });
    const updateModelDagTask = new tasks.LambdaInvoke(this, 'updateModelDagTask', {
      lambdaFunction: updateModelDagLambda,
      outputPath: '$.Payload',
      retryOnServiceExceptions: true,
      taskTimeout: stepfunctions.Timeout.duration(cdk.Duration.minutes(60)),
    });
    updateModelDagTask.addRetry({
      maxAttempts: 1,
      errors: ['States.ALL'],
    });


    const parallelDeploymentState = new stepfunctions.Parallel(this, 'SageMaker Deployment')
      .branch(modelDeploymentTask.next(endpointDeploymentTask)
        .next(endpointScalingAndSSMTask))
    const parallelDagUpdateState = new stepfunctions.Parallel(this, 'Model Dag Update')
      .branch(updateModelDagTask)

    const modelsMapState = new stepfunctions.Map(this, 'Models Map State', {
      itemsPath: '$.models',
    })
    const graphsMapState = new stepfunctions.Map(this, 'Graphs Map State', {
      itemsPath: '$.execution_graphs',
    })

    const parallelDeployment = new stepfunctions.Parallel(this, 'Parallel Model and Endpoint Deployment', {
      resultPath: '$.parallelDeploymentResults',
    });
    const parallelDAGUpdate = new stepfunctions.Parallel(this, 'Parallel Model Dag Update', {
      resultPath: '$.parallelDAGUpdateResults',
    });

    const definition = parallelDeployment.branch(modelsMapState.itemProcessor(parallelDeploymentState)).next(parallelDAGUpdate.branch(graphsMapState.itemProcessor(parallelDagUpdateState)))


    const stateMachine = new stepfunctions.StateMachine(this, 'SageMaker Model and Endpoint Deployment', {
      timeout: cdk.Duration.minutes(180),
      definitionBody: stepfunctions.DefinitionBody.fromChainable(definition),
      role: stateMachineRole
    });


  }
}


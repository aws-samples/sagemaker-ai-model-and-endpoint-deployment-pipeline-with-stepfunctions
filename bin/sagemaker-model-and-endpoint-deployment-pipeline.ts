#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { SagemakerModelAndEndpointDeploymentPipelineStack } from '../lib/sagemaker-model-and-endpoint-deployment-pipeline-stack';

const app = new cdk.App();
new SagemakerModelAndEndpointDeploymentPipelineStack(app, 'SagemakerModelAndEndpointDeploymentPipelineStack');

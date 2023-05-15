package ecs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/smithy-go"

	cloudwatchlogsTypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	ecsTypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
)

// ecsClientInterface encapsulates all the required AWS functionality to
// successfully run tasks via this plugin.
type ecsClientInterface interface {

	// DescribeCluster is used to determine the health of the plugin by
	// querying AWS for the cluster and checking its current status. A status
	// other than ACTIVE is considered unhealthy.
	DescribeCluster(ctx context.Context) error

	// DescribeTaskStatus attempts to return the current health status of the
	// ECS task and should be used for health checking.
	DescribeTaskStatus(ctx context.Context, taskDetails TaskDetails) (string, error)

	// RunTRegisterTaskDefinition TODO
	RegisterTaskDefinition(ctx context.Context, cfg TaskConfig, env map[string]string) (string, error)

	// RunTask is used to trigger the running of a new ECS task based on the
	// provided configuration. The ARN of the task, as well as any errors are
	// returned to the caller.
	RunTask(ctx context.Context, cfg TaskConfig) (string, error)

	// StopTask stops the running ECS task, adding a custom message which can
	// be viewed via the AWS console specifying it was this Nomad driver which
	// performed the action.
	StopTask(ctx context.Context, taskDetails TaskDetails) error

	// TODO: implement TaskEvents

	// GetLogs retreives the most recent logs from CloudWatch to be viewed in
	// in nomads alloc stdout
	GetLogs(ctx context.Context, taskDetails TaskDetails, logToken string) (string, string, error)
}

type awsEcsClient struct {
	cluster    string
	ecsClient  *ecs.Client
	logsClient *cloudwatchlogs.Client
}

type TaskDetails struct {
	taskARN string
	group   string
	stream  string
}

const (
	CompatibilityFargateSpot ecsTypes.Compatibility = "FARGATE_SPOT"
)

// DescribeCluster satisfies the ecs.ecsClientInterface DescribeCluster
// interface function.
func (c awsEcsClient) DescribeCluster(ctx context.Context) error {
	input := ecs.DescribeClustersInput{Clusters: []string{c.cluster}}

	resp, err := c.ecsClient.DescribeClusters(ctx, &input)
	if err != nil {
		return err
	}

	if len(resp.Clusters) > 1 || len(resp.Clusters) < 1 {
		return fmt.Errorf("AWS returned %v ECS clusters, expected 1", len(resp.Clusters))
	}

	if *resp.Clusters[0].Status != "ACTIVE" {
		return fmt.Errorf("ECS cluster status: %s", *resp.Clusters[0].Status)
	}

	return nil
}

// DescribeTaskStatus satisfies the ecs.ecsClientInterface DescribeTaskStatus
// interface function.
func (c awsEcsClient) DescribeTaskStatus(ctx context.Context, taskDetails TaskDetails) (string, error) {
	input := ecs.DescribeTasksInput{
		Cluster: aws.String(c.cluster),
		Tasks:   []string{taskDetails.taskARN},
	}

	resp, err := c.ecsClient.DescribeTasks(ctx, &input)
	if err != nil {
		return "", err
	}
	return *resp.Tasks[0].LastStatus, nil
}

// RunTask satisfies the ecs.ecsClientInterface RunTask interface function.
func (c awsEcsClient) RunTask(ctx context.Context, cfg TaskConfig) (string, error) {

	// TODO: move out of here
	err := c.CreateLogGroupIfNotExist(ctx, cfg)
	if err != nil {
		return "", fmt.Errorf("failed to validate log group: %w", err)
	}

	input := c.buildTaskInput(cfg)
	resp, err := c.ecsClient.RunTask(ctx, input)
	if err != nil {
		return "", err
	}

	// preemptive cleanup
	err = c.deleteTaskDefinition(ctx, cfg.Task.TaskDefinition)
	if err != nil {
		return *resp.Tasks[0].TaskArn, err
	}

	return *resp.Tasks[0].TaskArn, nil
}

// CreateLogGroupIfNotExist checks to see if the log groups and creates it if necessary
func (c awsEcsClient) CreateLogGroupIfNotExist(ctx context.Context, cfg TaskConfig) error {

	describeLogGroupsResponse, err := c.logsClient.DescribeLogGroups(ctx, &cloudwatchlogs.DescribeLogGroupsInput{
		LogGroupNamePrefix: &cfg.Task.LogGroup,
	})
	if err != nil {
		return err
	}

	// do nothing if a log group already exists
	if len(describeLogGroupsResponse.LogGroups) > 0 {
		return nil
	}

	// create the log group if not exist
	_, err = c.logsClient.CreateLogGroup(ctx, &cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: &cfg.Task.LogGroup,
	})
	if err != nil {
		return err
	}

	// set aggressive retention policy for nomad created log groups
	_, err = c.logsClient.PutRetentionPolicy(ctx, &cloudwatchlogs.PutRetentionPolicyInput{
		LogGroupName:    &cfg.Task.LogGroup,
		RetentionInDays: aws.Int32(1),
	})
	if err != nil {
		return err
	}

	return nil
}

func (c awsEcsClient) RegisterTaskDefinition(ctx context.Context, cfg TaskConfig, env map[string]string) (string, error) {

	var taskEnv []ecsTypes.KeyValuePair

	for key, val := range env {
		taskEnv = append(taskEnv, ecsTypes.KeyValuePair{
			Name:  &key,
			Value: &val,
		})
	}

	v, m := buildMountPoint(cfg.Task.Volumes, cfg.Task.EfsVolumes)

	taskDefInput := ecs.RegisterTaskDefinitionInput{
		Memory:      aws.String(fmt.Sprintf("%d", cfg.Task.Memory)),
		Cpu:         aws.String(fmt.Sprintf("%d", cfg.Task.CPU)),
		TaskRoleArn: &cfg.Task.TaskRoleArn,
		ContainerDefinitions: []ecsTypes.ContainerDefinition{
			{
				Name:        &cfg.Task.Family,
				Image:       &cfg.Task.Image,
				Command:     cfg.Task.Command,
				Interactive: aws.Bool(true),
				LogConfiguration: &ecsTypes.LogConfiguration{
					LogDriver: ecsTypes.LogDriverAwslogs,
					Options: map[string]string{
						"awslogs-group":         cfg.Task.LogGroup,
						"awslogs-region":        cfg.Task.Region,
						"awslogs-stream-prefix": "nomad",
					},
				},
				Essential:   aws.Bool(true),
				Environment: taskEnv,
				// PortMappings: buildPortMapping(t.Publish),
				MountPoints: m,
				// VolumesFrom: []*ecs.VolumeFrom{},
			},
		},
		Family:  &cfg.Task.Family,
		Volumes: v,
		// TaskRoleArn: aws.String(t.TaskRoleArn),
	}

	// Set Fargate specific configuration
	if cfg.Task.LaunchType == string(ecsTypes.CompatibilityFargate) || cfg.Task.LaunchType == string(CompatibilityFargateSpot) {
		taskDefInput.RequiresCompatibilities = []ecsTypes.Compatibility{ecsTypes.CompatibilityFargate}
		taskDefInput.NetworkMode = ecsTypes.NetworkModeAwsvpc
		taskDefInput.ExecutionRoleArn = &cfg.Task.ExecutionRoleArn
	}

	registerTaskDefinitionResponse, err := c.ecsClient.RegisterTaskDefinition(ctx, &taskDefInput)
	if err != nil {
		return "", err
	}

	return *registerTaskDefinitionResponse.TaskDefinition.TaskDefinitionArn, nil
}

// buildTaskInput is used to convert the jobspec supplied configuration input
// into the appropriate ecs.RunTaskInput object.
func (c awsEcsClient) buildTaskInput(cfg TaskConfig) *ecs.RunTaskInput {

	input := ecs.RunTaskInput{
		Cluster:              aws.String(c.cluster),
		Count:                aws.Int32(1),
		StartedBy:            aws.String("nomad-ecs-driver"),
		NetworkConfiguration: &ecsTypes.NetworkConfiguration{AwsvpcConfiguration: &ecsTypes.AwsVpcConfiguration{}},
		EnableExecuteCommand: true,
		EnableECSManagedTags: true,
	}

	if cfg.Task.LaunchType != "" {

		// Run task on EC2 instances
		if cfg.Task.LaunchType == "EC2" {
			input.LaunchType = ecsTypes.LaunchTypeEc2

			// Run task on Fargate
		} else if cfg.Task.LaunchType == "FARGATE" {
			input.LaunchType = ecsTypes.LaunchTypeFargate

			// Run task on Fargate Spot
		} else if cfg.Task.LaunchType == "FARGATE_SPOT" {
			input.CapacityProviderStrategy = []ecsTypes.CapacityProviderStrategyItem{
				{
					CapacityProvider: aws.String(string(CompatibilityFargateSpot)),
					Weight:           1,
				},
			}
		}
	}

	if cfg.Task.TaskDefinition != "" {
		input.TaskDefinition = aws.String(cfg.Task.TaskDefinition)
	}

	// Handle the task networking setup.
	if cfg.Task.NetworkConfiguration.TaskAWSVPCConfiguration.AssignPublicIP != "" {
		assignPublicIp := cfg.Task.NetworkConfiguration.TaskAWSVPCConfiguration.AssignPublicIP
		if assignPublicIp == "ENABLED" {
			input.NetworkConfiguration.AwsvpcConfiguration.AssignPublicIp = ecsTypes.AssignPublicIpEnabled
		} else if assignPublicIp == "DISABLED" {
			input.NetworkConfiguration.AwsvpcConfiguration.AssignPublicIp = ecsTypes.AssignPublicIpDisabled
		}
	}
	if len(cfg.Task.NetworkConfiguration.TaskAWSVPCConfiguration.SecurityGroups) > 0 {
		input.NetworkConfiguration.AwsvpcConfiguration.SecurityGroups = cfg.Task.NetworkConfiguration.TaskAWSVPCConfiguration.SecurityGroups
	}
	if len(cfg.Task.NetworkConfiguration.TaskAWSVPCConfiguration.Subnets) > 0 {
		input.NetworkConfiguration.AwsvpcConfiguration.Subnets = cfg.Task.NetworkConfiguration.TaskAWSVPCConfiguration.Subnets
	}

	return &input
}

// StopTask satisfies the ecs.ecsClientInterface StopTask interface function.
func (c awsEcsClient) StopTask(ctx context.Context, taskDetails TaskDetails) error {
	input := ecs.StopTaskInput{
		Cluster: aws.String(c.cluster),
		Task:    &taskDetails.taskARN,
		Reason:  aws.String("stopped by nomad-ecs-driver automation"),
	}

	_, err := c.ecsClient.StopTask(ctx, &input)
	return err
}

// GetLogs function retrieves log events from CloudWatch logs using the AWS SDK.
func (c awsEcsClient) GetLogs(ctx context.Context, taskDetails TaskDetails, logToken string) (string, string, error) {

	// Create a GetLogEventsInput struct with StartFromHead, LogGroupName, and LogStreamName.
	logEventsInput := cloudwatchlogs.GetLogEventsInput{
		StartFromHead: aws.Bool(true),
		LogGroupName:  &taskDetails.group,
		LogStreamName: &taskDetails.stream,
	}

	// If a non-empty logToken is provided, set NextToken property in logEventsInput.
	if logToken != "" {
		logEventsInput.NextToken = &logToken
	}

	// Call GetLogEvents method on the logsClient interface with input instance logEventsInput.
	logEvents, err := c.logsClient.GetLogEvents(ctx, &logEventsInput)

	// Check if there's any error while calling GetLogEvents method.
	if err != nil {
		// Check if the error is due to ResourceNotFoundException.
		var nfe *cloudwatchlogsTypes.ResourceNotFoundException
		if errors.As(err, &nfe) {
			return logToken, "", nil
		}

		// Check if the error is due to OperationError and return an error message with service name, operation name, and underlying error message.
		var oe *smithy.OperationError
		if errors.As(err, &oe) {
			return logToken, "", fmt.Errorf("failed to call service: %s, operation: %s, error: %v", oe.Service(), oe.Operation(), oe.Unwrap())
		}
	}

	// Format log events into a readable string format.
	var logs string
	for _, logEvent := range logEvents.Events {
		ts := time.Unix(*logEvent.Timestamp/1000, 0).Format(time.RFC3339)
		logs = fmt.Sprintf("%s[%s] - %v\n", logs, ts, *logEvent.Message)
	}

	// Return the NextForwardToken, formatted logs string, and nil (no error).
	return *logEvents.NextForwardToken, logs, nil
}

func (c awsEcsClient) deleteTaskDefinition(ctx context.Context, arn string) error {

	// deregister the task definition
	_, err := c.ecsClient.DeregisterTaskDefinition(ctx, &ecs.DeregisterTaskDefinitionInput{
		TaskDefinition: &arn,
	})
	if err != nil {
		return err
	}

	// delete the task definition
	_, err = c.ecsClient.DeleteTaskDefinitions(ctx, &ecs.DeleteTaskDefinitionsInput{
		TaskDefinitions: []string{arn},
	})
	if err != nil {
		return err
	}

	return nil
}

func buildMountPoint(volumes []string, efsVolumes []string) (v []ecsTypes.Volume, k []ecsTypes.MountPoint) {
	if len(volumes) < 1 && len(efsVolumes) < 1 {
		return []ecsTypes.Volume{}, []ecsTypes.MountPoint{}
	}

	// Add Bind Mounts
	for i, volume := range volumes {
		av := strings.Split(volume, ":")

		// default to source path
		sourcePath := av[0]
		volumeName := "volume" + strconv.Itoa(i)

		mountPoint := ecsTypes.MountPoint{
			ContainerPath: &sourcePath,
			SourceVolume:  aws.String(volumeName),
			ReadOnly:      aws.Bool(false),
		}

		volume := ecsTypes.Volume{
			Name: aws.String(volumeName),
			Host: &ecsTypes.HostVolumeProperties{
				SourcePath: aws.String(sourcePath),
			},
		}

		// Map container port, if defined
		if len(av) > 1 {
			containerPath := av[1]
			mountPoint.ContainerPath = &containerPath
		}

		// Append to the slice
		k = append(k, mountPoint)
		v = append(v, volume)
	}

	// Add EFS Mounts
	for i, volume := range efsVolumes {
		av := strings.Split(volume, ":")
		efsFileSystemId := av[0]
		efsDirectory := av[1]
		containerDirectory := av[2]
		volumeName := "volume-efs" + strconv.Itoa(i)

		mountPoint := ecsTypes.MountPoint{
			ContainerPath: &containerDirectory,
			SourceVolume:  aws.String(volumeName),
			ReadOnly:      aws.Bool(false),
		}

		volume := ecsTypes.Volume{
			Name: aws.String(volumeName),
			EfsVolumeConfiguration: &ecsTypes.EFSVolumeConfiguration{
				// TODO parameterize this
				FileSystemId: &efsFileSystemId,
				// TODO pass in root efs mount path explicitly instead of using sourcePath
				RootDirectory: &efsDirectory,
			},
		}

		// Append to the slice
		k = append(k, mountPoint)
		v = append(v, volume)
	}
	return
}

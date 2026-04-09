import os
import boto3
from moto import mock_aws

# Import your actual main function from your script
from ConfigAWSssmManager import main

@mock_aws
def run_local_test():
    """
    This function creates a fake AWS environment, injects a dummy parameter,
    and then runs your ConfigAWSssmManager script to see if it can fetch it.
    """
    print("--- Starting Local AWS Mock Test ---")

    # 1. Set dummy environment variables so boto3 doesn't look for real ones
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'ap-northeast-1'

    # 2. Create the fake AWS SSM environment
    fake_ssm = boto3.client('ssm', region_name='ap-northeast-1')
    
    # 3. Inject a dummy parameter into the fake AWS environment
    fake_ssm.put_parameter(
        Name='TOKEN1',
        Description='A dummy token for local testing',
        Value='SuperSecretLocalTestValue123!',
        Type='SecureString'
    )
    print("Fake parameter 'TOKEN1' created in mock AWS environment.")

    # 4. Execute YOUR script's main function
    print("\nExecuting ConfigAWSssmManager.main()...")
    return_code, returned_value = main(
        p_user_id='test_user', 
        p_env_id='dev', 
        p_param_name='TOKEN1'
    )

    # 5. Evaluate the results
    print("\n--- Test Results ---")
    print(f"Return Code: {return_code}")
    print(f"Returned Value: {returned_value}")

    if return_code == 0 and returned_value == 'SuperSecretLocalTestValue123!':
        print("\nSUCCESS! Your script correctly fetched the mocked parameter.")
    else:
        print("\nFAILED. The script did not return the expected values.")

if __name__ == '__main__':
    run_local_test()
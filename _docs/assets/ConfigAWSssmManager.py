"""
Class Name: ConfigAWSssmManager.py
Process Overview: Retrieves configuration values from ConfigAWSssm Parameter store.
Execution User: Batch System User
Arguments:
  1. p_user_id: Caller User ID
  2. p_env_id: Caller Environment ID
  3. p_param_name: Parameter Name requested
Returns:
  0: Normal End
  1: Warning End
  8: Abnormal End
Author: ALPHA Project Team
Update History:
  1.0.0, 2026/03/23, ALPHA Project Team, Initial Release
"""

import os
import sys
import json
import boto3
from pathlib import Path
from botocore.exceptions import ClientError

# Import utilities from peer's modules.
from helpers import write_log, get_timestamp
from constants import (
    RETURN_NORMAL,
    RETURN_WARNING,
    RETURN_ABNORMAL,
    MESSAGE_COMM0017E,
    MESSAGE_COMM0018E,
    MESSAGE_COMM0033E,
    MESSAGE_COMM0034E,
    MESSAGE_COMM0012E,
    MESSAGE_COMM0002I
)


def main(p_user_id, p_env_id, p_param_name):
    """
    Name: main
    Function: Retrieves parameter values from AWS SSM.
    Argument:
      1. p_user_id
      2. p_env_id
      3. p_param_name
    Returns:
      0: Normal End
      1: Warning End
      8: Abnormal End
    """
    # Step 1: Start of the Processing.
    # Initialize all local variables to be used in this function.
    json_rules_file = 'parameter_rules.json'
    d_rules = {}
    
    # Generate the log file path format: {JOBID}_YYYYMMDD_HI24MISS. MSS.log
    p_timestamp = get_timestamp()
    p_log_path = Path(f"logs/AWSJOB_{p_timestamp}.MSS.log")

    try:
        # Step 2 & 3: Check if Parameter Rules file is available and not empty.
        if not os.path.exists(json_rules_file) or os.path.getsize(json_rules_file) == 0:
            write_log(
                p_log_path, 
                MESSAGE_COMM0033E, 
                'Error', 
                'Cannot access parameter rule file or file is empty.'
            )
            return RETURN_ABNORMAL, None

        # Step 4: Check if Parameter Rules file is readable/accessible.
        with open(json_rules_file, 'r', encoding='utf-8') as f_rule:
            try:
                d_rules = json.load(f_rule)
            except json.JSONDecodeError:
                write_log(
                    p_log_path, 
                    MESSAGE_COMM0017E, 
                    'Error', 
                    'Invalid JSON format in configuration file.'
                )
                return RETURN_ABNORMAL, None

        # Step 5: Initialize the SSM client with boto3.
        try:
            ssm_client = boto3.client('ssm')
        except Exception:
            write_log(
                p_log_path, 
                MESSAGE_COMM0034E, 
                'Error', 
                'Check the connection to AWSSSM.'
            )
            return RETURN_ABNORMAL, None

        # Step 6: Retrieve the caller rule set and verify key exists.
        if p_param_name not in d_rules:
            write_log(
                p_log_path, 
                MESSAGE_COMM0018E, 
                'Error', 
                f"Invalid Parameter Type Definition: {p_param_name}"
            )
            return RETURN_ABNORMAL, None

        # Step 7: Retrieve and Decrypt SSM Parameter.
        try:
            d_response = ssm_client.get_parameter(
                Name=p_param_name,
                WithDecryption=True
            )
            p_value = d_response['Parameter']['Value']
            
            # Output log when ConfigAWSssmManager call is successful.
            write_log(
                p_log_path, 
                MESSAGE_COMM0002I, 
                'Info', 
                'Parameter successfully retrieved.'
            )
            return RETURN_NORMAL, p_value

        except ClientError as e_client:
            write_log(
                p_log_path, 
                MESSAGE_COMM0012E, 
                'Error', 
                f"MissingParameter or AWS Error: {e_client}"
            )
            return RETURN_ABNORMAL, None
            
        except Exception as e_exception:
            write_log(
                p_log_path, 
                MESSAGE_COMM0034E, 
                'Error', 
                f"Check the connection to AWSSSM: {e_exception}"
            )
            return RETURN_ABNORMAL, None

    except Exception:
        write_log(
            p_log_path, 
            'UNKNOWN_ERR', 
            'Error', 
            'Abnormal End due to unexpected exception.'
        )
        return RETURN_ABNORMAL, None


if __name__ == '__main__':
    # Initialize all environment variables and input parameters.
    p_user_id = os.getenv('USER_ID', 'default_user')
    p_env_id = os.getenv('ENV_ID', 'dev')
    p_param_name = 'TOKEN1'

    # Execute the main function directly.
    return_code, returned_value = main(
        p_user_id, 
        p_env_id, 
        p_param_name
    )
    
    sys.exit(return_code)

# ------------------------------------------------------------------------------
# Class Name : config_awsssm_manager.py
# Function   : ConfigAWSssmManager
# Process Overview : Retrieve configuration values or parameter definitions from
#                  : AWS SSM Parameter Store using parameter_rules.json and
#                  : return them to the caller.
# Execution User   : Batch User / Application User
# Arguments        : 1. Request dictionary
# Returns          : 1. Tuple[int, dict]
# Author           : OpenAI
# Changelog:
#   1.00 2026/04/07 OpenAI Initial generated version based on workbook design
# ------------------------------------------------------------------------------

from __future__ import annotations

import json
import logging
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any


# ------------------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------------------
RETURN_CODE_NORMAL = 0
RETURN_CODE_WARNING = 1
RETURN_CODE_ABNORMAL = 8

LOG_MESSAGE_START = 'COMM0001I'
LOG_MESSAGE_SUCCESS = 'COMM0002I'
LOG_MESSAGE_PARAMETER_RULE_FILE_UNAVAILABLE = 'COMM0033E'
LOG_MESSAGE_PARAMETER_RULE_FILE_EMPTY = 'COMM0007E'
LOG_MESSAGE_PARAMETER_RULE_MISSING_ARGUMENT = 'COMM0006E'
LOG_MESSAGE_SSM_INITIALIZATION_FAILED = 'COMM0034E'
LOG_MESSAGE_MISSING_PARAMETER = 'COMM0012E'

DEFAULT_CONFIG_ROOT_ENV = 'ERP_conf_PATH'
DEFAULT_PARAMETER_RULES_FILENAME = 'parameter_rules.json'

SUPPORTED_CALLERS = {
    'InboundProcessor',
    'OutboundProcessor',
    'ESSJobProcessor',
    'ESSJobExecuteProcessor',
    'ESSJoBExecuteProcessor',
    'AuthManager',
    'S3OperationService',
}

LOGGER = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
# Exceptions
# ------------------------------------------------------------------------------
class ConfigAWSssmManagerError(Exception):
    """Base exception for ConfigAWSssmManager."""

    def __init__(self, message_id: str, message: str) -> None:
        super().__init__(message)
        self.message_id = message_id


class ParameterRuleFileError(ConfigAWSssmManagerError):
    """Raised when the parameter rule file is unavailable or invalid."""


class MissingArgumentKeyError(ConfigAWSssmManagerError):
    """Raised when placeholder input is missing."""


class MissingParameterError(ConfigAWSssmManagerError):
    """Raised when a parameter is missing in AWS SSM."""


@dataclass
class ConfigAWSssmManagerRequest:
    """Normalized request for ConfigAWSssmManager."""

    caller_process_name: str
    args: dict[str, Any]
    config_root_path: str | None = None
    config_root_env_name: str = DEFAULT_CONFIG_ROOT_ENV
    parameter_rules_filename: str = DEFAULT_PARAMETER_RULES_FILENAME
    temp_directory: str | None = None
    aws_region: str | None = None


# ------------------------------------------------------------------------------
# Main Function Definition
# ------------------------------------------------------------------------------
def main(p_request: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    """Execute ConfigAWSssmManager.

    Expected input format:
        {
            'caller_process_name': 'AuthManager',
            'args': {'env': 'dev', 'service': 'oauth'},
            'config_root_path': '/app/dev/config files',
            'aws_region': 'ap-southeast-1',
        }
    """
    request = initialize_runtime_context(p_request)
    write_start_log()

    parameter_rules_file_path = get_parameter_rules_file_path(request)
    validate_parameter_rules_file_exists(parameter_rules_file_path)
    copied_rule_file_path = copy_parameter_rules_file_to_temp(parameter_rules_file_path, request.temp_directory)
    validate_parameter_rules_file_not_empty(copied_rule_file_path)
    d_parameter_rules = load_parameter_rules(copied_rule_file_path)

    ssm_client = initialize_ssm_client(request.aws_region)
    d_caller_rule_set = get_caller_rule_set(d_parameter_rules, request.caller_process_name)
    validate_required_args(d_caller_rule_set, request.args)
    l_parameter_names = build_parameter_names(d_caller_rule_set, request.args)
    d_ssm_values = get_all_ssm_parameters(ssm_client, l_parameter_names)

    write_success_log()
    return RETURN_CODE_NORMAL, {
        'caller_process_name': request.caller_process_name,
        'parameter_names': l_parameter_names,
        'values': d_ssm_values,
    }


# ------------------------------------------------------------------------------
# Subfunction Definition
# ------------------------------------------------------------------------------
def initialize_runtime_context(p_request: dict[str, Any]) -> ConfigAWSssmManagerRequest:
    """Validate and normalize the input request."""
    if not isinstance(p_request, dict):
        raise TypeError('p_request must be a dictionary.')

    caller_process_name = str(p_request.get('caller_process_name', '')).strip()
    if not caller_process_name:
        raise ValueError('caller_process_name is required.')
    if caller_process_name not in SUPPORTED_CALLERS:
        raise ValueError(f'Unsupported caller_process_name: {caller_process_name}')

    d_args = p_request.get('args') or {}
    if not isinstance(d_args, dict):
        raise TypeError('args must be a dictionary.')

    return ConfigAWSssmManagerRequest(
        caller_process_name=caller_process_name,
        args=d_args,
        config_root_path=_clean_optional_string(p_request.get('config_root_path')),
        config_root_env_name=str(
            p_request.get('config_root_env_name', DEFAULT_CONFIG_ROOT_ENV)
        ).strip() or DEFAULT_CONFIG_ROOT_ENV,
        parameter_rules_filename=str(
            p_request.get('parameter_rules_filename', DEFAULT_PARAMETER_RULES_FILENAME)
        ).strip() or DEFAULT_PARAMETER_RULES_FILENAME,
        temp_directory=_clean_optional_string(p_request.get('temp_directory')),
        aws_region=_clean_optional_string(p_request.get('aws_region')),
    )


def write_start_log() -> None:
    """Output a start log for parameter acquisition."""
    write_log(LOG_MESSAGE_START, 'Info', 'Start parameter acquisition.')


def get_parameter_rules_file_path(p_request: ConfigAWSssmManagerRequest) -> Path:
    """Build the full path to parameter_rules.json."""
    config_root = p_request.config_root_path
    if not config_root:
        config_root = os.getenv(p_request.config_root_env_name, '').strip()

    if not config_root:
        handle_config_awsssm_error(
            LOG_MESSAGE_PARAMETER_RULE_FILE_UNAVAILABLE,
            f'Configuration root path is not defined: {p_request.config_root_env_name}',
            exception_class=ParameterRuleFileError,
        )

    return Path(config_root) / p_request.parameter_rules_filename


def validate_parameter_rules_file_exists(p_file_path: Path) -> None:
    """Check the availability of parameter rule file."""
    if not p_file_path.exists() or not p_file_path.is_file():
        handle_config_awsssm_error(
            LOG_MESSAGE_PARAMETER_RULE_FILE_UNAVAILABLE,
            f'Cannot access parameter rule file: {p_file_path}',
            exception_class=ParameterRuleFileError,
        )


def copy_parameter_rules_file_to_temp(
    p_file_path: Path,
    p_temp_directory: str | None = None,
) -> Path:
    """Copy the Parameter Rules file to a temporary folder for processing.

    The workbook explicitly includes this step. If no temp directory is supplied,
    the original file path is reused.
    """
    if not p_temp_directory:
        return p_file_path

    temp_directory = Path(p_temp_directory)
    temp_directory.mkdir(parents=True, exist_ok=True)
    destination = temp_directory / p_file_path.name
    shutil.copy2(p_file_path, destination)
    return destination


def validate_parameter_rules_file_not_empty(p_file_path: Path) -> None:
    """Check if Parameter Rules file is not empty."""
    if p_file_path.stat().st_size <= 0:
        handle_config_awsssm_error(
            LOG_MESSAGE_PARAMETER_RULE_FILE_EMPTY,
            f'Parameter rule file is empty: {p_file_path}',
            exception_class=ParameterRuleFileError,
        )


def load_parameter_rules(p_file_path: Path) -> dict[str, Any]:
    """Load parameter_rules.json and validate its shape."""
    try:
        with p_file_path.open('r', encoding='utf-8') as rule_file:
            d_parameter_rules = json.load(rule_file)
    except json.JSONDecodeError as e_exception:
        handle_config_awsssm_error(
            LOG_MESSAGE_PARAMETER_RULE_FILE_UNAVAILABLE,
            f'Invalid JSON format in parameter rule file: {p_file_path}',
            e_exception,
            exception_class=ParameterRuleFileError,
        )
    except OSError as e_exception:
        handle_config_awsssm_error(
            LOG_MESSAGE_PARAMETER_RULE_FILE_UNAVAILABLE,
            f'Cannot access parameter rule file: {p_file_path}',
            e_exception,
            exception_class=ParameterRuleFileError,
        )

    if not isinstance(d_parameter_rules, dict):
        handle_config_awsssm_error(
            LOG_MESSAGE_PARAMETER_RULE_FILE_UNAVAILABLE,
            f'Parameter rule file root must be an object: {p_file_path}',
            exception_class=ParameterRuleFileError,
        )
    return d_parameter_rules


def initialize_ssm_client(p_aws_region: str | None = None) -> Any:
    """Establish a connection to AWS SSM by creating an SSM client using boto3."""
    try:
        import boto3
    except ImportError as e_exception:
        handle_config_awsssm_error(
            LOG_MESSAGE_SSM_INITIALIZATION_FAILED,
            'boto3 is not installed.',
            e_exception,
        )

    try:
        if p_aws_region:
            return boto3.client('ssm', region_name=p_aws_region)
        return boto3.client('ssm')
    except Exception as e_exception:
        handle_config_awsssm_error(
            LOG_MESSAGE_SSM_INITIALIZATION_FAILED,
            'SSM Client Initialization failed.',
            e_exception,
        )


def get_caller_rule_set(
    d_parameter_rules: dict[str, Any],
    p_caller_process_name: str,
) -> dict[str, Any]:
    """Load the rule set associated with the caller."""
    caller_variants = [
        p_caller_process_name,
        _normalize_caller_name(p_caller_process_name),
    ]

    for caller_name in caller_variants:
        if caller_name in d_parameter_rules:
            d_rule_set = d_parameter_rules[caller_name]
            if isinstance(d_rule_set, dict):
                return d_rule_set

    handle_config_awsssm_error(
        LOG_MESSAGE_PARAMETER_RULE_FILE_UNAVAILABLE,
        f'Caller rule set not found: {p_caller_process_name}',
        exception_class=ParameterRuleFileError,
    )


def validate_required_args(
    d_rule_set: dict[str, Any],
    d_args: dict[str, Any],
) -> None:
    """Check if the necessary key fields exist for the rule set."""
    required_keys = set()

    explicit_required_keys = d_rule_set.get('required_keys')
    if isinstance(explicit_required_keys, list):
        required_keys.update(str(item) for item in explicit_required_keys)

    l_patterns = _extract_patterns(d_rule_set)
    for pattern in l_patterns:
        required_keys.update(extract_placeholders(pattern))

    missing_keys = [key for key in sorted(required_keys) if key not in d_args]
    if missing_keys:
        handle_config_awsssm_error(
            LOG_MESSAGE_PARAMETER_RULE_MISSING_ARGUMENT,
            f'Missing argument key(s): {", ".join(missing_keys)}',
            exception_class=MissingArgumentKeyError,
        )


def build_parameter_names(
    d_rule_set: dict[str, Any],
    d_args: dict[str, Any],
) -> list[str]:
    """Convert patterns into actual parameter names."""
    l_patterns = _extract_patterns(d_rule_set)
    l_parameter_names: list[str] = []

    for pattern in l_patterns:
        parameter_name = pattern
        for placeholder in extract_placeholders(pattern):
            parameter_name = parameter_name.replace(
                '{' + placeholder + '}',
                str(d_args[placeholder]),
            )
        l_parameter_names.append(parameter_name)

    if not l_parameter_names:
        handle_config_awsssm_error(
            LOG_MESSAGE_PARAMETER_RULE_FILE_UNAVAILABLE,
            'No patterns were defined for the caller rule set.',
            exception_class=ParameterRuleFileError,
        )

    return l_parameter_names


def get_all_ssm_parameters(
    p_ssm_client: Any,
    l_parameter_names: list[str],
) -> dict[str, str]:
    """Retrieve all generated parameter names from AWS SSM."""
    d_ssm_values: dict[str, str] = {}
    for parameter_name in l_parameter_names:
        d_ssm_values[parameter_name] = get_ssm_parameter(p_ssm_client, parameter_name)
    return d_ssm_values


def get_ssm_parameter(p_ssm_client: Any, p_parameter_name: str) -> str:
    """Retrieve one decrypted SSM parameter."""
    try:
        response = p_ssm_client.get_parameter(
            Name=p_parameter_name,
            WithDecryption=True,
        )
    except Exception as e_exception:
        handle_config_awsssm_error(
            LOG_MESSAGE_MISSING_PARAMETER,
            f'Missing parameter in AWS SSM: {p_parameter_name}',
            e_exception,
            exception_class=MissingParameterError,
        )

    if 'Parameter' not in response or 'Value' not in response['Parameter']:
        handle_config_awsssm_error(
            LOG_MESSAGE_MISSING_PARAMETER,
            f'Missing parameter in AWS SSM: {p_parameter_name}',
            exception_class=MissingParameterError,
        )

    return str(response['Parameter']['Value'])


def write_success_log() -> None:
    """Output a success log."""
    write_log(LOG_MESSAGE_SUCCESS, 'Info', 'ConfigAWSssmManager call function is successful.')


def write_log(p_message_id: str, p_log_level: str, p_message: str) -> None:
    """Write a log message using the standard logger."""
    log_level = str(p_log_level).strip().upper()
    if not LOGGER.handlers:
        logging.basicConfig(level=logging.INFO)

    formatted_message = f'[{p_message_id}] {p_message}'
    if log_level == 'ERROR':
        LOGGER.error(formatted_message)
    else:
        LOGGER.info(formatted_message)


def handle_config_awsssm_error(
    p_message_id: str,
    p_message: str,
    p_exception: Exception | None = None,
    *,
    exception_class: type[ConfigAWSssmManagerError] = ConfigAWSssmManagerError,
) -> None:
    """Write error log and raise a structured exception."""
    write_log(p_message_id, 'Error', p_message)
    if p_exception is None:
        raise exception_class(p_message_id, p_message)
    raise exception_class(p_message_id, f'{p_message} | cause={p_exception}') from p_exception


def extract_placeholders(p_pattern: str) -> list[str]:
    """Extract placeholders like {env} and {service} from a pattern."""
    return re.findall(r'\{([^{}]+)\}', p_pattern)


def _extract_patterns(d_rule_set: dict[str, Any]) -> list[str]:
    """Extract patterns from one caller rule set.

    This function accepts a few possible shapes because the workbook does not
    define a single strict JSON schema for parameter_rules.json.
    """
    if isinstance(d_rule_set.get('patterns'), list):
        return [str(item) for item in d_rule_set['patterns']]

    if isinstance(d_rule_set.get('rules'), list):
        l_patterns: list[str] = []
        for item in d_rule_set['rules']:
            if isinstance(item, dict):
                if 'pattern' in item:
                    l_patterns.append(str(item['pattern']))
                elif 'patterns' in item and isinstance(item['patterns'], list):
                    l_patterns.extend(str(pattern) for pattern in item['patterns'])
        if l_patterns:
            return l_patterns

    if isinstance(d_rule_set.get('pattern'), str):
        return [str(d_rule_set['pattern'])]

    return []


def _normalize_caller_name(p_caller_name: str) -> str:
    """Normalize inconsistent workbook caller spellings."""
    mapping = {
        'ESSJoBExecuteProcessor': 'ESSJobExecuteProcessor',
        'ESSJobProcessor': 'ESSJobExecuteProcessor',
    }
    return mapping.get(p_caller_name, p_caller_name)


def _clean_optional_string(p_value: Any) -> str | None:
    """Normalize optional string input."""
    if p_value is None:
        return None
    cleaned_value = str(p_value).strip()
    return cleaned_value or None


# ------------------------------------------------------------------------------
# Main Processing Execution
# ------------------------------------------------------------------------------
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Execute ConfigAWSssmManager.')
    parser.add_argument('--caller-process-name', required=True)
    parser.add_argument('--config-root-path')
    parser.add_argument('--parameter-rules-filename', default=DEFAULT_PARAMETER_RULES_FILENAME)
    parser.add_argument('--aws-region')
    parser.add_argument(
        '--arg',
        action='append',
        default=[],
        help='Argument in key=value form. Repeat as needed.',
    )

    args = parser.parse_args()

    d_args = {}
    for item in args.arg:
        if '=' not in item:
            raise ValueError(f'Invalid --arg value: {item}')
        key, value = item.split('=', 1)
        d_args[key] = value

    request = {
        'caller_process_name': args.caller_process_name,
        'args': d_args,
        'config_root_path': args.config_root_path,
        'parameter_rules_filename': args.parameter_rules_filename,
        'aws_region': args.aws_region,
    }

    rc, result = main(request)
    print(json.dumps({'return_code': rc, 'result': result}, ensure_ascii=False, indent=4))


# ------------------------------------------------------------------------------
# Class Name : config_manager.py
# Function   : Config Management Common Function
# Process Overview : Retrieve configuration values or parameter definitions from
#                  : runtime configuration files and return them to the caller.
# Execution User   : Batch User / Application User
# Arguments        : 1. Request dictionary
# Returns          : 1. Tuple[int, dict]
# Author           : OpenAI
# Changelog:
#   1.00 2026/04/07 OpenAI Initial generated version based on workbook design
# ------------------------------------------------------------------------------

from __future__ import annotations

import csv
import json
import logging
import os
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


# ------------------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------------------
RETURN_CODE_NORMAL = 0
RETURN_CODE_WARNING = 1
RETURN_CODE_ABNORMAL = 8

LOG_MESSAGE_START = 'COMM0001I'
LOG_MESSAGE_SUCCESS = 'COMM0001I'
LOG_MESSAGE_CONFIG_FILE_MISSING = 'COMM0016E'
LOG_MESSAGE_INVALID_JSON = 'COMM0017E'
LOG_MESSAGE_DEFINITION_MISSING = 'COMM0018E'
LOG_MESSAGE_REFERENCE_FILE_MISSING = 'COMM0019E'

DEFAULT_CONFIG_ROOT_ENV = 'ERP_conf_PATH'
DYNAMIC_CURRENT_EXECUTION_DATE_KEY = 'current_execution_date'
DYNAMIC_PREVIOUS_EXECUTION_DATE_KEY = 'previous_execution_date'

PARAMETER_TYPE_FIXED_VALUE = 'fixed_value'
PARAMETER_TYPE_BLANK_VALUE = 'blank_value'
PARAMETER_TYPE_INTERNAL_ID_CONVERSION = 'internal_id_conversion'
PARAMETER_TYPE_DYNAMIC_PARAMETER_DEFINITION = 'dynamic_parameter_definition'
PARAMETER_TYPE_DATE_PARAMETER = 'date_parameter'

SUPPORTED_CALLERS = {
    'InboundProcessor',
    'OutboundProcessor',
    'ESSJobProcessor',
    'ESSJobExecuteProcessor',
    'AuthManager',
    'FileOperationService',
    'S3OperationService',
}

CONFIG_FILENAME_BY_CALLER = {
    'InboundProcessor': ['CommonConfig.json', 'ERPInboundParameter.json'],
    'OutboundProcessor': ['CommonConfig.json', 'ERPOutboundParameter.json'],
    'ESSJobProcessor': ['CommonConfig.json', 'ESSJobParameter.json'],
    'ESSJobExecuteProcessor': ['CommonConfig.json', 'ESSJobParameter.json'],
    'AuthManager': ['CommonConfig.json', 'ConfigOauth.json'],
    'FileOperationService': ['CommonConfig.json', 'FileConfig.json'],
    'S3OperationService': ['CommonConfig.json', 'S3Config.json'],
}

REFERENCE_FILENAME_BY_PARAMETER_TYPE = {
    PARAMETER_TYPE_INTERNAL_ID_CONVERSION: 'COMF001.csv',
    PARAMETER_TYPE_DYNAMIC_PARAMETER_DEFINITION: 'DYNAMIC_<JOBID>.csv',
    PARAMETER_TYPE_DATE_PARAMETER: 'BASEDATE.csv',
}

LOGGER = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
# Exceptions
# ------------------------------------------------------------------------------
class ConfigManagerError(Exception):
    """Base exception for ConfigManager."""

    def __init__(self, message_id: str, message: str) -> None:
        super().__init__(message)
        self.message_id = message_id


class ConfigFileNotFoundError(ConfigManagerError):
    """Raised when a configuration file is missing."""


class InvalidConfigFormatError(ConfigManagerError):
    """Raised when a JSON configuration file is invalid."""


class DefinitionNotFoundError(ConfigManagerError):
    """Raised when a requested definition or key is missing."""


class ReferenceFileNotFoundError(ConfigManagerError):
    """Raised when a required reference file is missing."""


@dataclass
class ConfigManagerRequest:
    """Normalized request for ConfigManager."""

    caller_process_name: str
    requested_key: str
    job_id: str | None = None
    config_root_path: str | None = None
    config_root_env_name: str = DEFAULT_CONFIG_ROOT_ENV
    config_file_mapping: dict[str, list[str]] | None = None
    dynamic_parameter_mode: str = 'reference'
    runtime_args: dict[str, Any] | None = None


# ------------------------------------------------------------------------------
# Main Function Definition
# ------------------------------------------------------------------------------
def main(p_request: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    """Execute ConfigManager.

    Expected input format:
        {
            'caller_process_name': 'InboundProcessor',
            'requested_key': 'TOKEN1',
            'job_id': 'JOB001',
            'config_root_path': '/app/dev/config files',
            'runtime_args': {},
        }
    """
    d_context = initialize_variables()

    try:
        request = initialize_runtime_context(p_request)
        d_context['request'] = request
        d_context['config_root_path'] = get_config_root_path(request)

        write_start_log()

        l_target_config_files = get_target_config_files(
            request.caller_process_name,
            request.config_file_mapping,
        )
        d_context['target_config_files'] = l_target_config_files

        d_result = resolve_config_value(request, d_context['config_root_path'], l_target_config_files)
        d_result = store_result_by_caller(request.caller_process_name, d_result)

        write_success_log()
        return RETURN_CODE_NORMAL, d_result
    except ConfigManagerError:
        raise
    except Exception as e_exception:
        handle_config_manager_error(
            LOG_MESSAGE_DEFINITION_MISSING,
            'Unexpected error in ConfigManager.',
            e_exception,
        )


# ------------------------------------------------------------------------------
# Subfunction Definition
# ------------------------------------------------------------------------------
def initialize_variables() -> dict[str, Any]:
    """Initialize local variables used by the function."""
    return {
        'request': None,
        'config_root_path': '',
        'target_config_files': [],
        'result': {},
    }


def initialize_runtime_context(p_request: dict[str, Any]) -> ConfigManagerRequest:
    """Validate and normalize the input request."""
    if not isinstance(p_request, dict):
        raise TypeError('p_request must be a dictionary.')

    caller_process_name = str(p_request.get('caller_process_name', '')).strip()
    requested_key = str(p_request.get('requested_key', '')).strip()
    if not caller_process_name:
        raise ValueError('caller_process_name is required.')
    if not requested_key:
        raise ValueError('requested_key is required.')
    if caller_process_name not in SUPPORTED_CALLERS:
        raise ValueError(f'Unsupported caller_process_name: {caller_process_name}')

    return ConfigManagerRequest(
        caller_process_name=caller_process_name,
        requested_key=requested_key,
        job_id=_clean_optional_string(p_request.get('job_id')),
        config_root_path=_clean_optional_string(p_request.get('config_root_path')),
        config_root_env_name=str(
            p_request.get('config_root_env_name', DEFAULT_CONFIG_ROOT_ENV)
        ).strip() or DEFAULT_CONFIG_ROOT_ENV,
        config_file_mapping=p_request.get('config_file_mapping'),
        dynamic_parameter_mode=str(
            p_request.get('dynamic_parameter_mode', 'reference')
        ).strip() or 'reference',
        runtime_args=p_request.get('runtime_args') or {},
    )


def write_start_log() -> None:
    """Output a start log for parameter acquisition."""
    write_log(LOG_MESSAGE_START, 'Info', 'Start parameter acquisition.')


def get_config_root_path(p_request: ConfigManagerRequest) -> Path:
    """Retrieve the configuration root path from input or environment."""
    config_root = p_request.config_root_path
    if not config_root:
        config_root = os.getenv(p_request.config_root_env_name, '').strip()

    if not config_root:
        handle_config_manager_error(
            LOG_MESSAGE_CONFIG_FILE_MISSING,
            f'Configuration root path is not defined: {p_request.config_root_env_name}',
        )

    path = Path(config_root)
    if not path.exists():
        handle_config_manager_error(
            LOG_MESSAGE_CONFIG_FILE_MISSING,
            f'Configuration root path does not exist: {path}',
        )
    if not path.is_dir():
        handle_config_manager_error(
            LOG_MESSAGE_CONFIG_FILE_MISSING,
            f'Configuration root path is not a directory: {path}',
        )
    return path


def get_target_config_files(
    p_caller_process_name: str,
    p_config_file_mapping: dict[str, list[str]] | None = None,
) -> list[str]:
    """Resolve the target configuration files for the caller."""
    d_mapping = p_config_file_mapping or CONFIG_FILENAME_BY_CALLER
    l_target_files = d_mapping.get(p_caller_process_name, [])
    if not l_target_files:
        handle_config_manager_error(
            LOG_MESSAGE_CONFIG_FILE_MISSING,
            f'No configuration files defined for caller: {p_caller_process_name}',
        )
    return l_target_files


def resolve_config_value(
    p_request: ConfigManagerRequest,
    p_config_root_path: Path,
    l_target_config_files: list[str],
) -> dict[str, Any]:
    """Find the requested definition and resolve its value."""
    definition_node = None
    definition_source_file = None

    for config_filename in l_target_config_files:
        config_file_path = p_config_root_path / config_filename
        validate_config_file_exists(config_file_path)
        d_config_file = load_json_file(config_file_path)
        definition_node = find_definition_node(d_config_file, p_request.requested_key)
        if definition_node is not None:
            definition_source_file = config_filename
            break

    if definition_node is None:
        handle_config_manager_error(
            LOG_MESSAGE_DEFINITION_MISSING,
            f'Requested key not found: {p_request.requested_key}',
        )

    parameter_type = resolve_parameter_type(definition_node)
    resolved_value = resolve_parameter_value(
        parameter_type,
        definition_node,
        p_request,
        p_config_root_path,
    )

    return {
        'requested_key': p_request.requested_key,
        'parameter_type': parameter_type,
        'value': resolved_value,
        'source_file': definition_source_file,
    }


def validate_config_file_exists(p_file_path: Path) -> None:
    """Verify configuration file exists before attempting to read."""
    if not p_file_path.exists() or not p_file_path.is_file():
        handle_config_manager_error(
            LOG_MESSAGE_CONFIG_FILE_MISSING,
            f'Unable to read configuration file: {p_file_path}',
            exception_class=ConfigFileNotFoundError,
        )


def load_json_file(p_file_path: Path) -> dict[str, Any]:
    """Read and validate a JSON configuration file."""
    try:
        with p_file_path.open('r', encoding='utf-8') as config_file:
            d_config_file = json.load(config_file)
    except json.JSONDecodeError as e_exception:
        handle_config_manager_error(
            LOG_MESSAGE_INVALID_JSON,
            f'Invalid JSON format in configuration file: {p_file_path}',
            e_exception,
            exception_class=InvalidConfigFormatError,
        )
    except OSError as e_exception:
        handle_config_manager_error(
            LOG_MESSAGE_CONFIG_FILE_MISSING,
            f'Unable to read configuration file: {p_file_path}',
            e_exception,
            exception_class=ConfigFileNotFoundError,
        )

    if not isinstance(d_config_file, dict):
        handle_config_manager_error(
            LOG_MESSAGE_INVALID_JSON,
            f'Configuration root is not an object: {p_file_path}',
            exception_class=InvalidConfigFormatError,
        )
    return d_config_file


def find_definition_node(
    p_container: Any,
    p_requested_key: str,
) -> dict[str, Any] | Any | None:
    """Find the requested definition recursively inside a JSON structure.

    This function is intentionally permissive because the workbook does not
    define a single JSON schema for all config files.
    """
    if isinstance(p_container, dict):
        if p_requested_key in p_container:
            return p_container[p_requested_key]
        for key, value in p_container.items():
            if key == 'definitions' and isinstance(value, dict) and p_requested_key in value:
                return value[p_requested_key]
            result = find_definition_node(value, p_requested_key)
            if result is not None:
                return result

    if isinstance(p_container, list):
        for item in p_container:
            result = find_definition_node(item, p_requested_key)
            if result is not None:
                return result

    return None


def resolve_parameter_type(p_definition_node: Any) -> str:
    """Resolve the parameter type from a definition node.

    Supported workbook labels:
        - Fixed Value
        - Blank Value
        - Internal ID Conversion
        - Dynamic Parameter Definition
        - Date Parameter
    """
    if isinstance(p_definition_node, dict):
        raw_parameter_type = (
            p_definition_node.get('parameter_type')
            or p_definition_node.get('type')
            or p_definition_node.get('definition_type')
            or ''
        )
        raw_parameter_type = str(raw_parameter_type).strip().lower()

        d_type_map = {
            'fixed value': PARAMETER_TYPE_FIXED_VALUE,
            'blank value': PARAMETER_TYPE_BLANK_VALUE,
            'internal id conversion': PARAMETER_TYPE_INTERNAL_ID_CONVERSION,
            'dynamic parameter definition': PARAMETER_TYPE_DYNAMIC_PARAMETER_DEFINITION,
            'date parameter': PARAMETER_TYPE_DATE_PARAMETER,
            PARAMETER_TYPE_FIXED_VALUE: PARAMETER_TYPE_FIXED_VALUE,
            PARAMETER_TYPE_BLANK_VALUE: PARAMETER_TYPE_BLANK_VALUE,
            PARAMETER_TYPE_INTERNAL_ID_CONVERSION: PARAMETER_TYPE_INTERNAL_ID_CONVERSION,
            PARAMETER_TYPE_DYNAMIC_PARAMETER_DEFINITION: PARAMETER_TYPE_DYNAMIC_PARAMETER_DEFINITION,
            PARAMETER_TYPE_DATE_PARAMETER: PARAMETER_TYPE_DATE_PARAMETER,
        }
        if raw_parameter_type in d_type_map:
            return d_type_map[raw_parameter_type]

    if isinstance(p_definition_node, str):
        return PARAMETER_TYPE_FIXED_VALUE

    return PARAMETER_TYPE_FIXED_VALUE


def resolve_parameter_value(
    p_parameter_type: str,
    p_definition_node: Any,
    p_request: ConfigManagerRequest,
    p_config_root_path: Path,
) -> Any:
    """Dispatch resolution by parameter type."""
    if p_parameter_type == PARAMETER_TYPE_FIXED_VALUE:
        return process_fixed_value(p_definition_node)
    if p_parameter_type == PARAMETER_TYPE_BLANK_VALUE:
        return process_blank_value(p_definition_node)
    if p_parameter_type == PARAMETER_TYPE_INTERNAL_ID_CONVERSION:
        return process_internal_id_conversion(p_definition_node, p_request, p_config_root_path)
    if p_parameter_type == PARAMETER_TYPE_DYNAMIC_PARAMETER_DEFINITION:
        return process_dynamic_parameter_definition(p_definition_node, p_request, p_config_root_path)
    if p_parameter_type == PARAMETER_TYPE_DATE_PARAMETER:
        return process_date_parameter(p_definition_node, p_request, p_config_root_path)

    handle_config_manager_error(
        LOG_MESSAGE_DEFINITION_MISSING,
        f'Invalid Parameter Type Definition: {p_request.requested_key}',
        exception_class=DefinitionNotFoundError,
    )


def process_fixed_value(p_definition_node: Any) -> Any:
    """Return the raw configured value."""
    if isinstance(p_definition_node, dict):
        for key_name in ('value', 'default', 'configured_value'):
            if key_name in p_definition_node:
                return deepcopy(p_definition_node[key_name])
    return deepcopy(p_definition_node)


def process_blank_value(p_definition_node: Any) -> str:
    """Return a blank value.

    The workbook describes this only as 'A blank value', so this implementation
    returns an empty string by default.
    """
    if isinstance(p_definition_node, dict):
        if 'blank_value' in p_definition_node:
            return str(p_definition_node['blank_value'])
        if 'value' in p_definition_node and p_definition_node['value'] in (None, ''):
            return ''
    return ''


def process_internal_id_conversion(
    p_definition_node: Any,
    p_request: ConfigManagerRequest,
    p_config_root_path: Path,
) -> Any:
    """Resolve a value using COMF001.csv."""
    reference_file_path = p_config_root_path / 'COMF001.csv'
    validate_reference_file_exists(reference_file_path)

    l_rows = load_csv_reference_file(reference_file_path)
    lookup_key = _extract_lookup_key(p_definition_node, p_request.requested_key)
    converted_value = find_reference_value(l_rows, lookup_key)
    if converted_value is None:
        handle_config_manager_error(
            LOG_MESSAGE_DEFINITION_MISSING,
            f'Invalid Parameter Type Definition: {lookup_key}',
            exception_class=DefinitionNotFoundError,
        )
    return converted_value


def process_dynamic_parameter_definition(
    p_definition_node: Any,
    p_request: ConfigManagerRequest,
    p_config_root_path: Path,
) -> Any:
    """Resolve or generate dynamic parameter data.

    The workbook is internally inconsistent here:
        - Function overview says dynamic parameters are managed by another common function.
        - Detailed flow also says a dynamic parameter file will be generated.

    This implementation supports both modes:
        - 'reference': return a structured reference only
        - 'generate': read/update the dynamic CSV file
    """
    if not p_request.job_id:
        handle_config_manager_error(
            LOG_MESSAGE_DEFINITION_MISSING,
            'job_id is required for dynamic parameter processing.',
            exception_class=DefinitionNotFoundError,
        )

    filename = f'DYNAMIC_{p_request.job_id}.csv'
    dynamic_file_path = p_config_root_path / filename
    validate_reference_file_exists(dynamic_file_path)

    if p_request.dynamic_parameter_mode == 'reference':
        return {
            'job_id': p_request.job_id,
            'reference_file': filename,
            'definition': deepcopy(p_definition_node),
        }

    l_rows = load_csv_reference_file(dynamic_file_path)
    lookup_key = _extract_lookup_key(p_definition_node, p_request.requested_key)
    dynamic_value = find_reference_value(l_rows, lookup_key)
    if dynamic_value is None:
        handle_config_manager_error(
            LOG_MESSAGE_DEFINITION_MISSING,
            f'Dynamic parameter definition is missing: {lookup_key}',
            exception_class=DefinitionNotFoundError,
        )

    previous_execution_date = dynamic_value
    current_execution_date = datetime.now().strftime('%Y%m%d%H%M%S')
    return {
        'lookup_key': lookup_key,
        'value': dynamic_value,
        DYNAMIC_PREVIOUS_EXECUTION_DATE_KEY: previous_execution_date,
        DYNAMIC_CURRENT_EXECUTION_DATE_KEY: current_execution_date,
        'reference_file': filename,
    }


def process_date_parameter(
    p_definition_node: Any,
    p_request: ConfigManagerRequest,
    p_config_root_path: Path,
) -> str:
    """Resolve a date parameter using BASEDATE.csv."""
    reference_file_path = p_config_root_path / 'BASEDATE.csv'
    validate_reference_file_exists(reference_file_path)

    l_rows = load_csv_reference_file(reference_file_path)
    lookup_key = _extract_lookup_key(p_definition_node, p_request.requested_key)
    actual_date_value = find_reference_value(l_rows, lookup_key)
    if actual_date_value is None:
        handle_config_manager_error(
            LOG_MESSAGE_DEFINITION_MISSING,
            f'Date parameter definition is missing: {lookup_key}',
            exception_class=DefinitionNotFoundError,
        )

    if isinstance(p_definition_node, dict):
        template = str(
            p_definition_node.get('format_template')
            or p_definition_node.get('value')
            or '{value}'
        )
        if '{value}' in template:
            return template.replace('{value}', str(actual_date_value))

    return str(actual_date_value)


def validate_reference_file_exists(p_file_path: Path) -> None:
    """Validate that a reference CSV file exists."""
    if not p_file_path.exists() or not p_file_path.is_file():
        handle_config_manager_error(
            LOG_MESSAGE_REFERENCE_FILE_MISSING,
            f'The reference file does not exist: {p_file_path.name}',
            exception_class=ReferenceFileNotFoundError,
        )


def load_csv_reference_file(p_file_path: Path) -> list[dict[str, Any]]:
    """Load a CSV file into a list of dictionaries."""
    try:
        with p_file_path.open('r', encoding='utf-8-sig', newline='') as csv_file:
            reader = csv.DictReader(csv_file)
            return [dict(row) for row in reader]
    except OSError as e_exception:
        handle_config_manager_error(
            LOG_MESSAGE_REFERENCE_FILE_MISSING,
            f'Unable to read reference file: {p_file_path.name}',
            e_exception,
            exception_class=ReferenceFileNotFoundError,
        )


def find_reference_value(
    l_rows: list[dict[str, Any]],
    p_lookup_key: str,
) -> Any | None:
    """Find a reference value using common CSV key conventions."""
    normalized_lookup_key = p_lookup_key.strip().lower()

    for row in l_rows:
        d_row = {str(key).strip().lower(): value for key, value in row.items()}

        candidate_keys = [
            d_row.get('key'),
            d_row.get('parameter_name'),
            d_row.get('name'),
            d_row.get('code'),
            d_row.get('keyword'),
        ]
        if any(str(candidate or '').strip().lower() == normalized_lookup_key for candidate in candidate_keys):
            for value_key in ('value', 'internal_id', 'converted_value', 'date_value', 'actual_date'):
                if value_key in d_row and d_row[value_key] not in (None, ''):
                    return d_row[value_key]
            for key_name, value in row.items():
                if str(key_name).strip().lower() not in {
                    'key', 'parameter_name', 'name', 'code', 'keyword'
                } and value not in (None, ''):
                    return value
    return None


def store_result_by_caller(
    p_caller_process_name: str,
    d_result: dict[str, Any],
) -> dict[str, Any]:
    """Return result in a caller-safe structure.

    The workbook mentions dynamic global variables for some callers, but this
    implementation returns a dictionary so the caller can decide scope and
    lifetime safely.
    """
    d_output = deepcopy(d_result)
    d_output['caller_process_name'] = p_caller_process_name
    d_output['storage_mode'] = 'returned_dictionary'
    return d_output


def write_success_log() -> None:
    """Output a success log."""
    write_log(LOG_MESSAGE_SUCCESS, 'Info', 'ConfigManager call function is successful.')


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


def handle_config_manager_error(
    p_message_id: str,
    p_message: str,
    p_exception: Exception | None = None,
    *,
    exception_class: type[ConfigManagerError] = ConfigManagerError,
) -> None:
    """Write error log and raise a structured exception."""
    write_log(p_message_id, 'Error', p_message)
    if p_exception is None:
        raise exception_class(p_message_id, p_message)
    raise exception_class(p_message_id, f'{p_message} | cause={p_exception}') from p_exception


def _clean_optional_string(p_value: Any) -> str | None:
    """Normalize optional string input."""
    if p_value is None:
        return None
    cleaned_value = str(p_value).strip()
    return cleaned_value or None


def _extract_lookup_key(p_definition_node: Any, p_default_key: str) -> str:
    """Extract a lookup key from a definition node."""
    if isinstance(p_definition_node, dict):
        for key_name in ('lookup_key', 'key', 'reference_key', 'value'):
            if key_name in p_definition_node and p_definition_node[key_name] not in (None, ''):
                return str(p_definition_node[key_name]).strip()
    return p_default_key


# ------------------------------------------------------------------------------
# Main Processing Execution
# ------------------------------------------------------------------------------
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Execute ConfigManager.')
    parser.add_argument('--caller-process-name', required=True)
    parser.add_argument('--requested-key', required=True)
    parser.add_argument('--job-id')
    parser.add_argument('--config-root-path')
    parser.add_argument('--dynamic-parameter-mode', default='reference')

    args = parser.parse_args()

    request = {
        'caller_process_name': args.caller_process_name,
        'requested_key': args.requested_key,
        'job_id': args.job_id,
        'config_root_path': args.config_root_path,
        'dynamic_parameter_mode': args.dynamic_parameter_mode,
    }

    rc, result = main(request)
    print(json.dumps({'return_code': rc, 'result': result}, ensure_ascii=False, indent=4))

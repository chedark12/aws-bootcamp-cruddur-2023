# ============================================================
# constants.py
# Contains: message IDs, return codes, valid callers, file list
# ============================================================
# ------------------------------------------------------------
# (General) Return Codes
# ------------------------------------------------------------
RETURN_NORMAL = 0        # Normal End
RETURN_WARNING = 1       # Warning End
RETURN_ABNORMAL = 8      # Abnormal End
# ------------------------------------------------------------
# (General) Message IDs based on requirements specification
# ------------------------------------------------------------
MESSAGE_COMM0001I = "COMM0001I"     # Successful operation log
MESSAGE_COMM0016E = "COMM0016E"     # Config file missing
MESSAGE_COMM0017E = "COMM0017E"     # JSON parsing failure
MESSAGE_COMM0018E = "COMM0018E"     # Key not found
MESSAGE_COMM0019E = "COMM0019E"     # Required supporting file missing
MESSAGE_COMM0033E = 'COMM0033E'
MESSAGE_COMM0034E = 'COMM0034E'
MESSAGE_COMM0012E = 'COMM0012E'
MESSAGE_COMM0002I = 'COMM0002I'
# ------------------------------------------------------------
# (Allowed Callers)
# ------------------------------------------------------------
VALID_CALLERS = {
    "InboundProcessor",
    "OutboundProcessor",
    "ESSJobExecuteProcessor",
    "AuthManager",
    "S3OperationService",
    "FileOperationService"
}
# ------------------------------------------------------------
# (List of Valid Configuration Files)
# ------------------------------------------------------------
VALID_CONFIG_FILES = {
    "CommonConfig": "CommonConfig.json",
    "ESSJobParameter": "ESSJobParameter.json",
    "ERPInboundParameter": "ERPInboundParameter.json",
    "ERPOutboundParameter": "ERPOutboundParameter.json",
    "ConfigOauth": "ConfigOauth.json",
    "COMF001": "COMF001.csv",
    "S3Config": "S3Config.json",
    "FileConfig": "FileConfig.json",
    "BASEDATE": "BASEDATE.csv",
    "ErpMessage": "ErpMessage.json",
    "ConfigOCILog": "ConfigOCILog.json"
    # DYNAMIC_<JOBID>.csv handled separately (Step 5.3)
}
# ------------------------------------------------------------
# (Parameter Types - Based on Section 5.1)
# ------------------------------------------------------------
PARAM_TYPE_FIXED = "fixed"
PARAM_TYPE_BLANK = "blank"
PARAM_TYPE_INTERNAL_ID = "internal_id_conversion"
PARAM_TYPE_DYNAMIC = "dynamic_parameter"
PARAM_TYPE_DATE = "date_parameter"
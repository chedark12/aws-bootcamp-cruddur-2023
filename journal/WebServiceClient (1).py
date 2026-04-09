#==================================================================================================
# Script Name : WebServiceClient.py
# Function Name : Web Service Client
# Process Overview : Function to run and execute SOAP envelopes
# Execution User   : ??
# Arguments        : 1. endpoint_url: the URL of the SOAP web service endpoint
#                  : 2. soap_envelope: the SOAP envelope to be sent
# Returns          : 1. Result 0: Normal Termination | 1: Warning Termination | 8: Abnormal Termination
# Author           : IBMPH - Kylee
# Change log:
# 1.00| 2026/4/6| IBMPH - Kylee| Initial development
# Change log (in development):
# 1.00| 2026/4/6| IBMPH - Kylee| Initial development
# 2.00| 2026/4/7| IBMPH - Kylee| Added retry mechanism for web service execution
# 3.00| 2026/4/8| IBMPH - Kylee| Added temporary soap envelope and endpoint URL for testing
# 4.00| 2026/4/8| IBMPH - Kylee| Working soap when tested the ucmupload, need to clean up the code and remove the temporary testing part
#==================================================================================================

#--------------------------------------------------------------------------------------------------
#Preparation:
#--------------------------------------------------------------------------------------------------
# General libraries:
import os
import sys
import logging
import datetime as dt
import json
import requests
import base64 # for file encoding, subject for deletion, just for testing
# Function Specific Common External Scripts:
import OERLogger
import OERExceptions
import AuthManager
import xmltrial
#--------------------------------------------------------------------------------------------------

#--------------------------------------------------------------------------------------------------
# Main Function Definition:
#--------------------------------------------------------------------------------------------------
def main(p_endpoint_url, p_soap_envelope):
    
    #(1) Initialize the variables.
    access_token = ""
    MAX_RETRIES = 3 #Will be replaced soon as this should be env var.
    
    #(2) Log the start of the process.
    logging.critical(f'Start Run_process for WebService')
    try:
        #(3) Execute Auth Manager to retrieve the access token.
        logging.critical(f'Calling Auth Manager to retrieve access token.')
        return_code, access_token = AuthManager.authenticate()
        if return_code != 0:
            raise Exception(f'Failed to retrieve access token.')
        else:
            logging.critical(f'Access token retrieved successfully.')


        # ----------------------- This is from xml_service, subject for deletion, just for testing -----------------------#
            file_name = "C:\\Users\\P1014IPH1\\OneDrive - IBM\\Desktop\\Inbound Processor\\Test.zip"
            with open(file_name, 'rb') as file:
                enc = base64.b64encode(file.read())
                enc_content = enc.decode('UTF8')
            envelope_uploadFileToUcm = '''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:typ="http://xmlns.oracle.com/apps/financials/commonModules/shared/model/erpIntegrationService/types/" xmlns:erp="http://xmlns.oracle.com/apps/financials/commonModules/shared/model/erpIntegrationService/">
            <soapenv:Header/>
            <soapenv:Body>
              <typ:uploadFileToUcm>
                 <typ:document>
                    <erp:Content>{0}</erp:Content>
                    <erp:FileName>{1}</erp:FileName>
                    <erp:ContentType>zip</erp:ContentType>
                    <erp:DocumentTitle>{2}</erp:DocumentTitle>
                    <erp:document_author>{3}</erp:document_author>
                    <erp:DocumentSecurityGroup>FAFusionImportExport</erp:DocumentSecurityGroup>
                    <erp:DocumentAccount>{4}</erp:DocumentAccount>
                    <erp:DocumentName></erp:DocumentName>
                    <erp:DocumentId></erp:DocumentId>
                 </typ:document>
              </typ:uploadFileToUcm>
            </soapenv:Body>
            </soapenv:Envelope>'''.format(enc_content, file_name, "Test.zip", "Kairou-san", "prc/supplier/import")

            print(envelope_uploadFileToUcm)
        # ----------------------- This is from xml_service, subject for deletion, just for testing -----------------------#


        #(4) Call the function to run the web service client.
        retry_count = 0
        while retry_count < MAX_RETRIES:
            result, response = run_websvc(p_endpoint_url, envelope_uploadFileToUcm, access_token)
            if result == 0:
                logging.critical(f'Web service executed successfully.')
                print(f'Web service response: {response}')
                return 0, None
            else:
                logging.critical(f'Web service execution failed on attempt {retry_count + 1}.')
                retry_count += 1
                if retry_count < MAX_RETRIES:
                    logging.critical(f'Retrying... (Attempt {retry_count + 1} of {MAX_RETRIES})')
                else:
                    raise Exception(f'Web service execution failed after {MAX_RETRIES} attempts.')
        
    except Exception as error:
        o_ErrorMessage = str(error).strip() 
        OERExceptions.handle_exception("COMM0003W", str(o_ErrorMessage))
        return 1, o_ErrorMessage
        
#--------------------------------------------------------------------------------------------------
# Subfunction Definition:
#--------------------------------------------------------------------------------------------------
def run_websvc(p_endpoint_url, p_soap_envelope, access_token):
    print(p_soap_envelope)
    # Send the SOAP request
    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'Authorization': f'Bearer {access_token}'
    }
    try:                                        
        response = requests.post(p_endpoint_url, data=p_soap_envelope, headers=headers)
        response.raise_for_status()
        logging.critical('SOAP request successful.')
        return 0, response.text
    except requests.exceptions.RequestException as e:
        logging.critical(f'SOAP request failed: {str(e)}')
        return 8, str(e)


#--------------------------------------------------------------------------------------------------
# Main Processing Execution:
#--------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    # p_endpoint_url = sys.argv[1]
    # p_soap_envelope = sys.argv[2]
    # main(p_endpoint_url, p_soap_envelope)

    p_endpoint_url = "https://iacfou-dev2.fa.ocs.oraclecloud.com:443/fscmService/ErpIntegrationService"
    p_soap_envelope = """<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:sch="http://xmlns.oracle.com/oxp/service/ScheduleReportService">
        <soap:Header/>
        <soap:Body>
                <sch:getAllJobInstanceIDs>
                        <submittedJobId></submittedJobId>
                        <schedulereportid>B23</schedulereportid>
                </sch:getAllJobInstanceIDs>
        </soap:Body>
</soap:Envelope>"""
    main(p_endpoint_url, p_soap_envelope)


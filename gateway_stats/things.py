from http.client import GATEWAY_TIMEOUT
from sqlite3 import DatabaseError
import requests
from requests.exceptions import HTTPError


class EndDeviceRegistryService():
    def __init__(self, DOMAIN_NAME = '', APP_NAME = '', API_KEY = ''):
        self.DOMAIN_NAME = DOMAIN_NAME.rstrip(DOMAIN_NAME[-1]) if DOMAIN_NAME[-1] == '/' else DOMAIN_NAME
        self.APP_NAME = APP_NAME
        self.API_KEY = API_KEY
        self.headers = {'Authorization': f'Bearer {self.API_KEY}'}

    
    def ListEndDevices(self, DATA = ''):
        try:
            
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/applications/{self.APP_NAME}/devices', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    def CreateEndDevice(self, DATA = ''):
        try:
            response = requests.post(f'{self.DOMAIN_NAME}/api/v3/applications/{self.APP_NAME}/devices', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text
        
    def GetEndDevice(self, END_DEVICE_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/applications/{self.APP_NAME}/devices/{END_DEVICE_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text
    
    def UpdateEndDevice(self, END_DEVICE_ID, DATA = ''):
        """
            Not Yet Tested
        """
        try:
            response = requests.put(f'{self.DOMAIN_NAME}/api/v3/applications/{self.APP_NAME}/devices/{END_DEVICE_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text
    
    
    def UpdateEndDevice(self, END_DEVICE_ID, DATA = ''):
        """
            Not Yet Tested
        """
        try:
            response = requests.put(f'{self.DOMAIN_NAME}/api/v3/applications/{self.APP_NAME}/devices/{END_DEVICE_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text
        
    def DeleteEndDevice(self, END_DEVICE_ID, DATA = ''):
        """
            Not Yet Tested
        """
        try:
            response = requests.delete(f'{self.DOMAIN_NAME}/api/v3/applications/{self.APP_NAME}/devices/{END_DEVICE_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

class JsEndDeviceRegistryService():
    def __init__(self, DOMAIN_NAME = '', APP_NAME = '', API_KEY = ''):
        self.DOMAIN_NAME = DOMAIN_NAME.rstrip(DOMAIN_NAME[-1]) if DOMAIN_NAME[-1] == '/' else DOMAIN_NAME
        self.APP_NAME = APP_NAME
        self.API_KEY = API_KEY
        self.headers = {'Authorization': f'Bearer {self.API_KEY}'}

    def SetEndDevice(self, END_DEVICE_ID, DATA = ''):
        """
            Not Yet Tested
        """
        try:
            response = requests.put(f'{self.DOMAIN_NAME}/api/v3/js/applications/{self.APP_NAME}/devices/{END_DEVICE_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text
    
    def SetEndDevice(self, DATA = ''):
        """
            Not Yet Tested
        """
        try:
            
            response = requests.post(f'{self.DOMAIN_NAME}/api/v3/js/applications/{self.APP_NAME}/devices', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text
    
    def GetEndDevice(self, END_DEVICE_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/js/applications/{self.APP_NAME}/devices/{END_DEVICE_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text
    

    def DeleteEndDevice(self, END_DEVICE_ID, DATA = ''):
        """
            Not Yet Tested
        """
        try:
            response = requests.delete(f'{self.DOMAIN_NAME}/api/v3/js/applications/{self.APP_NAME}/devices/{END_DEVICE_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

class NsEndDeviceRegistryService():
    def __init__(self, DOMAIN_NAME = '', APP_NAME = '', API_KEY = ''):
        self.DOMAIN_NAME = DOMAIN_NAME.rstrip(DOMAIN_NAME[-1]) if DOMAIN_NAME[-1] == '/' else DOMAIN_NAME
        self.APP_NAME = APP_NAME
        self.API_KEY = API_KEY
        self.headers = {'Authorization': f'Bearer {self.API_KEY}'}

    def SetEndDevice(self, END_DEVICE_ID, DATA = ''):
        """
            Not Yet Tested
        """
        try:
            response = requests.put(f'{self.DOMAIN_NAME}/api/v3/ns/applications/{self.APP_NAME}/devices/{END_DEVICE_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text
    
    def SetEndDevice(self, DATA = ''):
        """
            Not Yet Tested
        """
        try:
            
            response = requests.post(f'{self.DOMAIN_NAME}/api/v3/ns/applications/{self.APP_NAME}/devices', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text
    
    def GetEndDevice(self, END_DEVICE_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/ns/applications/{self.APP_NAME}/devices/{END_DEVICE_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text
    

    def DeleteEndDevice(self, END_DEVICE_ID, DATA = ''):
        """
            Not Yet Tested
        """
        try:
            response = requests.delete(f'{self.DOMAIN_NAME}/api/v3/ns/applications/{self.APP_NAME}/devices/{END_DEVICE_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

class AsEndDeviceRegistryService():
    def __init__(self, DOMAIN_NAME = '', APP_NAME = '', API_KEY = ''):
        self.DOMAIN_NAME = DOMAIN_NAME.rstrip(DOMAIN_NAME[-1]) if DOMAIN_NAME[-1] == '/' else DOMAIN_NAME
        self.APP_NAME = APP_NAME
        self.API_KEY = API_KEY
        self.headers = {'Authorization': f'Bearer {self.API_KEY}'}

    def SetEndDevice(self, END_DEVICE_ID, DATA = ''):
        """
            Not Yet Tested
        """
        try:
            response = requests.put(f'{self.DOMAIN_NAME}/api/v3/as/applications/{self.APP_NAME}/devices/{END_DEVICE_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text
    
    def SetEndDevice(self, DATA = ''):
        """
            Not Yet Tested
        """
        try:
            
            response = requests.post(f'{self.DOMAIN_NAME}/api/v3/as/applications/{self.APP_NAME}/devices', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text
    
    def GetEndDevice(self, END_DEVICE_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/as/applications/{self.APP_NAME}/devices/{END_DEVICE_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text
    

    def DeleteEndDevice(self, END_DEVICE_ID, DATA = ''):
        """
            Not Yet Tested
        """
        try:
            response = requests.delete(f'{self.DOMAIN_NAME}/api/v3/as/applications/{self.APP_NAME}/devices/{END_DEVICE_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

class EndDeviceClaimingServerService():
    def __init__(self, DOMAIN_NAME = '', APP_NAME = '', API_KEY = ''):
        self.DOMAIN_NAME = DOMAIN_NAME.rstrip(DOMAIN_NAME[-1]) if DOMAIN_NAME[-1] == '/' else DOMAIN_NAME
        self.APP_NAME = APP_NAME
        self.API_KEY = API_KEY
        self.headers = {'Authorization': f'Bearer {self.API_KEY}'}

    def AuthorizeApplication(self, DATA = ''):
        """
            Not Yet Tested
        """
        try:
            response = requests.post(f'{self.DOMAIN_NAME}/api/v3/edcs/applications/{self.APP_NAME}/authorize', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


    def ClaimEndDevice(self, DATA = ''):
        """
            Not Yet Tested
        """
        try:
            response = requests.post(f'{self.DOMAIN_NAME}/api/v3/edcs/claim', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text
        

    def GetClaimStatus(self, END_DEVICE_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/edcs/claim/{self.APP_NAME}/devices/{END_DEVICE_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    def GetInfoByJoinEUI(self, DATA = ''):
        """
            Not Yet Tested
        """
        try:
            response = requests.post(f'{self.DOMAIN_NAME}/api/v3/edcs/claim/info', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


    def UnauthorizeApplication(self, DATA = ''):
        """
            Not Yet Tested
        """
        try:
            response = requests.delete(f'{self.DOMAIN_NAME}/api/v3/edcs/applications/{self.APP_NAME}/authorize', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


    def UnclaimEndDevice(self, END_DEVICE_ID, DATA = ''):
        """
            Not Yet Tested
        """
        try:
            response = requests.delete(f'{self.DOMAIN_NAME}/api/v3/edcs/applications/{self.APP_NAME}/devices/{END_DEVICE_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


class DeviceRepositoryService():
    def __init__(self, DOMAIN_NAME = '', APP_NAME = '', API_KEY = ''):
        self.DOMAIN_NAME = DOMAIN_NAME.rstrip(DOMAIN_NAME[-1]) if DOMAIN_NAME[-1] == '/' else DOMAIN_NAME
        self.APP_NAME = APP_NAME
        self.API_KEY = API_KEY
        self.headers = {'Authorization': f'Bearer {self.API_KEY}'}
    

    def ListEndDeviceBrands(self, APP_NAME, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/dr/applications/{APP_NAME}/brands', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    def ListEndDeviceBrands(self, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/dr/brands', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    
    def GetEndDeviceBrand(self, BRAND_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/dr/brands/{BRAND_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


    def GetEndDeviceBrand(self, APP_NAME, BRAND_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/dr/applications/{APP_NAME}/brands/{BRAND_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    
    def ListEndDeviceModels(self, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/dr/models', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


    def ListEndDeviceModels(self, APP_NAME, BRAND_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/dr/applications/{APP_NAME}/brands/{BRAND_ID}/models', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


    def ListEndDeviceModels(self, APP_NAME, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/dr/applications/{APP_NAME}/models', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


    def ListEndDeviceModels(self, BRAND_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/dr/brands/{BRAND_ID}/models', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


    def GetEndDeviceModel(self, BRAND_ID, MODEL_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/dr/brands/{BRAND_ID}/models/{MODEL_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    def ListEndDeviceModels(self, APP_NAME, BRAND_ID, MODEL_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/dr/applications/{APP_NAME}/brands/{BRAND_ID}/models/{MODEL_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


    def GetTemplate(self, BRAND_ID, MODEL_ID, FIRMWARE_VERSION, BAND_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/dr/brands/{BRAND_ID}/models/{MODEL_ID}/{FIRMWARE_VERSION}/{BAND_ID}/template', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    def GetTemplate(self, VENDOR_ID, VENDOR_PROFILE, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/dr/vendors/{VENDOR_ID}/profiles/{VENDOR_PROFILE}/template', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


    def GetTemplate(self, APP_ID, BRAND_ID, MODEL_ID, FIRMWARE_VERSION, BAND_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/dr/applications/{APP_ID}/brands/{BRAND_ID}/models/{MODEL_ID}/{FIRMWARE_VERSION}/{BAND_ID}/template', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


    def GetTemplate(self, APP_ID, VENDOR_ID, VENDOR_PROFILE, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/dr/applications/{APP_ID}/vendors/{VENDOR_ID}/profiles/{VENDOR_PROFILE}/template', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text
        

    def GetDecoder(self, APP_ID, BRAND_ID, MODEL_ID, FIRMWARE_VERSION, BAND_ID, FORMATTER, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/dr/applications/{APP_ID}/brands/{BRAND_ID}/models/{MODEL_ID}/{FIRMWARE_VERSION}/{BAND_ID}/formatters/{FORMATTER}/decoder', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text    


    def GetDecoder(self, BRAND_ID, MODEL_ID, FIRMWARE_VERSION, BAND_ID, FORMATTER, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/dr/brands/{BRAND_ID}/models/{MODEL_ID}/{FIRMWARE_VERSION}/{BAND_ID}/formatters/{FORMATTER}/decoder', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    
    def GetEncoder(self, APP_ID, BRAND_ID, MODEL_ID, FIRMWARE_VERSION, BAND_ID, FORMATTER, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/dr/applications/{APP_ID}/brands/{BRAND_ID}/models/{MODEL_ID}/{FIRMWARE_VERSION}/{BAND_ID}/formatters/{FORMATTER}/encoder', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text    


    def GetEncoder(self, BRAND_ID, MODEL_ID, FIRMWARE_VERSION, BAND_ID, FORMATTER, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/dr/brands/{BRAND_ID}/models/{MODEL_ID}/{FIRMWARE_VERSION}/{BAND_ID}/formatters/{FORMATTER}/encoder', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


class EndDeviceQRCodeGeneratorService():
    def __init__(self, DOMAIN_NAME = '', APP_NAME = '', API_KEY = ''):
        self.DOMAIN_NAME = DOMAIN_NAME.rstrip(DOMAIN_NAME[-1]) if DOMAIN_NAME[-1] == '/' else DOMAIN_NAME
        self.APP_NAME = APP_NAME
        self.API_KEY = API_KEY
        self.headers = {'Authorization': f'Bearer {self.API_KEY}'}
    

    def GetQRCodeFormat(self, FORMAT_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/qr-codes/end-devices/formats/{FORMAT_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


    def ListFormats(self, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/qr-codes/end-devices/formats', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


    def ListFormats(self, DATA = ''):
        try:
            response = requests.post(f'{self.DOMAIN_NAME}/api/v3/qr-codes/end-devices', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


    def ParseEndDeviceQRCode(self, DATA = ''):
        try:
            response = requests.post(f'{self.DOMAIN_NAME}/api/v3/qr-code/end-devices/parse', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


    def ParseEndDeviceQRCode(self, FORMAT_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/qr-codes/end-devices/{FORMAT_ID}/parse', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


class GatewayRegistryService():
    def __init__(self, DOMAIN_NAME = '', APP_NAME = '', API_KEY = ''):
        self.DOMAIN_NAME = DOMAIN_NAME.rstrip(DOMAIN_NAME[-1]) if DOMAIN_NAME[-1] == '/' else DOMAIN_NAME
        self.APP_NAME = APP_NAME
        self.API_KEY = API_KEY
        self.headers = {'Authorization': f'Bearer {self.API_KEY}'}


    def CreateGateway(self, USER_ID, DATA = ''):
        try:
            response = requests.post(f'{self.DOMAIN_NAME}/api/v3/users/{USER_ID}/gateways', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    def CreateGateway(self, ORG_ID, DATA = ''):
        try:
            response = requests.post(f'{self.DOMAIN_NAME}/api/v3/organizations/{ORG_ID}/gateways', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


    def GetGateway(self, GATEWAY_ID, PARAMS = '', DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/gateways/{GATEWAY_ID}', headers=self.headers, params=PARAMS, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text
    

    def ListGateways(self, USER_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/users/{USER_ID}/gateways', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text
        
    def ListGateways(self, ORG_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/organizations/{ORG_ID}/gateways', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    def ListGateways(self, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/gateways', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


    def UpdateGateway(self, GATEWAY_ID, DATA = ''):
        try:
            response = requests.put(f'{self.DOMAIN_NAME}/api/v3/gateways/{GATEWAY_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    def RestoreGateway(self, GATEWAY_ID, DATA = ''):
        try:
            response = requests.post(f'{self.DOMAIN_NAME}/api/v3/gateways/{GATEWAY_ID}/restore', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text



    def PurgeGateway(self, GATEWAY_ID, DATA = ''):
        try:
            response = requests.delete(f'{self.DOMAIN_NAME}/api/v3/gateways/{GATEWAY_ID}/purge', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text



class EntityRegistrySearchService():
    def __init__(self, DOMAIN_NAME = '', API_KEY = ''):
        self.DOMAIN_NAME = DOMAIN_NAME.rstrip(DOMAIN_NAME[-1]) if DOMAIN_NAME[-1] == '/' else DOMAIN_NAME
        self.API_KEY = API_KEY
        self.headers = {'Authorization': f'Bearer {self.API_KEY}'}


    def SearchGateways(self, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/search/gateways', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


class GatewayAccessService():
    def __init__(self, DOMAIN_NAME = '', APP_NAME = '', API_KEY = ''):
        self.DOMAIN_NAME = DOMAIN_NAME.rstrip(DOMAIN_NAME[-1]) if DOMAIN_NAME[-1] == '/' else DOMAIN_NAME
        self.APP_NAME = APP_NAME
        self.API_KEY = API_KEY
        self.headers = {'Authorization': f'Bearer {self.API_KEY}'}

    def ListRights(self, GATEWAY_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/gateways/{GATEWAY_ID}/rights', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text 

    def CreateAPIKey(self, GATEWAY_ID, DATA = ''):
        try:
            response = requests.post(f'{self.DOMAIN_NAME}/api/v3/gateways/{GATEWAY_ID}/api-keys', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text 

    def ListGatewayAPIKeys(self, GATEWAY_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/gateways/{GATEWAY_ID}/api-keys', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text
    
    def GetGatewayAPIKey(self, GATEWAY_ID, KEY_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/gateways/{GATEWAY_ID}/api-keys/{KEY_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text 

    def UpdateGatewayAPIKey(self, GATEWAY_ID, KEY_ID, DATA = ''):
        try:
            response = requests.put(f'{self.DOMAIN_NAME}/api/v3/gateways/{GATEWAY_ID}/api-keys/{KEY_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text 

    def GetGatewayCollaborator(self, GATEWAY_ID, USER_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/gateways/{GATEWAY_ID}/collaborator/user/{USER_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text 

    def GetGatewayCollaborator(self, GATEWAY_ID, ORG_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/gateways/{GATEWAY_ID}/collaborator/organization/{ORG_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text 
    
    def SetGatewayCollaborator(self, GATEWAY_ID, DATA = ''):
        try:
            response = requests.put(f'{self.DOMAIN_NAME}/api/v3/gateways/{GATEWAY_ID}/collaborators', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text 
    
    def ListGatewayCollaborators(self, GATEWAY_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/gateways/{GATEWAY_ID}/collaborators', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

class ConfigurationService():
    def __init__(self, DOMAIN_NAME = '', API_KEY = ''):
        self.DOMAIN_NAME = DOMAIN_NAME.rstrip(DOMAIN_NAME[-1]) if DOMAIN_NAME[-1] == '/' else DOMAIN_NAME
        self.API_KEY = API_KEY
        self.headers = {'Authorization': f'Bearer {self.API_KEY}'}


    def SearchGateways(self, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/configuration/frequency-plans', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


class GatewayClaimingServerService():
    def __init__(self, DOMAIN_NAME = '', API_KEY = ''):
        self.DOMAIN_NAME = DOMAIN_NAME.rstrip(DOMAIN_NAME[-1]) if DOMAIN_NAME[-1] == '/' else DOMAIN_NAME
        self.API_KEY = API_KEY
        self.headers = {'Authorization': f'Bearer {self.API_KEY}'}


    def AuthorizeGateway(self, GATEWAY_ID, DATA = ''):
        try:
            response = requests.post(f'{self.DOMAIN_NAME}/api/v3/gcls/gateways/{GATEWAY_ID}/authorize', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


    def ClaimGateway(self, DATA = ''):
        try:
            response = requests.post(f'{self.DOMAIN_NAME}/api/v3/gcls/claim', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    def GetInfoByGatewayEUI(self, DATA = ''):
        try:
            response = requests.post(f'{self.DOMAIN_NAME}/api/v3/gcls/claim/info', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


    def UnauthorizeGateway(self, GATEWAY_ID, DATA = ''):
        try:
            response = requests.delete(f'{self.DOMAIN_NAME}/api/v3/gcls/gateways/{GATEWAY_ID}/authorize', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

class GatewayServerService():
    def __init__(self, DOMAIN_NAME = '', API_KEY = ''):
        self.DOMAIN_NAME = DOMAIN_NAME.rstrip(DOMAIN_NAME[-1]) if DOMAIN_NAME[-1] == '/' else DOMAIN_NAME
        self.API_KEY = API_KEY
        self.headers = {'Authorization': f'Bearer {self.API_KEY}'}

    def GetGatewayConnectionStats(self, GATEWAY_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/gs/gateways/{GATEWAY_ID}/connection/stats', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text    


    def BatchGetGatewayConnectionStats(self, DATA = ''):
        try:
            response = requests.post(f'{self.DOMAIN_NAME}/api/v3/gs/gateways/connection/stats', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    def BatchGetGatewayConnectionStats(self, DATA = ''):
        try:
            response = requests.post(f'{self.DOMAIN_NAME}/api/v3/gs/gateways/connection/stats', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

class GatewayConfigurationService():
    def __init__(self, DOMAIN_NAME = '', API_KEY = ''):
        self.DOMAIN_NAME = DOMAIN_NAME.rstrip(DOMAIN_NAME[-1]) if DOMAIN_NAME[-1] == '/' else DOMAIN_NAME
        self.API_KEY = API_KEY
        self.headers = {'Authorization': f'Bearer {self.API_KEY}'}

    def GetGatewayConfiguration(self, GATEWAY_ID, FORMAT, FILENAME, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/gcs/gateways/configuration/{GATEWAY_ID}/{FORMAT}/{FILENAME}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    def GetGatewayConfiguration(self, GATEWAY_ID, FORMAT, TYPE, FILENAME, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/gcs/gateways/configuration/{GATEWAY_ID}/{FORMAT}/{TYPE}/{FILENAME}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

class ClientRegistryService():
    def __init__(self, DOMAIN_NAME = '', API_KEY = ''):
        self.DOMAIN_NAME = DOMAIN_NAME.rstrip(DOMAIN_NAME[-1]) if DOMAIN_NAME[-1] == '/' else DOMAIN_NAME
        self.API_KEY = API_KEY
        self.headers = {'Authorization': f'Bearer {self.API_KEY}'}

    def CreateClient(self, USER_ID, DATA = ''):
        try:
            response = requests.post(f'{self.DOMAIN_NAME}/api/v3/users/{USER_ID}/clients', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    def CreateClient(self, ORG_ID, DATA = ''):
        try:
            response = requests.post(f'{self.DOMAIN_NAME}/api/v3/organizations/{ORG_ID}/clients', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    def GetClient(self, CLIENT_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/clients/{CLIENT_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


    def ListClients(self, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/clients', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    def ListClients(self, USER_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/users/{USER_ID}/clients', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    def ListClients(self, ORG_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/organizations/{ORG_ID}/clients', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    def UpdateClient(self, CLIENT_ID, DATA = ''):
        try:
            response = requests.put(f'{self.DOMAIN_NAME}/api/v3/clients/{CLIENT_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    def DeleteClient(self, CLIENT_ID, DATA = ''):
        try:
            response = requests.delete(f'{self.DOMAIN_NAME}/api/v3/clients/{CLIENT_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    def RestoreClient(self, CLIENT_ID, DATA = ''):
        try:
            response = requests.post(f'{self.DOMAIN_NAME}/api/v3/clients/{CLIENT_ID}/restore', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    def PurgeClient(self, CLIENT_ID, DATA = ''):
        try:
            response = requests.delete(f'{self.DOMAIN_NAME}/api/v3/clients/{CLIENT_ID}/purge', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

class ClientAccessService():
    def __init__(self, DOMAIN_NAME = '', API_KEY = ''):
        self.DOMAIN_NAME = DOMAIN_NAME.rstrip(DOMAIN_NAME[-1]) if DOMAIN_NAME[-1] == '/' else DOMAIN_NAME
        self.API_KEY = API_KEY
        self.headers = {'Authorization': f'Bearer {self.API_KEY}'}

    def ListRights(self, CLIENT_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/clients/{CLIENT_ID}/rights', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    def GetClientCollaborator(self, CLIENT_ID, USER_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/clients/{CLIENT_ID}/collaborator/user/{USER_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


    def GetClientCollaborator(self, CLIENT_ID, ORG_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/clients/{CLIENT_ID}/organization/user/{ORG_ID}', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text


    def SetClientCollaborator(self, CLIENT_ID, DATA = ''):
        try:
            response = requests.put(f'{self.DOMAIN_NAME}/api/v3/clients/{CLIENT_ID}/collaborators', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text

    def ListClientCollaborators(self, CLIENT_ID, DATA = ''):
        try:
            response = requests.get(f'{self.DOMAIN_NAME}/api/v3/clients/{CLIENT_ID}/collaborators', headers=self.headers, json=DATA)

            response.raise_for_status()
        except HTTPError as http_err:
            return f'HTTP error occurred: {http_err}'
        except Exception as err:
            return f'Other error occurred: {err}'
        else:
            return response.text
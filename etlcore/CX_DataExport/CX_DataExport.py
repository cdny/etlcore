import os, time, bz2, re, json
from typing import Tuple, Type, Union

from etlcore.CX.CX import CX
from etlcore.CX_DataExport.CX_DataExport import CX_Utils
from bs4 import BeautifulSoup
from pandas import read_csv
from io import StringIO


class DataExport:
    """This class represents a single data export available to the user"""
    def __init__(self, record: dict) -> None:
        self.DataExportID = record.get("DataExportID", None)
        self.DynamicViewName = record.get("DynamicViewName", None)
        self.Name = record.get("Name", None)
        self.RunByName = record.get("RunByName", None)
        self.DynamicViewGUID = record.get("DynamicViewGUID", None)
        self.DynamicViewID = record.get("DynamicViewID", None)
        self.RunBy = record.get("RunBy", None)
        self.JobID = record.get("JobID", None)
        self.Downloadable = record.get("Downloadable", None)
        self.LastExportDate = record.get("LastExportDate", None)
        self.Slug = re.sub(r"[^0-9a-zA-Z]", "-", record.get("DynamicViewName", None))
        self.requestedRun = 0

    def __repr__(self):
        return "DataExportID: {} - DynamicViewName: {} - RunByName: {}".format(self.DataExportID, self.DynamicViewName, self.RunByName)

class CX_Utils:
    
    def __init__(self, URL: str, Username: str, Password: str, *args, **kwargs):
        self.url = URL
        self.username = Username
        self.password = Password
        self.cx_object = CX(URL, Username, Password)
        self.report_list = self.getReportList()

    def getReportList(self):
        response = self.cx_object.session.get("{}/Reporting/DataExport".format(self.url)) # navigate to the data exports page to grab cookies
        response = self.cx_object.session.post("{}/Reporting/DataExportList_Read".format(self.url), data={"pageSize": 200}) # get the list of files

        availableReports = json.loads(response.content.decode())["Data"] # parse the list of data exports

        data_export_list = [] #create empty list
        for report in availableReports:
            data_export_list.append(DataExport(report))
        #self.report_list = data_export_list

        self.cx_object.refreshSession() # Refresh our session
        return data_export_list

    #Return the first item found by ID and its index in the list or None
    def findReportByID(self, exportID: int, includeIndex: bool = False) -> Tuple[DataExport, Union[int, None]]:
        for index, item in enumerate(self.report_list):
            if item.DataExportID == exportID:
                if includeIndex:
                    return item, index
                else:
                    return item
        return None # If we haven't found anything

    def update(self, export: DataExport):
        ex, idx = self.findReportByID(export.DataExportID, True) #idx = 35?
        if ex is None: #identify what ex and idx are
            self.report_list.append(export)
        else:
            self.report_list[idx] = ex
    
    #runs export but does not download it, making it have fresh data to be pulled by downloadExport()
    def runExport(self, export: DataExport) -> None:
        if self.cx_object.logoutTime > time.time():
            if self.report_list is None:
                self.getReportList()

            r = self.cx_object.session.get("{}/Reporting/RunExport_Window?guid={}&_/undefined&_={}".format(self.url, export.DynamicViewGUID, time.time() * 1000))

            bs = BeautifulSoup(r.content, "lxml")
            rvt = bs.find(attrs={"name": "__RequestVerificationToken"}).attrs["value"]

            # Generate the report
            r = self.cx_object.session.post("{}/Reporting/RunExport_Window".format(self.url),
                                    data={"__RequestVerificationToken": rvt,
                                          "DynamicViewGUID": export.DynamicViewGUID,
                                          "DataExportID": ""}
                                )
            if r.ok:
                export.requestedRun = int(time.time())
                self.update(export)

        self.cx_object.refreshSession()

    def downloadExport(self, export: Type[DataExport], savePath: str = None) -> Union[str, bytes]:
        response = self.cx_object.session.get("{}/Reporting/DownloadExport?dataExportID={}&_".format(self.url, export.DataExportID))
        if response.ok:
            self.cx_object.refreshSession() # Refresh session

        if response.ok and savePath: # Save our file and return the path
            fileName = os.path.join(os.path.abspath(savePath), "{}-{}.csv.bz2".format(int(time.time()), export.Slug))
            with open(fileName, "wb") as f:
                content = bz2.compress(response.content)
                f.write(content)
            return fileName

        elif response.ok: # return the content for use in a dataframe
            return response.content

        else:
            raise Exception("An error occured and the file was unable to be downloaded - Status Code: {}".format(response.status_code))
        
    def get_report(cx_util: CX_Utils, Report: DataExport):
        return read_csv(StringIO(cx_util.downloadExport(Report).decode()), dtype=object)    

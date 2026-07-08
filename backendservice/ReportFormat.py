from copy import deepcopy
from dbmodels.ReconReport import ReconReport
from repository.ReconReportRepository import save_bulk_data
from repository.UploadRawDataRepository import update_cleared_status
from decimal import Decimal

class ReportFormat:
    OUTPUT_COL = {
        "Value_Date" : "",
        "DESCRIPTION" : "",
        "CATEGORY" : "",
        "RUNID" : "",
        "TYPE" : "",
        "CHECK #" : "",
        "EIN" : "",
        "AMOUNT" : ""
    }


    def cloneTemplate(self):
        return deepcopy(self.OUTPUT_COL)

    def getReportData(self, self, rpt):
        reportData = self.cloneTemplate()
        for key in self.OUTPUT_COL.keys():
            if (rpt.get(key) is not None):
                reportData[key] = (float(rpt[key]) if isinstance(rpt[key], Decimal) else rpt[key])
        return reportData

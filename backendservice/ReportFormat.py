class ReportFormat:

    def cloneTemplate(self):
        return deepcopy(self.OUTPUT_COL)

    def getReportData(self, self, rpt):
        reportData = self.cloneTemplate()
        for key in self.OUTPUT_COL.keys():
            if (rpt.get(key) is not None):
                reportData[key] = (float(rpt[key]) if isinstance(rpt[key], Decimal) else rpt[key])
        return reportData

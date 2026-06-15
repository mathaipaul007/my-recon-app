import pandas as pd
from openpyxl.styles import PatternFill
import io
from fastapi.responses import StreamingResponse
from repository.ReconReportRepository import get_recon_report_json
from connexion import request

def download_report(report_id : int):
    #usersession = request.context["token_info"]
    #clientid = usersession['clientid']
    clientid = "12345"
    cleared_results = get_recon_report_json(clientid,report_id,"cleared")
    outstanding_results = get_recon_report_json(clientid,report_id,"Outstanding")
    
    df_cleared = pd.DataFrame([r for r in cleared_results])
    df_outstanding = pd.DataFrame([r for r in outstanding_results])
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        
        df_outstanding.to_excel(writer, index=False, sheet_name="OUTSTANDING")
        df_cleared.to_excel(writer, index=False, sheet_name="CLEARED ITEMS")
        
        
        header_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
        
        for shee_name in ["OUTSTANDING", "CLEARED ITEMS"]:
            sheet = writer.sheets[shee_name]
            
            for cell in sheet[1]:
                cell.fill = header_fill
                
            for column_cells in sheet.columns:
                length = max(len(str(cell.value)) if cell.value else 0 for cell in column_cells)
                sheet.column_dimensions[column_cells[0].column_letter].width = length + 2
                
    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers = {"Content-Disposition" : "attachment; filename=report.xlsx"}
    )
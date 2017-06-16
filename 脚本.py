#coding=utf-8
from arcpy import env
import time
import sys
import pypyodbc
import xlwt
import xlrd
from xlutils.copy import copy
import os
reload(sys)
sys.setdefaultencoding('utf-8')

try:
    isCheckTB = True
    TBHArrar = []

    GSQTB = arcpy.GetParameterAsText(0)  # 古树群调查表
    MMTB = arcpy.GetParameterAsText(1)  # 每木调查表
    ZDSJK = arcpy.GetParameterAsText(2)  # 字典数据库
    OutFolder = arcpy.GetParameterAsText(3)  # 输出目录


    xzjx = ZDSJK+"\\xzjx\\gzxzjx"
    outFildGsq = ZDSJK+"\\dataset\\outFildGsq"
    outFildGsmm = ZDSJK+"\\dataset\\outFildGsmm"
    joinDataCopyGsq = ZDSJK+"\\dataset\\joinDataCopyGsq"
    joinDataCopyGsmm = ZDSJK+"\\dataset\\joinDataCopyGsmm"
    topo_dataset_path = ZDSJK+"\\dataset"


    def initData():
        updateDataBySql("update GSQBZJG set CWSL = 0,BZ=''")
        updateDataBySql("update MMBZJG set CWSL = 0,BZ=''")

    def initTbhArr():
        gsqtbhCursor = arcpy.SearchCursor(GSQTB,fields="TBH",sort_fields="TBH")
        for field in gsqtbhCursor:
            TBHArrar.append(field.getValue("TBH"))

    def is_valid_date(dateStr):
        try:
            if " " in dateStr:
                dateStr = dateStr.split(" ")[0]
            time.strptime(dateStr, "%Y-%m-%d")
            return True
        except:
            return False

    def validateJd(jdStr):
        reStr = '(((\d|[1-9]\d|1[1-7]\d)\.\d*)|180(\.0*)?)'
        return re.match(restr,jdStr)

    def validateWd(jdStr):
        restr = '(((\d|[1-8]\d)\.\d*)|90(\.0*)?)'
        return re.match(restr,jdStr)

    def deleteTemp():
        arcpy.Delete_management(topo_dataset_path + "\\my_topo_error_poly")
        arcpy.Delete_management(topo_dataset_path + "\\my_topo_error_line")
        arcpy.Delete_management(topo_dataset_path + "\\my_topo_error_point")
        arcpy.Delete_management(topo_dataset_path + "\\" + "checkZCD")
        arcpy.Delete_management(topo_dataset_path + "\\" + "landuse_singlepart")
        arcpy.Delete_management(joinDataCopyGsq)
        arcpy.Delete_management(joinDataCopyGsmm)
        arcpy.Delete_management(outFildGsq)
        arcpy.Delete_management(outFildGsmm)

    def checkTb(fieldList,table):
        gszd = getDataBySql('select ZDMC,ZDLX from '+table)
        gszdArr = []
        fieldObj = {}
        for field in fieldList:
            fieldObj[field.baseName] = field.type
        for field in gszd:
            gszdArr.append(field[0])
            if (field[0] not in fieldObj or field[1] != fieldObj[field[0]]):
                arcpy.AddError("缺少字段或字段类型不对：" + field[0])
                global isCheckTB
                isCheckTB = False
        for key in fieldObj:
            if key not in gszdArr and key not in ['SHAPE_Leng', 'OBJECTID', 'FID', 'SHAPE_Area', 'Shape']:
                arcpy.AddError("多余字段：" + str(key))
                global isCheckTB
                isCheckTB = False
        arcpy.AddMessage("表结构检查完成："+table)

    def getDataBySql(sql):
        constr = 'Driver={Microsoft Access Driver (*.mdb)};DBQ='+ZDSJK
        conn = pypyodbc.win_connect_mdb(constr)
        cur = conn.cursor()
        cur.execute(sql)
        result = cur.fetchall()
        conn.commit()
        cur.close()
        conn.close()
        return result

    def updateDataBySql(sql):
        constr = 'Driver={Microsoft Access Driver (*.mdb)};DBQ='+ZDSJK
        conn = pypyodbc.win_connect_mdb(constr)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()

    def SpatialJoin(joinData,table):
        if table == 'GSQTB':
            joinDataStr = joinDataCopyGsq
            outFildStr = outFildGsq
        else:
            joinDataStr = joinDataCopyGsmm
            outFildStr = outFildGsmm
        arcpy.AddMessage(joinData)
        arcpy.DeleteField_management(joinData, ["OBJECTID"])
        arcpy.CopyFeatures_management(joinData, joinDataStr)
        arcpy.DeleteField_management(joinDataStr, ["XIAN", "XIANG", "CUN"])
        arcpy.SpatialJoin_analysis(joinDataStr, xzjx, outFildStr, "#", "#", "#")
        arcpy.DeleteField_management(outFildStr, ["Join_Count", "TARGET_FID","SHAPE_Leng"])
        for field in arcpy.ListFields(outFildStr):
            if field.baseName[-2:] == "_1" and field.editable == True:
                arcpy.DeleteField_management(outFildStr, [field.baseName])
        arcpy.AddMessage("二调县乡村挂接完成")

    def CheckZCD():
        zcdArr = []
        checkData = outFildGsq
        arcpy.CreateTopology_management(topo_dataset_path, "checkZCD")
        arcpy.AddFeatureClassToTopology_management(
            topo_dataset_path + "\\" + "checkZCD", checkData, 1, 1)
        arcpy.AddRuleToTopology_management(
            topo_dataset_path + "\\" + "checkZCD", "Must Not Overlap (Area)", checkData, "", "", "")
        arcpy.ValidateTopology_management(
            topo_dataset_path + "\\" + "checkZCD")
        mytopo = topo_dataset_path + "\\" + "checkZCD"
        arcpy.ExportTopologyErrors_management(
            mytopo, topo_dataset_path, "my_topo_error")
        errorPolyCursor = arcpy.SearchCursor(topo_dataset_path + "\\my_topo_error_poly")
        for errorData in errorPolyCursor:
            if (errorData.getValue("OriginObjectID")-1) not in zcdArr:
                zcdArr.append(errorData.getValue("OriginObjectID")-1)
        if len(zcdArr)!=0:
            writeExcel(u"自重叠检查(古树群)",u"拓扑错误",u"未通过",len(zcdArr),u','.join(str(i) for i in zcdArr),1)
        zcdArrGsmm = []
        checkDatagsmm = outFildGsmm
        arcpy.CreateTopology_management(topo_dataset_path, "checkZCDgsmm")
        arcpy.AddFeatureClassToTopology_management(
            topo_dataset_path + "\\" + "checkZCDgsmm", checkDatagsmm, 1, 1)
        arcpy.AddRuleToTopology_management(
            topo_dataset_path + "\\" + "checkZCDgsmm", "Must Be Disjoint (Point)", checkDatagsmm, "", "", "")
        arcpy.ValidateTopology_management(
            topo_dataset_path + "\\" + "checkZCDgsmm")
        mytopoGsmm = topo_dataset_path + "\\" + "checkZCDgsmm"
        arcpy.ExportTopologyErrors_management(
            mytopoGsmm, topo_dataset_path, "gsmm_error")
        errorPolyCursorgsmm = arcpy.SearchCursor(topo_dataset_path + "\\gsmm_error_point")
        for errorDatagsmm in errorPolyCursorgsmm:
            if (errorDatagsmm.getValue("OriginObjectID")-1) not in zcdArrGsmm:
                zcdArrGsmm.append(errorDatagsmm.getValue("OriginObjectID")-1)
        if len(zcdArrGsmm)!=0:
            writeExcel(u"自重叠检查(古树名木)",u"拓扑错误",u"未通过",len(zcdArrGsmm),u','.join(str(i) for i in zcdArrGsmm),1)
        arcpy.AddMessage("自重叠检查完成")

    def CheckDBJ():
        dbjArr = []
        checkData = outFildGsq
        arcpy.AddField_management(checkData,"aa","TEXT",20,"","","","NULLABLE")
        arcpy.CalculateField_management(checkData, "aa","!OBJECTID!","PYTHON_9.3")
        arcpy.MultipartToSinglepart_management(checkData,
                                       topo_dataset_path+"\\landuse_singlepart")
        dbjData = getDataBySql("select OBJECTID from landuse_singlepart where aa in (select aa from landuse_singlepart group by aa having count(aa)>1)")
        for dbj in dbjData:
            dbjArr.append(dbj[0])
        if len(dbjArr)!=0:
            writeExcel(u"多部键检查(古树群)",u"拓扑错误",u"未通过",len(dbjArr),u','.join(str(i) for i in dbjArr),1)
        arcpy.DeleteField_management(checkData, ["aa"])
        arcpy.AddMessage("多部键检查完成")

    def CheckZXJ():
        zxjArr = []
        checkData = topo_dataset_path+"\\PolyToLine"
        arcpy.PolygonToLine_management(outFildGsq,checkData,False)
        arcpy.CreateTopology_management(topo_dataset_path, "checkZXJ")
        arcpy.AddFeatureClassToTopology_management(
            topo_dataset_path + "\\" + "checkZXJ", checkData, 1, 1)
        arcpy.AddRuleToTopology_management(
            topo_dataset_path + "\\" + "checkZXJ", "Must Not Self-Intersect (Line)", checkData, "", "", "")
        arcpy.ValidateTopology_management(
            topo_dataset_path + "\\" + "checkZXJ")
        mytopo = topo_dataset_path + "\\" + "checkZXJ"
        arcpy.ExportTopologyErrors_management(mytopo, topo_dataset_path, "zxj_error")
        errorPolyCursor = arcpy.SearchCursor(topo_dataset_path + "\\zxj_error_point")
        for errorData in errorPolyCursor:
            if (errorData.getValue("OriginObjectID")-1) not in zxjArr:
                zxjArr.append(errorData.getValue("OriginObjectID")-1)
        if len(zxjArr)!=0:
            writeExcel(u"自相交检查(古树群)",u"拓扑错误",u"未通过",len(zxjArr),u','.join(str(i) for i in zxjArr),1)
        arcpy.AddMessage("自相交检查完成")

    def checkGsqData(data):
        for row in data:
            #---------------图斑号检查----------------
            tbh = row.getValue("TBH")
            tbhisBT = getDataBySql("select ISBT from GSQBZJG where ZDMC = 'TBH'")[0][0]
            XBHrepeat = []
            if (tbh.strip() == ''):
                if tbhisBT == "是":
                    tbhBtError = getDataBySql(
                        "select CWSL from GSQBZJG where ZDMC = 'TBH'")
                    tbhBtError = tbhBtError[0][0]
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(tbhBtError) + 1) + " where ZDMC = 'TBH'")
            else:
                tbhBtBZstr = getDataBySql(
                    "select bz from GSQBZJG where ZDMC = 'TBH'")[0][0]
                tbhBtBZstr += str(row.getValue("OBJECTID")) + ","
                if row.getValue("XIAN") + tbh not in XBHrepeat:
                    XBHrepeat.append(row.getValue("XIAN") + tbh)
                else:
                    updateDataBySql("update GSQBZJG  set bz = " +
                                    tbhBtBZstr + "where ZDMC = 'TBH'")
                if len(tbh) > 4:
                    updateDataBySql("update GSQBZJG  set bz = " +
                                    tbhBtBZstr + "where ZDMC = 'TBH'")
                elif len(tbh) < 4:
                    row.setValue("TBH", "0" + tbh if len(tbh) ==
                                 3 else ("00" + tbh if len(tbh) == 2 else "000" + tbh))
            #---------------四肢界限----------------
            szjx = row.getValue("SZJX")
            szjxisBT = getDataBySql(
                "select ISBT from GSQBZJG where ZDMC = 'SZJX'")[0][0]
            if (szjx.strip() == ''):
                if szjxisBT == "是":
                    szjxBtError = getDataBySql(
                        "select CWSL from GSQBZJG where ZDMC = 'SZJX'")
                    szjxBtError = szjxBtError[0][0]
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(szjxBtError) + 1) + " where ZDMC = 'SZJX'")
            #--------------主要树种----------------
            zysz = row.getValue("ZYSZ")
            zyszisBT = getDataBySql(
                "select ISBT from GSQBZJG where ZDMC = 'ZYSZ'")[0][0]
            if (zysz.strip() == ''):
                if zyszisBT == "是":
                    szjxBtError = getDataBySql(
                        "select CWSL from GSQBZJG where ZDMC = 'ZYSZ'")
                    szjxBtError = szjxBtError[0][0]
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(szjxBtError) + 1) + " where ZDMC = 'ZYSZ'")
            else:
                zyszBtBZ = getDataBySql(
                    "select bz from GSQBZJG where ZDMC = 'ZYSZ'")[0][0]
                zyszBtBZ += str(row.getValue("OBJECTID")) + ","
                zyszArr = zysz.split("；")
                if len(zyszArr) > 3:
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    zyszBtBZ + "' where ZDMC = 'ZYSZ'")
                else:
                    for szdm in zyszArr:
                        bhdm = getDataBySql(
                            "select BHDM FROM SZB WHERE (BHDM = '" + szdm + "' OR SZMC = '" + szdm + "') and SZJB = 3")
                        if len(bhdm) == 0:
                            updateDataBySql("update GSQBZJG  set bz = '" +
                                            zyszBtBZ + "' where ZDMC = 'ZYSZ'")
            #--------------面积----------------
            mj = row.getValue("MJ")
            mjisBT = getDataBySql("select ISBT from GSQBZJG where ZDMC = 'MJ'")[0][0]
            if (str(mj).strip() == '' or mj == 0):
                if mjisBT == "是":
                    mjBtError = getDataBySql(
                        "select CWSL from GSQBZJG where ZDMC = 'MJ'")
                    mjBtError = mjBtError[0][0]
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(mjBtError) + 1) + " where ZDMC = 'MJ'")
            #--------------古树株树----------------
            gszs = row.getValue("GSZS")
            gszsisBT = getDataBySql(
                "select ISBT from GSQBZJG where ZDMC = 'GSZS'")[0][0]
            if (str(gszs).strip() == '' or gszs == 0):
                if gszsisBT == "是":
                    gszsBtError = getDataBySql(
                        "select CWSL from GSQBZJG where ZDMC = 'GSZS'")
                    gszsBtError = gszsBtError[0][0]
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(gszsBtError) + 1) + " where ZDMC = 'GSZS'")
            else:
                gszsBtBZ = getDataBySql(
                    "select bz from GSQBZJG where ZDMC = 'GSZS'")[0][0]
                gszsBtBZ += str(row.getValue("OBJECTID")) + ","
                if gszs >= 10:
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    gszsBtBZ + "' where ZDMC = 'GSZS'")
            #--------------林分平均高----------------
            lfpjg = row.getValue("LFPJG")
            lfpjgisBT = getDataBySql(
                "select ISBT from GSQBZJG where ZDMC = 'LFPJG'")[0][0]
            if (str(lfpjg).strip() == '' or lfpjg == 0):
                if lfpjgisBT == "是":
                    lfpjgBtError = getDataBySql(
                        "select CWSL from GSQBZJG where ZDMC = 'LFPJG'")
                    lfpjgBtError = lfpjgBtError[0][0]
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(lfpjgBtError) + 1) + " where ZDMC = 'LFPJG'")
            else:
                lfpjgBtBZ = getDataBySql(
                    "select bz from GSQBZJG where ZDMC = 'LFPJG'")[0][0]
                lfpjgBtBZ += str(row.getValue("OBJECTID")) + ","
                row.setValue("LFPJG", round(lfpjg, 2))
                if lfpjg >= 50:
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    lfpjgBtBZ + "' where ZDMC = 'LFPJG'")
            #---------------林分平均胸径----------------
            lfpjxj = row.getValue("LFPJXJ")
            lfpjxjisBT = getDataBySql(
                "select ISBT from GSQBZJG where ZDMC = 'LFPJXJ'")[0][0]
            if (str(lfpjxj).strip() == '' or lfpjxj == 0):
                if lfpjxjisBT == "是":
                    lfpjxjBtError = getDataBySql(
                        "select CWSL from GSQBZJG where ZDMC = 'LFPJXJ'")
                    lfpjxjBtError = lfpjxjBtError[0][0]
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(lfpjxjBtError) + 1) + " where ZDMC = 'LFPJXJ'")
            else:
                lfpjxjBtBZ = getDataBySql(
                    "select bz from GSQBZJG where ZDMC = 'LFPJXJ'")[0][0]
                lfpjxjBtBZ += str(row.getValue("OBJECTID")) + ","
                row.setValue("LFPJXJ", round(lfpjxj, 2))
                if lfpjxj >= 500:
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    lfpjxjBtBZ + "' where ZDMC = 'LFPJXJ'")
            #---------------平均树龄----------------
            pjsl = row.getValue("PJSL")
            pjslisBT = getDataBySql(
                "select ISBT from GSQBZJG where ZDMC = 'PJSL'")[0][0]
            if (str(pjsl).strip() == '' or pjsl == 0):
                if pjslisBT == "是":
                    pjslBtError = getDataBySql(
                        "select CWSL from GSQBZJG where ZDMC = 'PJSL'")
                    pjslBtError = pjslBtError[0][0]
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(pjslBtError) + 1) + " where ZDMC = 'PJSL'")
            else:
                pjslBtBZ = getDataBySql(
                    "select bz from GSQBZJG where ZDMC = 'PJSL'")[0][0]
                pjslBtBZ += str(row.getValue("OBJECTID")) + ","
                if pjsl >= 100:
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    pjslBtBZ + "' where ZDMC = 'PJSL'")
            #------------郁闭度---------------
            ybd = row.getValue("YBD")
            ybdisBT = getDataBySql("select ISBT from GSQBZJG where ZDMC = 'YBD'")[0][0]
            if (str(ybd).strip() == '' or ybd == 0):
                if ybdisBT == "是":
                    ybdBtError = getDataBySql(
                        "select CWSL from GSQBZJG where ZDMC = 'YBD'")
                    ybdBtError = ybdBtError[0][0]
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(ybdBtError) + 1) + " where ZDMC = 'YBD'")
            else:
                row.setValue("YBD", round(ybd, 2))
            #------------海拔---------------
            hb = row.getValue("HB")
            hbisBT = getDataBySql("select ISBT from GSQBZJG where ZDMC = 'HB'")[0][0]
            if (str(hb).strip() == '' or hb == 0):
                if hbisBT == "是":
                    hbBtError = getDataBySql(
                        "select CWSL from GSQBZJG where ZDMC = 'HB'")
                    hbBtError = hbBtError[0][0]
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(hbBtError) + 1) + " where ZDMC = 'HB'")
            else:
                hbBtBZ = getDataBySql("select bz from GSQBZJG where ZDMC = 'HB'")[0][0]
                hbBtBZ += str(row.getValue("OBJECTID")) + ","
                if hb > 2900 or hb < 147:
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    hbBtBZ + "' where ZDMC = 'HB'")
            #------------坡度---------------
            pd = row.getValue("PD")
            pdisBT = getDataBySql("select ISBT from GSQBZJG where ZDMC = 'PD'")[0][0]
            if (str(pd).strip() == '' or pd == 0):
                if pdisBT == "是":
                    pdBtError = getDataBySql(
                        "select CWSL from GSQBZJG where ZDMC = 'PD'")
                    pdBtError = pdBtError[0][0]
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(pdBtError) + 1) + " where ZDMC = 'PD'")
            else:
                pdBtBZ = getDataBySql("select bz from GSQBZJG where ZDMC = 'PD'")[0][0]
                pdBtBZ += str(row.getValue("OBJECTID")) + ","
                if pd > 6 or pd < 1:
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    pdBtBZ + "' where ZDMC = 'PD'")
            #------------坡向---------------
            px = row.getValue("PX")
            pxisBT = getDataBySql("select ISBT from GSQBZJG where ZDMC = 'PX'")[0][0]
            if (str(px).strip() == '' or px == 0):
                if pxisBT == "是":
                    pxBtError = getDataBySql(
                        "select CWSL from GSQBZJG where ZDMC = 'PX'")
                    pxBtError = pxBtError[0][0]
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(pxBtError) + 1) + " where ZDMC = 'PX'")
            else:
                pxBtBZ = getDataBySql("select bz from GSQBZJG where ZDMC = 'PX'")[0][0]
                pxBtBZ += str(row.getValue("OBJECTID")) + ","
                if px > 7 or px < 1:
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    pxBtBZ + "' where ZDMC = 'PX'")
            #------------土壤类型---------------
            trlx = row.getValue("TRLX")
            trlxisBT = getDataBySql(
                "select ISBT from GSQBZJG where ZDMC = 'TRLX'")[0][0]
            if (str(trlx).strip() == '' or trlx == 0):
                if trlxisBT == "是":
                    trlxBtError = getDataBySql(
                        "select CWSL from GSQBZJG where ZDMC = 'TRLX'")
                    trlxBtError = trlxBtError[0][0]
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(trlxBtError) + 1) + " where ZDMC = 'TRLX'")
            else:
                trlxBtBZ = getDataBySql(
                    "select bz from GSQBZJG where ZDMC = 'TRLX'")[0][0]
                trlxBtBZ += str(row.getValue("OBJECTID")) + ","
                trlxdm = getDataBySql(
                    "select TRLX FROM TRLX WHERE TRLX = '" + str(trlx) + "'")
                if len(trlxdm) == 0:
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    trlxBtBZ + "' where ZDMC = 'TRLX'")
            #------------图层厚度---------------
            tchd = row.getValue("TCHD")
            tchdisBT = getDataBySql(
                "select ISBT from GSQBZJG where ZDMC = 'TCHD'")[0][0]
            if (str(tchd).strip() == '' or tchd == 0):
                if tchdisBT == "是":
                    tchdBtError = getDataBySql(
                        "select CWSL from GSQBZJG where ZDMC = 'TCHD'")
                    tchdBtError = tchdBtError[0][0]
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(tchdBtError) + 1) + " where ZDMC = 'TCHD'")
            else:
                tchdBtBZ = getDataBySql(
                    "select bz from GSQBZJG where ZDMC = 'TCHD'")[0][0]
                tchdBtBZ += str(row.getValue("OBJECTID")) + ","
                if tchd > 200:
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    tchdBtBZ + "' where ZDMC = 'TCHD'")
            #------------下木种类---------------
            xmzl = row.getValue("XMZL")
            xmzlBtBZ = getDataBySql("select bz from GSQBZJG where ZDMC = 'XMZL'")[0][0]
            xmzlBtBZ += str(row.getValue("OBJECTID")) + ","
            if (xmzl.strip() == '' and row.getValue("XMMD") > 0):
                updateDataBySql("update GSQBZJG  set bz = '" +
                                xmzlBtBZ + "' where ZDMC = 'XMZL'")
            else:
                xmzlArr = xmzl.split("；")
                if len(xmzlArr) > 3:
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    xmzlBtBZ + "' where ZDMC = 'XMZL'")
            #------------下木密度---------------
            xmmd = row.getValue("XMMD")
            xmmdBtBZ = getDataBySql("select bz from GSQBZJG where ZDMC = 'XMMD'")[0][0]
            xmmdBtBZ += str(row.getValue("OBJECTID")) + ","
            if xmmd > 1000:
                updateDataBySql("update GSQBZJG  set bz = '" +
                                xmmdBtBZ + "' where ZDMC = 'XMMD'")
            #------------地被物种类---------------
            dbwzl = row.getValue("DBWZL")
            dbwzlBtBZ = getDataBySql(
                "select bz from GSQBZJG where ZDMC = 'DBWZL'")[0][0]
            dbwzlBtBZ += str(row.getValue("OBJECTID")) + ","
            if (dbwzl.strip() == '' and row.getValue("DBWMD") > 0):
                updateDataBySql("update GSQBZJG  set bz = '" +
                                dbwzlBtBZ + "' where ZDMC = 'DBWZL'")
            #------------地被物密度---------------
            dbwmd = row.getValue("DBWMD")
            dbwmdBtBZ = getDataBySql(
                "select bz from GSQBZJG where ZDMC = 'DBWMD'")[0][0]
            dbwmdBtBZ += str(row.getValue("OBJECTID")) + ","
            if dbwmd > 100:
                updateDataBySql("update GSQBZJG  set bz = '" +
                                dbwmdBtBZ + "' where ZDMC = 'DBWMD'")
            #------------目的保护树种---------------
            mdbhsz = row.getValue("MDBHSZ")
            mdbhszisBT = getDataBySql(
                "select ISBT from GSQBZJG where ZDMC = 'MDBHSZ'")[0][0]
            if (mdbhsz.strip() == ''):
                if mdbhszisBT == "是":
                    mdbhszBtError = getDataBySql(
                        "select CWSL from GSQBZJG where ZDMC = 'MDBHSZ'")
                    mdbhszBtError = mdbhszBtError[0][0]
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(mdbhszBtError) + 1) + " where ZDMC = 'MDBHSZ'")
            else:
                mdbhszBtBZ = getDataBySql(
                    "select bz from GSQBZJG where ZDMC = 'MDBHSZ'")[0][0]
                mdbhszBtBZ += str(row.getValue("OBJECTID")) + ","
                mdbhszdm = getDataBySql("select BHDM FROM SZB WHERE (BHDM = '" +
                                        mdbhsz + "' or SZMC = '" + mdbhsz + "')  and SZJB = 3")
                if len(mdbhszdm) == 0:
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    mdbhszBtBZ + "' where ZDMC = 'MDBHSZ'")
            #------------目的保护树种科---------------
            mdszk = row.getValue("MDSZK")
            mdszkisBT = getDataBySql(
                "select ISBT from GSQBZJG where ZDMC = 'MDSZK'")[0][0]
            if (mdszk.strip() == ''):
                if mdszkisBT == "是":
                    mdszkBtError = getDataBySql(
                        "select CWSL from GSQBZJG where ZDMC = 'MDSZK'")
                    mdszkBtError = mdszkBtError[0][0]
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(mdszkBtError) + 1) + " where ZDMC = 'MDSZK'")
            else:
                mdbSZKBtBZ = getDataBySql(
                    "select bz from GSQBZJG where ZDMC = 'MDSZK'")[0][0]
                mdbSZKBtBZ += str(row.getValue("OBJECTID")) + ","
                if mdbhsz.strip() == '':
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    mdbSZKBtBZ + "' where ZDMC = 'MDSZK'")
                else:
                    mdbhszK = getDataBySql("select K FROM SZB WHERE (BHDM = '" +
                                           mdbhsz + "' or SZMC = '" + mdbhsz + "')  and SZJB = 3")
                    if len(mdbhszK) == 0:
                        updateDataBySql("update GSQBZJG  set bz = '" +
                                        mdbSZKBtBZ + "' where ZDMC = 'MDSZK'")
                    elif len(mdbhszK) > 0 and mdbhszK[0][0] != mdszk:
                        updateDataBySql("update GSQBZJG  set bz = '" +
                                        mdbSZKBtBZ + "' where ZDMC = 'MDSZK'")
            #------------目的保护树种属---------------
            MDSZS = row.getValue("MDSZS")
            MDSZSisBT = getDataBySql(
                "select ISBT from GSQBZJG where ZDMC = 'MDSZS'")[0][0]
            if (MDSZS.strip() == ''):
                if MDSZSisBT == "是":
                    MDSZSBtError = getDataBySql(
                        "select CWSL from GSQBZJG where ZDMC = 'MDSZS'")
                    MDSZSBtError = MDSZSBtError[0][0]
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(MDSZSBtError) + 1) + " where ZDMC = 'MDSZS'")
            else:
                MDSZSBtBZ = getDataBySql(
                    "select bz from GSQBZJG where ZDMC = 'MDSZS'")[0][0]
                MDSZSBtBZ += str(row.getValue("OBJECTID")) + ","
                if mdbhsz.strip() == '':
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    MDSZSBtBZ + "' where ZDMC = 'MDSZS'")
                else:
                    mdbhszK = getDataBySql("select S FROM SZB WHERE (BHDM = '" +
                                           mdbhsz + "' or SZMC = '" + mdbhsz + "')  and SZJB = 3")
                    if len(mdbhszK) == 0:
                        updateDataBySql("update GSQBZJG  set bz = '" +
                                        MDSZSBtBZ + "' where ZDMC = 'MDSZS'")
                    elif len(mdbhszK) > 0 and mdbhszK[0][0] != MDSZS:
                        updateDataBySql("update GSQBZJG  set bz = '" +
                                        MDSZSBtBZ + "' where ZDMC = 'MDSZS'")
            #------------照片名---------------
            zpm = row.getValue("ZPM")
            zpmisBT = getDataBySql("select ISBT from GSQBZJG where ZDMC = 'ZPM'")[0][0]
            if (str(zpm).strip() == ''):
                if zpmisBT == "是":
                    zpmBtError = getDataBySql(
                        "select CWSL from GSQBZJG where ZDMC = 'ZPM'")
                    zpmBtError = zpmBtError[0][0]
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(zpmBtError) + 1) + " where ZDMC = 'ZPM'")
            else:
                zpmBtBZ = getDataBySql(
                    "select bz from GSQBZJG where ZDMC = 'ZPM'")[0][0]
                zpmBtBZ += str(row.getValue("OBJECTID")) + ","
                if zpm[0:10] != row.getValue("GSQBH") :
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    zpmBtBZ + "' where ZDMC = 'ZPM'")
            #------------调查人---------------
            dcr = row.getValue("DCR")
            dcrisBT = getDataBySql(
                "select ISBT from GSQBZJG where ZDMC = 'DCR'")[0][0]
            if (str(dcr).strip() == ''):
                if dcrisBT == "是":
                    dcrBtError = getDataBySql(
                        "select CWSL from GSQBZJG where ZDMC = 'DCR'")
                    dcrBtError = dcrBtError[0][0]
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(dcrBtError) + 1) + " where ZDMC = 'DCR'")
            #------------调查日期---------------
            dcrq = row.getValue("DCRQ")
            dcrqisBT = getDataBySql(
                "select ISBT from GSQBZJG where ZDMC = 'DCRQ'")[0][0]
            dcrqBtError = getDataBySql(
                "select CWSL from GSQBZJG where ZDMC = 'DCRQ'")
            dcrqBtError = dcrqBtError[0][0]
            if (str(dcrq).strip() == '' or dcrq.year == 1899):
                if dcrqisBT == "是":
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(dcrqBtError) + 1) + " where ZDMC = 'DCRQ'")
            else:
                dcrqBtBZ = getDataBySql(
                    "select bz from GSQBZJG where ZDMC = 'DCRQ'")[0][0]
                dcrqBtBZ += str(row.getValue("OBJECTID")) + ","
                if not is_valid_date(str(dcrq)):
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    dcrqBtBZ + "' where ZDMC = 'DCRQ'")
                elif dcrq.year > 2017:
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    dcrqBtBZ + "' where ZDMC = 'DCRQ'")
            #------------审核人---------------
            shr = row.getValue("SHR")
            shrisBT = getDataBySql(
                "select ISBT from GSQBZJG where ZDMC = 'SHR'")[0][0]
            if (str(shr).strip() == ''):
                if shrisBT == "是":
                    shrBtError = getDataBySql(
                        "select CWSL from GSQBZJG where ZDMC = 'SHR'")
                    shrBtError = shrBtError[0][0]
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(shrBtError) + 1) + " where ZDMC = 'SHR'")
            #------------审核日期---------------
            shrq = row.getValue("SHRQ")
            shrqisBT = getDataBySql(
                "select ISBT from GSQBZJG where ZDMC = 'SHRQ'")[0][0]
            shrqBtError = getDataBySql(
                "select CWSL from GSQBZJG where ZDMC = 'SHRQ'")
            shrqBtError = shrqBtError[0][0]
            if (str(shrq).strip() == '' or shrq.year == 1899):
                if shrqisBT == "是":
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(shrqBtError) + 1) + " where ZDMC = 'SHRQ'")
            else:
                shrqBtBZ = getDataBySql(
                    "select bz from GSQBZJG where ZDMC = 'SHRQ'")[0][0]
                shrqBtBZ += str(row.getValue("OBJECTID")) + ","
                if not is_valid_date(str(shrq)):
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    shrqBtBZ + "' where ZDMC = 'SHRQ'")
                elif shrq.year > 2017:
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    shrqBtBZ + "' where ZDMC = 'SHRQ'")
            #------------古树群编号---------------
            NewGsqBh = str(row.getValue("XIAN"))+str(tbh)
            gsqbh = row.getValue("GSQBH")
            gsqbhisBT = getDataBySql(
                "select ISBT from GSQBZJG where ZDMC = 'GSQBH'")[0][0]
            if (gsqbh.strip() == ''):
                if gsqbhisBT == "是":
                    gsqbhBtError = getDataBySql(
                        "select CWSL from GSQBZJG where ZDMC = 'GSQBH'")
                    gsqbhBtError = gsqbhBtError[0][0]
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(gsqbhBtError) + 1) + " where ZDMC = 'GSQBH'")
            else:
                if NewGsqBh != gsqbh:
                    gsqbhBtBZ = getDataBySql(
                        "select bz from GSQBZJG where ZDMC = 'GSQBH'")[0][0]
                    gsqbhBtBZ += str(row.getValue("OBJECTID")) + ","
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    gsqbhBtBZ + "' where ZDMC = 'GSQBH'")
            data.updateRow(row)
        arcpy.AddMessage("古树群逻辑检查完成")

    def checkMmData(data):
        for row in data:
            #-------------------调查顺序号--------------------
            dcbh = row.getValue("DCBH")
            dcbhisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'DCBH'")[0][0]
            DCBHrepeat = []
            if (dcbh.strip() == ''):
                if dcbhisBT == "是":
                    dcbhBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'DCBH'")
                    dcbhBtError = dcbhBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(dcbhBtError) + 1) + " where ZDMC = 'DCBH'")
            else:
                dcbhBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'DCBH'")[0][0]
                dcbhBtBZ += str(row.getValue("OBJECTID")) + ","
                if row.getValue("XIAN") + dcbh not in DCBHrepeat:
                    DCBHrepeat.append(row.getValue("XIAN") + dcbh)
                else:
                    updateDataBySql("update MMBZJG  set bz = " +
                                    dcbhBtBZ + "where ZDMC = 'DCBH'")
                if len(dcbh) > 5:
                    updateDataBySql("update MMBZJG  set bz = " +
                                    dcbhBtBZ + "where ZDMC = 'DCBH'")
                elif len(dcbh) < 5:
                    row.setValue("DCBH", "0" + dcbh if len(dcbh) ==
                                 4 else ("00" + dcbh if len(dcbh) == 3 else ("000" + dcbh if len(dcbh) == 2 else "0000" + dcbh)))
            #------------古树大树名木编号---------------
            NewGsmmBh = str(row.getValue("XIAN"))+str(row.getValue("DCBH"))
            gsmmbh = row.getValue("GDMMBH")
            gsmmbhisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'GDMMBH'")[0][0]
            if (gsmmbh.strip() == ''):
                if gsmmbhisBT == "是":
                    gsmmbhBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'GDMMBH'")
                    gsmmbhBtError = gsmmbhBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(gsmmbhBtError) + 1) + " where ZDMC = 'GDMMBH'")
            else:
                if NewGsmmBh != gsmmbh:
                    gsmmbhBtBZ = getDataBySql(
                        "select bz from MMBZJG where ZDMC = 'GDMMBH'")[0][0]
                    gsmmbhBtBZ += str(row.getValue("OBJECTID")) + ","
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    gsmmbhBtBZ + "' where ZDMC = 'GDMMBH'")
            #--------------中文名----------------
            zwm = row.getValue("ZWM")
            zwmisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'ZWM'")[0][0]
            if (zwm.strip() == ''):
                if zwmisBT == "是":
                    zwmBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'ZWM'")
                    zwmBtError = zwmBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(zwmBtError) + 1) + " where ZDMC = 'ZWM'")
            else:
                zyszBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'ZWM'")[0][0]
                zyszBtBZ += str(row.getValue("OBJECTID")) + ","
                zwmjg = getDataBySql(
                    "select ZWM FROM ZGZWZ WHERE ZWM = '" + zwm + "'")
                if len(zwmjg) == 0:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    zyszBtBZ + "' where ZDMC = 'ZWM'")
            #------------拉丁名---------------
            ldm = row.getValue("LDM")
            ldmisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'LDM'")[0][0]
            if (ldm.strip() == ''):
                if ldmisBT == "是":
                    ldmBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'LDM'")
                    ldmBtError = ldmBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(ldmBtError) + 1) + " where ZDMC = 'LDM'")
            else:
                ldmBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'LDM'")[0][0]
                ldmBtBZ += str(row.getValue("OBJECTID")) + ","
                if zwm.strip() == '':
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    ldmBtBZ + "' where ZDMC = 'LDM'")
                else:
                    zwmszLDM = getDataBySql(
                        "select LDM FROM ZGZWZ WHERE ZWM = '" + zwm + "'")
                    if len(zwmszLDM) == 0:
                        updateDataBySql("update MMBZJG  set bz = '" +
                                        ldmBtBZ + "' where ZDMC = 'LDM'")
                    elif len(zwmszLDM) > 0 and zwmszLDM[0][0] != ldm:
                        updateDataBySql("update MMBZJG  set bz = '" +
                                        ldmBtBZ + "' where ZDMC = 'LDM'")
            #------------科---------------
            ke = row.getValue("KE")
            keisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'KE'")[0][0]
            if (ke.strip() == ''):
                if keisBT == "是":
                    keBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'KE'")
                    keBtError = keBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(keBtError) + 1) + " where ZDMC = 'KE'")
            else:
                keBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'KE'")[0][0]
                keBtBZ += str(row.getValue("OBJECTID")) + ","
                if zwm.strip() == '':
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    keBtBZ + "' where ZDMC = 'KE'")
                else:
                    zwmszK = getDataBySql(
                        "select KM FROM ZGZWZ WHERE ZWM = '" + zwm + "'")
                    if len(zwmszK) == 0:
                        updateDataBySql("update MMBZJG  set bz = '" +
                                        keBtBZ + "' where ZDMC = 'KE'")
                    elif len(zwmszK) > 0 and zwmszK[0][0] != ke:
                        updateDataBySql("update MMBZJG  set bz = '" +
                                        keBtBZ + "' where ZDMC = 'KE'")
            #------------属---------------
            shu = row.getValue("SHU")
            shuisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'SHU'")[0][0]
            if (shu.strip() == ''):
                if shuisBT == "是":
                    shuBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'SHU'")
                    shuBtError = shuBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(shuBtError) + 1) + " where ZDMC = 'SHU'")
            else:
                shuBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'SHU'")[0][0]
                shuBtBZ += str(row.getValue("OBJECTID")) + ","
                if shu.strip() == '':
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    shuBtBZ + "' where ZDMC = 'SHU'")
                else:
                    zwmszSHU = getDataBySql(
                        "select SM FROM ZGZWZ WHERE ZWM = '" + zwm + "'")
                    if len(zwmszSHU) == 0:
                        updateDataBySql("update MMBZJG  set bz = '" +
                                        shuBtBZ + "' where ZDMC = 'SHU'")
                    elif len(zwmszSHU) > 0 and zwmszSHU[0][0] != shu:
                        updateDataBySql("update MMBZJG  set bz = '" +
                                        shuBtBZ + "' where ZDMC = 'SHU'")
            #------------生长场所---------------
            szcs = row.getValue("SZCS")
            szcsisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'SZCS'")[0][0]
            if (str(szcs).strip() == '' or szcs == 0):
                if szcsisBT == "是":
                    szcsBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'SZCS'")
                    szcsBtError = szcsBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(szcsBtError) + 1) + " where ZDMC = 'SZCS'")
            else:
                szcsBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'SZCS'")[0][0]
                szcsBtBZ += str(row.getValue("OBJECTID")) + ","
                if szcs not in [1, 2]:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    szcsBtBZ + "' where ZDMC = 'SZCS'")
            #------------分布特点---------------
            fbtd = row.getValue("FBTD")
            fbtdisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'FBTD'")[0][0]
            if (str(fbtd).strip() == '' or fbtd == 0):
                if fbtdisBT == "是":
                    fbtdBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'FBTD'")
                    fbtdBtError = fbtdBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(fbtdBtError) + 1) + " where ZDMC = 'FBTD'")
            else:
                fbtdBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'FBTD'")[0][0]
                fbtdBtBZ += str(row.getValue("OBJECTID")) + ","
                if fbtd not in [1, 2]:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    fbtdBtBZ + "' where ZDMC = 'FBTD'")
            #------------经度---------------
            jd = row.getValue("JD")
            jdisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'JD'")[0][0]
            if (str(jd).strip() == '' or jd == 0):
                if jdisBT == "是":
                    jdBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'JD'")
                    jdBtError = jdBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(jdBtError) + 1) + " where ZDMC = 'JD'")
            else:
                jdBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'JD'")[0][0]
                jdBtBZ += str(row.getValue("OBJECTID")) + ","
                if validateJd(jd) =="None":
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    jdBtBZ + "' where ZDMC = 'JD'")
            #------------纬度---------------
            WD = row.getValue("WD")
            WDisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'WD'")[0][0]
            if (str(WD).strip() == '' or WD == 0):
                if WDisBT == "是":
                    WDBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'WD'")
                    WDBtError = WDBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(WDBtError) + 1) + " where ZDMC = 'WD'")
            else:
                WDBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'WD'")[0][0]
                WDBtBZ += str(row.getValue("OBJECTID")) + ","
                if validateWd(WD) =="None":
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    WDBtBZ + "' where ZDMC = 'WD'")
            #------------权属---------------
            qs = row.getValue("QS")
            qsisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'QS'")[0][0]
            if (str(qs).strip() == '' or qs == 0):
                if qsisBT == "是":
                    qsBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'QS'")
                    qsBtError = qsBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(qsBtError) + 1) + " where ZDMC = 'QS'")
            else:
                qsBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'QS'")[0][0]
                qsBtBZ += str(row.getValue("OBJECTID")) + ","
                if qs not in [1, 2, 3, 4]:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    qsBtBZ + "' where ZDMC = 'QS'")
            #------------标识代码---------------
            bsdm = row.getValue("BSDM")
            bsdmisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'BSDM'")[0][0]
            if (str(bsdm).strip() == ''):
                if bsdmisBT == "是":
                    bsdmBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'BSDM'")
                    bsdmBtError = bsdmBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(bsdmBtError) + 1) + " where ZDMC = 'BSDM'")
            else:
                bsdmBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'BSDM'")[0][0]
                bsdmBtBZ += str(row.getValue("OBJECTID")) + ","
                if bsdm not in ['1', '2', '3', '4', '5', '6', '7'] or (bsdm in ['2','3','6','7'] and (row.getValue("GSLS1").strip() == "" and row.getValue("GSLS2").strip() == ""  and row.getValue("GSLS3").strip() == "" )):
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    bsdmBtBZ + "' where ZDMC = 'BSDM'")
            #------------种类代码---------------
            zldm = row.getValue("ZLDM")
            zldmszisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'ZLDM'")[0][0]
            if (zldm.strip() == ''):
                if zldmszisBT == "是":
                    zldmBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'ZLDM'")
                    zldmBtError = zldmBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(zldmBtError) + 1) + " where ZDMC = 'ZLDM'")
            else:
                zldmBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'ZLDM'")[0][0]
                zldmBtBZ += str(row.getValue("OBJECTID")) + ","
                zldmMC = getDataBySql("select SZMC FROM SZB WHERE BHDM = '" +
                                      zldm + "' AND SZJB = 3")
                if len(zldmMC) == 0 or zldmMC[0][0] != row.getValue("ZWM"):
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    zldmBtBZ + "' where ZDMC = 'ZLDM'")
            #------------特征代码---------------
            TZDM = row.getValue("TZDM")
            TZDMisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'TZDM'")[0][0]
            if (TZDM.strip() == ''):
                if TZDMisBT == "是":
                    TZDMBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'TZDM'")
                    TZDMBtError = TZDMBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(TZDMBtError) + 1) + " where ZDMC = 'TZDM'")
            #------------真实树龄---------------
            zssl = row.getValue("ZSSL")
            zsslisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'ZSSL'")[0][0]
            if (str(zssl).strip() == '' or zssl == 0):
                if zsslisBT == "是":
                    zsslBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'ZSSL'")
                    zsslBtError = zsslBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(zsslBtError) + 1) + " where ZDMC = 'ZSSL'")
            else:
                zsslBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'ZSSL'")[0][0]
                zsslBtBZ += str(row.getValue("OBJECTID")) + ","
                if (row.getValue("BSDM") in ['2','4'] and zssl!=0) or zssl > 1500 or (row.getValue("BSDM") in ['1', '3', '5', '7'] and zssl < 100) or (row.getValue("GSDJ")== 1 and zssl < 500) or (row.getValue("GSDJ")==2 and (zssl < 300 or zssl >= 500)) or (row.getValue("GSDJ")==3 and (zssl < 100 or zssl >= 300)):
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    zsslBtBZ + "' where ZDMC = 'ZSSL'")
            #------------估测树龄---------------
            gcsl=row.getValue("GCSL")
            gcslisBT=getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'GCSL'")[0][0]
            if (str(gcsl).strip() == '' or gcsl == 0):
                if gcslisBT == "是":
                    gcslBtError=getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'GCSL'")
                    gcslBtError=gcslBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(gcslBtError) + 1) + " where ZDMC = 'GCSL'")
            else:
                gcslBtBZ=getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'GCSL'")[0][0]
                gcslBtBZ += str(row.getValue("OBJECTID")) + ","
                if (row.getValue("ZSSL") != 0):
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    gcslBtBZ + "' where ZDMC = 'GCSL'")
                elif ((row.getValue("BSDM") in ['2','4'] and gcsl!=0) or (row.getValue("BSDM") in ['1', '3', '5', '7'] and gcsl < 100) or gcsl > 1500 or (row.getValue("GSDJ")==1 and gcsl < 500) or (row.getValue("GSDJ")==2 and (gcsl < 300 or gcsl >= 500)) or (row.getValue("GSDJ")==3 and (gcsl < 100 or gcsl >= 300))):
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    gcslBtBZ + "' where ZDMC = 'GCSL'")
            #---------------------胸径------------------
            xj=row.getValue("XJ")
            xjisBT=getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'XJ'")[0][0]
            if (str(xj).strip() == '' or xj == 0):
                if xjisBT == "是":
                    xjBtError=getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'XJ'")
                    xjBtError=xjBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(xjBtError) + 1) + " where ZDMC = 'XJ'")
            else:
                xjBtBZ=getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'XJ'")[0][0]
                xjBtBZ += str(row.getValue("OBJECTID")) + ","
                if (xj < 314 and row.getValue("BSDM") == '4') or xj > 1570:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    xjBtBZ + "' where ZDMC = 'XJ'")
            #------------古树等级---------------
            gsdj=row.getValue("GSDJ")
            gsdjisBT=getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'GSDJ'")[0][0]
            if (str(gsdj).strip() == ''):
                if gsdjisBT == "是":
                    gsdjBtError=getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'GSDJ'")
                    gsdjBtError=gsdjBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(gsdjBtError) + 1) + " where ZDMC = 'GSDJ'")
            else:
                gsdjBtBZ=getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'GSDJ'")[0][0]
                gsdjBtBZ += str(row.getValue("OBJECTID")) + ","
                if gsdj not in [0, 1, 2, 3] or (row.getValue("BSDM") in ['2','4'] and gsdj!=0):
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    gsdjBtBZ + "' where ZDMC = 'GSDJ'")
            #------------载植时间---------------
            zzsj = row.getValue("ZZSJ")
            zzsjisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'ZZSJ'")[0][0]
            if (str(zzsj).strip() == ''):
                if zzsjisBT == "是":
                    zzsjBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'ZZSJ'")
                    zzsjBtError = zzsjBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(zzsjBtError) + 1) + " where ZDMC = 'ZZSJ'")
            else:
                zzsjBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'ZZSJ'")[0][0]
                zzsjBtBZ += str(row.getValue("OBJECTID")) + ","
                if zzsj.year > 2017 or not is_valid_date(str(zzsj)) or (row.getValue("BSDM") in ['2', '3','6', '7'] and zzsj.year == 1899) or (row.getValue("BSDM") in ['1','4'] and zzsj.year != 1899) :
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    zzsjBtBZ + "' where ZDMC = 'ZZSJ'")
            #------------载植人---------------
            zzr = row.getValue("ZZR")
            zzrisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'ZZR'")[0][0]
            if (str(zzr).strip() == ''):
                if zzrisBT == "是":
                    zzrBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'ZZR'")
                    zzrBtError = zzrBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(zzrBtError) + 1) + " where ZDMC = 'ZZR'")
            else:
                zzrBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'ZZR'")[0][0]
                zzrBtBZ += str(row.getValue("OBJECTID")) + ","
                if row.getValue("BSDM") in ['1','4'] and zzr.strip() != "" :
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    zzrBtBZ + "' where ZDMC = 'ZZR'")
            #------------树高---------------
            sg = row.getValue("SG")
            sgisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'SG'")[0][0]
            if (str(sg).strip() == '' or sg == 0):
                if sgisBT == "是":
                    sgBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'SG'")
                    sgBtError = sgBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(sgBtError) + 1) + " where ZDMC = 'SG'")
            else:
                sgBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'SG'")[0][0]
                sgBtBZ += str(row.getValue("OBJECTID")) + ","
                if sg > 50:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    sgBtBZ + "' where ZDMC = 'SG'")
            #------------冠幅东西---------------
            gfdx = row.getValue("GFDX")
            gfdxisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'GFDX'")[0][0]
            if (str(gfdx).strip() == '' or gfdx == 0):
                if gfdxisBT == "是":
                    gfdxBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'GFDX'")
                    gfdxBtError = gfdxBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(gfdxBtError) + 1) + " where ZDMC = 'GFDX'")
            else:
                gfdxBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'GFDX'")[0][0]
                gfdxBtBZ += str(row.getValue("OBJECTID")) + ","
                if gfdx > 40:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    gfdxBtBZ + "' where ZDMC = 'GFDX'")
            #------------冠幅南北---------------
            GFNB = row.getValue("GFNB")
            GFNBisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'GFNB'")[0][0]
            if (str(GFNB).strip() == '' or GFNB == 0):
                if GFNBisBT == "是":
                    GFNBBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'GFNB'")
                    GFNBBtError = GFNBBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(GFNBBtError) + 1) + " where ZDMC = 'GFNB'")
            else:
                GFNBBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'GFNB'")[0][0]
                GFNBBtBZ += str(row.getValue("OBJECTID")) + ","
                if GFNB > 40:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    GFNBBtBZ + "' where ZDMC = 'GFNB'")
            #-----------冠幅平均------------
            GFPJ = row.getValue("GFPJ")
            GFPJisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'GFPJ'")[0][0]
            if row.getValue("GFNB") == 0 or row.getValue("GFDX") == 0:
                GFPJBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'GFPJ'")[0][0]
                GFPJBtBZ += str(row.getValue("OBJECTID")) + ","
                updateDataBySql("update MMBZJG  set bz = '" +
                                GFPJBtBZ + "' where ZDMC = 'GFPJ'")
            else:
                row.setValue("GFPJ",(row.getValue("GFNB")+row.getValue("GFDX"))/2)

            #------------海拔---------------
            HB = row.getValue("HB")
            HBisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'HB'")[0][0]
            if (str(HB).strip() == '' or HB == 0):
                if HBisBT == "是":
                    HBBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'HB'")
                    HBBtError = HBBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(HBBtError) + 1) + " where ZDMC = 'HB'")
            else:
                HBBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'HB'")[0][0]
                HBBtBZ += str(row.getValue("OBJECTID")) + ","
                if HB > 2900 or HB < 147:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    HBBtBZ + "' where ZDMC = 'HB'")
            #------------坡向---------------
            PX = row.getValue("PX")
            PXisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'PX'")[0][0]
            if (str(PX).strip() == '' or PX == 0):
                if PXisBT == "是":
                    PXBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'PX'")
                    PXBtError = PXBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(PXBtError) + 1) + " where ZDMC = 'PX'")
            else:
                PXBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'PX'")[0][0]
                PXBtBZ += str(row.getValue("OBJECTID")) + ","
                if PX not in [1,2,3,4,5,6,7,8,9]:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    PXBtBZ + "' where ZDMC = 'PX'")
            #------------坡度---------------
            PD = row.getValue("PD")
            PDisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'PD'")[0][0]
            if (str(PD).strip() == '' or PD == 0):
                if PDisBT == "是":
                    PDBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'PD'")
                    PDBtError = PDBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(PDBtError) + 1) + " where ZDMC = 'PD'")
            else:
                PDBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'PD'")[0][0]
                PDBtBZ += str(row.getValue("OBJECTID")) + ","
                if PD not in [1,2,3,4,5,6]:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    PDBtBZ + "' where ZDMC = 'PD'")
            #------------坡位---------------
            PW = row.getValue("PW")
            PWisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'PW'")[0][0]
            if (str(PW).strip() == '' or PW == 0):
                if PWisBT == "是":
                    PWBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'PW'")
                    PWBtError = PWBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(PWBtError) + 1) + " where ZDMC = 'PW'")
            else:
                PWBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'PW'")[0][0]
                PWBtBZ += str(row.getValue("OBJECTID")) + ","
                if PW not in [1,2,3,4,5,6,7]:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    PWBtBZ + "' where ZDMC = 'PW'")
            #------------土壤类型---------------
            trlx = row.getValue("TRLX")
            trlxisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'TRLX'")[0][0]
            if (str(trlx).strip() == '' or trlx == 0):
                if trlxisBT == "是":
                    trlxBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'TRLX'")
                    trlxBtError = trlxBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(trlxBtError) + 1) + " where ZDMC = 'TRLX'")
            else:
                trlxBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'TRLX'")[0][0]
                trlxBtBZ += str(row.getValue("OBJECTID")) + ","
                trlxdm = getDataBySql(
                    "select TRLX FROM TRLX WHERE TRLX = '" + str(trlx) + "'")
                if len(trlxdm) == 0:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    trlxBtBZ + "' where ZDMC = 'TRLX'")
            #------------土壤紧密度---------------
            TRJMD = row.getValue("TRJMD")
            TRJMDisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'TRJMD'")[0][0]
            if (str(TRJMD).strip() == '' or TRJMD == 0):
                if TRJMDisBT == "是":
                    TRJMDBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'TRJMD'")
                    TRJMDBtError = TRJMDBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(TRJMDBtError) + 1) + " where ZDMC = 'TRJMD'")
            else:
                TRJMDBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'TRJMD'")[0][0]
                TRJMDBtBZ += str(row.getValue("OBJECTID")) + ","
                if TRJMD not in [1,2,3,4,5]:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    TRJMDBtBZ + "' where ZDMC = 'TRJMD'")
            #------------生长势---------------
            SZS = row.getValue("SZS")
            SZSisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'SZS'")[0][0]
            if (str(SZS).strip() == '' or SZS == 0):
                if SZSisBT == "是":
                    SZSBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'SZS'")
                    SZSBtError = SZSBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(SZSBtError) + 1) + " where ZDMC = 'SZS'")
            else:
                SZSBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'SZS'")[0][0]
                SZSBtBZ += str(row.getValue("OBJECTID")) + ","
                if SZS not in [1,2,3,4]:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    SZSBtBZ + "' where ZDMC = 'SZS'")
            #------------生长环境---------------
            SZHJ = row.getValue("SZHJ")
            SZHJisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'SZHJ'")[0][0]
            if (str(SZHJ).strip() == '' or SZHJ == 0):
                if SZHJisBT == "是":
                    SZHJBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'SZHJ'")
                    SZHJBtError = SZHJBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(SZHJBtError) + 1) + " where ZDMC = 'SZHJ'")
            else:
                SZHJBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'SZHJ'")[0][0]
                SZHJBtBZ += str(row.getValue("OBJECTID")) + ","
                if SZHJ not in [1,2,3]:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    SZHJBtBZ + "' where ZDMC = 'SZHJ'")
            #------------影响生长的环境因素---------------
            YXSZHJYS = row.getValue("YXSZHJYS")
            YXSZHJYSisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'YXSZHJYS'")[0][0]
            if (str(YXSZHJYS).strip() == '' or YXSZHJYS == 0):
                if YXSZHJYSisBT == "是":
                    YXSZHJYSBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'YXSZHJYS'")
                    YXSZHJYSBtError = YXSZHJYSBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(YXSZHJYSBtError) + 1) + " where ZDMC = 'YXSZHJYS'")
            else:
                YXSZHJYSBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'YXSZHJYS'")[0][0]
                YXSZHJYSBtBZ += str(row.getValue("OBJECTID")) + ","
                if YXSZHJYS.strip() == "" and row.getValue("SZS" in [2,3]):
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    YXSZHJYSBtBZ + "' where ZDMC = 'YXSZHJYS'")
            #------------受害情况---------------
            SHQK = row.getValue("SHQK")
            SHQKBtBZ = getDataBySql(
                "select bz from MMBZJG where ZDMC = 'SHQK'")[0][0]
            SHQKBtBZ += str(row.getValue("OBJECTID")) + ","
            if (SHQK == 0 and row.getValue("SZS") in [2,3]) or SHQK not in [1,2,3,4]:
                updateDataBySql("update MMBZJG  set bz = '" +
                                SHQKBtBZ + "' where ZDMC = 'SHQK'")
            #------------新增原因---------------
            XZYY = row.getValue("XZYY")
            XZYYisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'XZYY'")[0][0]
            if (str(XZYY).strip() == '' or XZYY == 0):
                if XZYYisBT == "是":
                    XZYYBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'XZYY'")
                    XZYYBtError = XZYYBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(XZYYBtError) + 1) + " where ZDMC = 'XZYY'")
            else:
                XZYYBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'XZYY'")[0][0]
                XZYYBtBZ += str(row.getValue("OBJECTID")) + ","
                if XZYY in [1,2,3]:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    XZYYBtBZ + "' where ZDMC = 'XZYY'")
            #------------树种鉴定记载---------------
            SZJDJZ = row.getValue("SZJDJZ")
            SZJDJZisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'SZJDJZ'")[0][0]
            if (str(SZJDJZ).strip() == '' or SZJDJZ == 0):
                if SZJDJZisBT == "是":
                    SZJDJZBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'SZJDJZ'")
                    SZJDJZBtError = SZJDJZBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(SZJDJZBtError) + 1) + " where ZDMC = 'SZJDJZ'")
            else:
                SZJDJZBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'SZJDJZ'")[0][0]
                SZJDJZBtBZ += str(row.getValue("OBJECTID")) + ","
                if SZJDJZ not in ['县级','市级','存疑']:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    SZJDJZBtBZ + "' where ZDMC = 'SZJDJZ'")
            #------------名木介绍1---------------
            GSLS1 = row.getValue("GSLS1")
            GSLS1isBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'GSLS1'")[0][0]
            if (str(GSLS1).strip() == '' or GSLS1 == 0):
                if GSLS1isBT == "是":
                    GSLS1BtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'GSLS1'")
                    GSLS1BtError = GSLS1BtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(GSLS1BtError) + 1) + " where ZDMC = 'GSLS1'")
            else:
                GSLS1BtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'GSLS1'")[0][0]
                GSLS1BtBZ += str(row.getValue("OBJECTID")) + ","
                if GSLS1.strip()!="" and row.getValue('BSDM') == '4':
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    GSLS1BtBZ + "' where ZDMC = 'GSLS1'")
            #------------名木介绍2---------------
            GSLS2 = row.getValue("GSLS2")
            GSLS2isBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'GSLS2'")[0][0]
            if (str(GSLS2).strip() == '' or GSLS2 == 0):
                if GSLS2isBT == "是":
                    GSLS2BtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'GSLS2'")
                    GSLS2BtError = GSLS2BtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(GSLS2BtError) + 1) + " where ZDMC = 'GSLS2'")
            else:
                GSLS2BtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'GSLS2'")[0][0]
                GSLS2BtBZ += str(row.getValue("OBJECTID")) + ","
                if GSLS2.strip()!="" and row.getValue('BSDM') == '4':
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    GSLS2BtBZ + "' where ZDMC = 'GSLS2'")
            #------------名木介绍3---------------
            GSLS3 = row.getValue("GSLS3")
            GSLS3isBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'GSLS3'")[0][0]
            if (str(GSLS3).strip() == '' or GSLS3 == 0):
                if GSLS3isBT == "是":
                    GSLS3BtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'GSLS3'")
                    GSLS3BtError = GSLS3BtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(GSLS3BtError) + 1) + " where ZDMC = 'GSLS3'")
            else:
                GSLS3BtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'GSLS3'")[0][0]
                GSLS3BtBZ += str(row.getValue("OBJECTID")) + ","
                if GSLS3.strip()!="" and row.getValue('BSDM') == '4':
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    GSLS3BtBZ + "' where ZDMC = 'GSLS3'")
            #------------保护现状---------------
            BHXZ = row.getValue("BHXZ")
            BHXZisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'BHXZ'")[0][0]
            if (str(BHXZ).strip() == '' or BHXZ == 0):
                if BHXZisBT == "是":
                    BHXZBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'BHXZ'")
                    BHXZBtError = BHXZBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(BHXZBtError) + 1) + " where ZDMC = 'BHXZ'")
            else:
                BHXZBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'BHXZ'")[0][0]
                BHXZBtBZ += str(row.getValue("OBJECTID")) + ","
                if BHXZ not in [1,2,3,4,5,6,7]:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    BHXZBtBZ + "' where ZDMC = 'BHXZ'")
            #------------养护复壮现状---------------
            YHFZXZ = row.getValue("YHFZXZ")
            YHFZXZisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'YHFZXZ'")[0][0]
            if (str(YHFZXZ).strip() == '' or YHFZXZ == 0):
                if YHFZXZisBT == "是":
                    YHFZXZBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'YHFZXZ'")
                    YHFZXZBtError = YHFZXZBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(YHFZXZBtError) + 1) + " where ZDMC = 'YHFZXZ'")
            else:
                YHFZXZBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'YHFZXZ'")[0][0]
                YHFZXZBtBZ += str(row.getValue("OBJECTID")) + ","
                if YHFZXZ not in [1,2,3,4,5,6,7]:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    YHFZXZBtBZ + "' where ZDMC = 'YHFZXZ'")
            #------------照片名---------------
            zpm = row.getValue("ZPM")
            zpmisBT = getDataBySql("select ISBT from MMBZJG where ZDMC = 'ZPM'")[0][0]
            if (str(zpm).strip() == ''):
                if zpmisBT == "是":
                    zpmBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'ZPM'")
                    zpmBtError = zpmBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(zpmBtError) + 1) + " where ZDMC = 'ZPM'")
            else:
                zpmBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'ZPM'")[0][0]
                zpmBtBZ += str(row.getValue("OBJECTID")) + ","
                if zpm[0:11] != row.getValue("GDMMBH") :
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    zpmBtBZ + "' where ZDMC = 'ZPM'")
            #------------有无标本---------------
            YWBB = row.getValue("YWBB")
            YWBBisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'YWBB'")[0][0]
            if (str(YWBB).strip() == '' or YWBB == 0):
                if YWBBisBT == "是":
                    YWBBBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'YWBB'")
                    YWBBBtError = YWBBBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(YWBBBtError) + 1) + " where ZDMC = 'YWBB'")
            else:
                YWBBBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'YWBB'")[0][0]
                YWBBBtBZ += str(row.getValue("OBJECTID")) + ","
                if YWBB not in [1,2]:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    YWBBBtBZ + "' where ZDMC = 'YWBB'")
            #------------调查日期---------------
            dcrq = row.getValue("DCRQ")
            dcrqisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'DCRQ'")[0][0]
            dcrqBtError = getDataBySql(
                "select CWSL from MMBZJG where ZDMC = 'DCRQ'")
            dcrqBtError = dcrqBtError[0][0]
            if (str(dcrq).strip() == ''  or dcrq.year == 1899):
                if dcrqisBT == "是":
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(dcrqBtError) + 1) + " where ZDMC = 'DCRQ'")
            else:
                dcrqBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'DCRQ'")[0][0]
                dcrqBtBZ += str(row.getValue("OBJECTID")) + ","
                if not is_valid_date(str(dcrq)):
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    dcrqBtBZ + "' where ZDMC = 'DCRQ'")
                elif dcrq.year > 2017:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    dcrqBtBZ + "' where ZDMC = 'DCRQ'")
            #------------审核日期---------------
            shrq = row.getValue("SHRQ")
            shrqisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'SHRQ'")[0][0]
            shrqBtError = getDataBySql(
                "select CWSL from MMBZJG where ZDMC = 'SHRQ'")
            shrqBtError = shrqBtError[0][0]
            if (str(shrq).strip() == '' or shrq.year == 1899):
                if shrqisBT == "是":
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(shrqBtError) + 1) + " where ZDMC = 'SHRQ'")
            else:
                shrqBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'SHRQ'")[0][0]
                shrqBtBZ += str(row.getValue("OBJECTID")) + ","
                if not is_valid_date(str(shrq)):
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    shrqBtBZ + "' where ZDMC = 'SHRQ'")
                elif shrq.year > 2017:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    shrqBtBZ + "' where ZDMC = 'SHRQ'")
            #--------古树群图斑号---------------
            GSQTBH = row.getValue("GSQTBH")
            GSQTBHisBT = getDataBySql(
                "select ISBT from MMBZJG where ZDMC = 'GSQTBH'")[0][0]
            if (str(GSQTBH).strip() == '' or GSQTBH == 0):
                if GSQTBHisBT == "是":
                    GSQTBHBtError = getDataBySql(
                        "select CWSL from MMBZJG where ZDMC = 'GSQTBH'")
                    GSQTBHBtError = GSQTBHBtError[0][0]
                    updateDataBySql("update MMBZJG SET CWSL = " +
                                    str(int(GSQTBHBtError) + 1) + " where ZDMC = 'GSQTBH'")
            else:
                GSQTBHBtBZ = getDataBySql(
                    "select bz from MMBZJG where ZDMC = 'GSQTBH'")[0][0]
                GSQTBHBtBZ += str(row.getValue("OBJECTID")) + ","
                if GSQTBH not in TBHArrar or row.getValue("BSDM") in ['2','4']:
                    updateDataBySql("update MMBZJG  set bz = '" +
                                    GSQTBHBtBZ + "' where ZDMC = 'GSQTBH'")
            data.updateRow(row)
        arcpy.AddMessage("古树名木逻辑检查完成")

    def exportResult():
        if (os.path.exists(OutFolder+"\\古树群调查.shp")):
            arcpy.Delete_management(OutFolder+"\\古树群调查.shp")
        if (os.path.exists(OutFolder+"\\古树大树名木每木调查.shp")):
            arcpy.Delete_management(OutFolder+"\\古树大树名木每木调查.shp")
        arcpy.CopyFeatures_management(outFildGsq, OutFolder+"\\古树群调查.shp")
        arcpy.CopyFeatures_management(outFildGsmm, OutFolder+"\\古树大树名木每木调查.shp")
        arcpy.AddMessage("成果导出完成")

    def addExcel():
        outPath = OutFolder+"\\质检结果.xls"
        if (os.path.exists(outPath)):
            os.remove(outPath)
        workbook = xlwt.Workbook(encoding = 'utf-8')
        sheet = workbook.add_sheet('Summary')
        font0 = xlwt.Font()
        font0.bold = True
        font0.size = 30
        alignment = xlwt.Alignment() #创建居中
        alignment.horz = xlwt.Alignment.HORZ_CENTER
        borders = xlwt.Borders()
        borders.left = 1
        borders.right = 1
        borders.top = 1
        borders.bottom = 1
        borders.bottom_colour=0x3A
        style0 = xlwt.XFStyle()
        style0.font = font0
        style0.alignment = alignment
        style0.borders = borders

        sheet.write(0,0,'质检项名称',style0)
        sheet.write(0,1,'质检内容',style0)
        sheet.write(0,2,'质检状态',style0)
        sheet.write(0,3,'错误数',style0)
        sheet.write(0,4,'备注',style0)
        col0=sheet.col(0)
        col1=sheet.col(1)
        col2=sheet.col(2)
        col3=sheet.col(3)
        col4=sheet.col(4)
        col0.width=256*20
        col1.width=256*50
        col2.width=256*10
        col3.width=256*10
        col4.width=256*80
        workbook.save(outPath)

    def writeExcel(col1,col2,col3,col4,col5,ori):
        outPath = OutFolder+u"\\质检结果.xls"
        if (not os.path.exists(outPath)):
            addExcel()
        workbook = xlrd.open_workbook(outPath,formatting_info=True)
        sheet1 = workbook.sheet_by_index(0)
        rows = sheet1.nrows
        excel = copy(workbook)
        table = excel.get_sheet(0)

        style = xlwt.XFStyle()
        styleFirst = xlwt.XFStyle()
        alignment = xlwt.Alignment() #创建居中
        alignment.horz = ori
        borders = xlwt.Borders()
        borders.left = 1
        borders.right = 1
        borders.top = 1
        borders.bottom = 1
        borders.bottom_colour=0x3A
        style.borders = borders
        styleFirst.alignment = alignment
        styleFirst.borders = borders

        table.write(rows, 0, col1,styleFirst)
        table.write(rows, 1, col2,style)
        table.write(rows, 2, col3,style)
        table.write(rows, 3, col4,style)
        table.write(rows, 4, col5,style)
        excel.save(outPath)

    def exportError(tableName):
        if(tableName=="GSQBZJG"):
            writeExcel(u"古树群调查表：","","","","",1)
        else:
            writeExcel(u"古树名木调查表：","","","","",1)
        errorData = getDataBySql("select ZDMC,JCNR,CWSL,BZ from "+tableName+" where BZ <> '' or CWSL <>0")
        for errorRow in errorData:
            writeExcel(u"      "+errorRow[0],errorRow[1],u"未通过",errorRow[2],errorRow[3],3)

    addExcel()
    initData()#清空错误缓存
    initTbhArr()#初始化古树群图斑号数组
    gsqFieldList = arcpy.ListFields(GSQTB)#古树群字段
    mmFieldList = arcpy.ListFields(MMTB)#古树名木字段
    checkTb(gsqFieldList,'GSQBZJG')#检查古树群结构
    checkTb(mmFieldList,'MMBZJG')#检查古树名木结构
    if isCheckTB:#通过结构检查  才能进行数据质检
        SpatialJoin(GSQTB,"GSQTB")#自动挂二调县乡村
        SpatialJoin(MMTB,'MMTB')#自动挂二调县乡村
        CheckZCD()#检查自重叠
        CheckDBJ()#检查多部键
        CheckZXJ()#检查自相交
        gsqCursor = arcpy.UpdateCursor(outFildGsq)#古树群数据
        mmCursor = arcpy.UpdateCursor(outFildGsmm)#古树名木数据
        arcpy.AddMessage("逻辑检查开始")
        checkGsqData(gsqCursor)#古树群逻辑检查
        checkMmData(mmCursor)#古树名木逻辑检查
        exportResult()#导出成果
        exportError('GSQBZJG')
        exportError('MMBZJG')
        #deleteTemp()#删除缓存数据

except arcpy.ExecuteError:
    arcpy.AddMessage("error")
    arcpy.AddMessage(arcpy.GetMessages())

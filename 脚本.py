from arcpy import env
import time
import sys
import pypyodbc
reload(sys)
sys.setdefaultencoding('utf-8')

try:
    global isCheckTB
    isCheckTB = True
    env.workspace = r"E：/temp"
    xzjx = "E:/BASEDB.mdb/dataset/gzxzjx"
    outFild = "E:/BASEDB.mdb/dataset/outFild"
    joinDataCopy = "E:/BASEDB.mdb/dataset/joinDataCopy"
    topo_dataset_path = "E:/BASEDB.mdb/dataset"

    def initData():
        updateDataBySql("update GSQBZJG set CWSL = 0,BZ=''")

    def is_valid_date(dateStr):
        try:
            if " " in dateStr:
                dateStr = dateStr.split(" ")[0]
            time.strptime(dateStr, "%Y-%m-%d")
            return True
        except:
            return False

    def deleteTemp():
        arcpy.Delete_management(outFild)
        arcpy.Delete_management(joinDataCopy)
        arcpy.Delete_management(topo_dataset_path + "\\my_topo_error_poly")
        arcpy.Delete_management(topo_dataset_path + "\\my_topo_error_line")
        arcpy.Delete_management(topo_dataset_path + "\\my_topo_error_point")
        arcpy.Delete_management(topo_dataset_path + "\\" + "checkZCD")

    def checkTb(fieldList):
        gszd = getDataBySql('select ZDMC,ZDLX from GSQBZJG')
        gszdArr = []
        fieldObj = {}
        for field in fieldList:
            fieldObj[field.baseName] = field.type
        for field in gszd:
            gszdArr.append(field[0])
            if (field[0] not in fieldObj or field[1] != fieldObj[field[0]]):
                arcpy.AddMessage("缺少字段或字段类型不对：" + field[0])
                isCheckTB = False
        for key in fieldObj:
            if key not in gszdArr and key not in ['SHAPE_Leng', 'OBJECTID', 'FID', 'SHAPE_Area', 'Shape']:
                arcpy.AddMessage("多余字段" + str(key))
                isCheckTB = False

    def getDataBySql(sql):
        constr = 'Driver={Microsoft Access Driver (*.mdb)};DBQ=E:\\BASEDB.mdb'
        conn = pypyodbc.win_connect_mdb(constr)
        cur = conn.cursor()
        cur.execute(sql)
        result = cur.fetchall()
        conn.commit()
        cur.close()
        conn.close()
        return result

    def updateDataBySql(sql):
        constr = 'Driver={Microsoft Access Driver (*.mdb)};DBQ=E:\\BASEDB.mdb'
        conn = pypyodbc.win_connect_mdb(constr)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()

    def SpatialJoin(joinData):
        arcpy.AddMessage(joinData)
        arcpy.CopyFeatures_management(joinData, joinDataCopy)
        arcpy.DeleteField_management(joinDataCopy, ["XIAN", "XIANG", "CUN"])
        arcpy.AddMessage("copy OK")
        arcpy.SpatialJoin_analysis(joinDataCopy, xzjx, outFild, "#", "#", "#")
        arcpy.AddMessage("SpatialJoin_analysis OK")
        arcpy.DeleteField_management(outFild, ["Join_Count", "TARGET_FID"])
        for field in arcpy.ListFields(outFild):
            if field.baseName[-2:] == "_1" and field.editable == True:
                arcpy.DeleteField_management(outFild, [field.baseName])

    def CheckTopology():
        checkData = topo_dataset_path + "\\outFild"
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
        topoCursor = arcpy.UpdateCursor(
            topo_dataset_path + "\\my_topo_error_poly")

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
                tbhBtBZstr += str(row.getValue("FID")) + ","
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
                zyszBtBZ += str(row.getValue("FID")) + ","
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
                gszsBtBZ += str(row.getValue("FID")) + ","
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
                lfpjgBtBZ += str(row.getValue("FID")) + ","
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
                lfpjxjBtBZ += str(row.getValue("FID")) + ","
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
                pjslBtBZ += str(row.getValue("FID")) + ","
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
                hbBtBZ += str(row.getValue("FID")) + ","
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
                pdBtBZ += str(row.getValue("FID")) + ","
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
                pxBtBZ += str(row.getValue("FID")) + ","
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
                trlxBtBZ += str(row.getValue("FID")) + ","
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
                tchdBtBZ += str(row.getValue("FID")) + ","
                if tchd > 200:
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    tchdBtBZ + "' where ZDMC = 'TCHD'")
            #------------下木种类---------------
            xmzl = row.getValue("XMZL")
            xmzlBtBZ = getDataBySql("select bz from GSQBZJG where ZDMC = 'XMZL'")[0][0]
            xmzlBtBZ += str(row.getValue("FID")) + ","
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
            xmmdBtBZ += str(row.getValue("FID")) + ","
            if xmmd > 1000:
                updateDataBySql("update GSQBZJG  set bz = '" +
                                xmmdBtBZ + "' where ZDMC = 'XMMD'")
            #------------地被物种类---------------
            dbwzl = row.getValue("DBWZL")
            dbwzlBtBZ = getDataBySql(
                "select bz from GSQBZJG where ZDMC = 'DBWZL'")[0][0]
            dbwzlBtBZ += str(row.getValue("FID")) + ","
            if (dbwzl.strip() == '' and row.getValue("DBWMD") > 0):
                updateDataBySql("update GSQBZJG  set bz = '" +
                                dbwzlBtBZ + "' where ZDMC = 'DBWZL'")
            #------------地被物密度---------------
            dbwmd = row.getValue("DBWMD")
            dbwmdBtBZ = getDataBySql(
                "select bz from GSQBZJG where ZDMC = 'DBWMD'")[0][0]
            dbwmdBtBZ += str(row.getValue("FID")) + ","
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
                mdbhszBtBZ += str(row.getValue("FID")) + ","
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
                mdbSZKBtBZ += str(row.getValue("FID")) + ","
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
                MDSZSBtBZ += str(row.getValue("FID")) + ","
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
                zpmBtBZ += str(row.getValue("FID")) + ","
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
            if (str(dcrq).strip() == ''):
                if dcrqisBT == "是":
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(dcrqBtError) + 1) + " where ZDMC = 'DCRQ'")
            else:
                dcrqBtBZ = getDataBySql(
                    "select bz from GSQBZJG where ZDMC = 'DCRQ'")[0][0]
                dcrqBtBZ += str(row.getValue("FID")) + ","
                if not is_valid_date(str(dcrq)):
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    dcrqBtBZ + "' where ZDMC = 'DCRQ'")
                elif dcrq.year > 2017:
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    dcrqBtBZ + "' where ZDMC = 'DCRQ'")
                elif dcrq.year == 1899:
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(dcrqBtError) + 1) + " where ZDMC = 'DCRQ'")
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
            if (str(shrq).strip() == ''):
                if shrqisBT == "是":
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(shrqBtError) + 1) + " where ZDMC = 'SHRQ'")
            else:
                shrqBtBZ = getDataBySql(
                    "select bz from GSQBZJG where ZDMC = 'SHRQ'")[0][0]
                shrqBtBZ += str(row.getValue("FID")) + ","
                if not is_valid_date(str(shrq)):
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    shrqBtBZ + "' where ZDMC = 'SHRQ'")
                elif shrq.year > 2017:
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    shrqBtBZ + "' where ZDMC = 'SHRQ'")
                elif shrq.year == 1899:
                    updateDataBySql("update GSQBZJG SET CWSL = " +
                                    str(int(shrqBtError) + 1) + " where ZDMC = 'SHRQ'")
            #------------古树群编号---------------
            NewGsqBh = row.getValue("XIAN")+row.getValue("TBH")
            #row.setValue("GSQBH", row.getValue("XIAN"))
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
                    gsqbhBtBZ += str(row.getValue("FID")) + ","
                    updateDataBySql("update GSQBZJG  set bz = '" +
                                    gsqbhBtBZ + "' where ZDMC = 'GSQBH'")

    def checkMmData(data):

    #GSQTB = arcpy.GetParameterAsText(0)  # 古树群调查表
    MMTB = arcpy.GetParameterAsText(1)  # 每木调查表
    #OutFolder = arcpy.GetParameterAsText(2)  # 输出目录
    #gsqCursor = arcpy.UpdateCursor(GSQTB)
    mmCursor = arcpy.UpdateCursor(MMTB)
    #gsqFieldList = arcpy.ListFields(GSQTB)
    mmFieldList = arcpy.ListFields(MMTB)
    initData()
    # checkTb(gsqFieldList)
    if True:
        for field in mmFieldList:
        # SpatialJoin(GSQTB)
        # CheckTopology()
        #checkGsqData(gsqCursor)
        #checkMmData(mmCursor)
except arcpy.ExecuteError:
    arcpy.AddMessage("error")

    arcpy.AddMessage(arcpy.GetMessages())

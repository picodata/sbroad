import {newTarantoolClient, openVinFile, query} from 'k6/x/dtm';

const client = newTarantoolClient("admin:app-cluster-cookie@localhost:3301");

export function stressQuery() {
    return `SELECT *
    FROM (SELECT "vehicleguid", "reestrid", "reestrstatus", "vehicleregno", "vehiclevin", "vehiclevin2", "vehiclechassisnum", "vehiclereleaseyear", "operationregdoctypename", "operationregdoc", "operationregdocissuedate", "operationregdoccomments", "vehicleptstypename", "vehicleptsnum", "vehicleptsissuedate", "vehicleptsissuer", "vehicleptscomments", "vehiclebodycolor", "vehiclebrand", "vehiclemodel", "vehiclebrandmodel", "vehiclebodynum", "vehiclecost", "vehiclegasequip", "vehicleproducername", "vehiclegrossmass", "vehiclemass", "vehiclesteeringwheeltypeid", "vehiclekpptype", "vehicletransmissiontype", "vehicletypename", "vehiclecategory", "vehicletypeunit", "vehicleecoclass", "vehiclespecfuncname", "vehicleenclosedvolume", "vehicleenginemodel", "vehicleenginenum", "vehicleenginepower", "vehicleenginepowerkw", "vehicleenginetype", "holdrestrictiondate", "approvalnum", "approvaldate", "approvaltype", "utilizationfeename", "customsdoc", "customsdocdate", "customsdocissue", "customsdocrestriction", "customscountryremovalid", "customscountryremovalname", "ownerorgname", "ownerinn", "ownerogrn", "ownerkpp", "ownerpersonlastname", "ownerpersonfirstname", "ownerpersonmiddlename", "ownerpersonbirthdate", "ownerbirthplace", "ownerpersonogrnip", "owneraddressindex", "owneraddressmundistrict", "owneraddresssettlement", "owneraddressstreet", "ownerpersoninn", "ownerpersondoccode", "ownerpersondocnum", "ownerpersondocdate", "operationname", "operationdate", "operationdepartmentname", "operationattorney", "operationlising", "holdertypeid", "holderpersondoccode", "holderpersondocnum", "holderpersondocdate", "holderpersondocissuer", "holderpersonlastname", "holderpersonfirstname", "holderpersonmiddlename", "holderpersonbirthdate", "holderpersonbirthregionid", "holderpersonsex", "holderpersonbirthplace", "holderpersoninn", "holderpersonsnils", "holderpersonogrnip", "holderaddressguid", "holderaddressregionid", "holderaddressregionname", "holderaddressdistrict", "holderaddressmundistrict", "holderaddresssettlement", "holderaddressstreet", "holderaddressbuilding", "holderaddressstructureid", "holderaddressstructurename", "holderaddressstructure"
    FROM "vehicle_history"
    WHERE "sys_from" <= 332 AND "sys_to" >= 332
    UNION ALL
    SELECT "vehicleguid", "reestrid", "reestrstatus", "vehicleregno", "vehiclevin", "vehiclevin2", "vehiclechassisnum", "vehiclereleaseyear", "operationregdoctypename", "operationregdoc", "operationregdocissuedate", "operationregdoccomments", "vehicleptstypename", "vehicleptsnum", "vehicleptsissuedate", "vehicleptsissuer", "vehicleptscomments", "vehiclebodycolor", "vehiclebrand", "vehiclemodel", "vehiclebrandmodel", "vehiclebodynum", "vehiclecost", "vehiclegasequip", "vehicleproducername", "vehiclegrossmass", "vehiclemass", "vehiclesteeringwheeltypeid", "vehiclekpptype", "vehicletransmissiontype", "vehicletypename", "vehiclecategory", "vehicletypeunit", "vehicleecoclass", "vehiclespecfuncname", "vehicleenclosedvolume", "vehicleenginemodel", "vehicleenginenum", "vehicleenginepower", "vehicleenginepowerkw", "vehicleenginetype", "holdrestrictiondate", "approvalnum", "approvaldate", "approvaltype", "utilizationfeename", "customsdoc", "customsdocdate", "customsdocissue", "customsdocrestriction", "customscountryremovalid", "customscountryremovalname", "ownerorgname", "ownerinn", "ownerogrn", "ownerkpp", "ownerpersonlastname", "ownerpersonfirstname", "ownerpersonmiddlename", "ownerpersonbirthdate", "ownerbirthplace", "ownerpersonogrnip", "owneraddressindex", "owneraddressmundistrict", "owneraddresssettlement", "owneraddressstreet", "ownerpersoninn", "ownerpersondoccode", "ownerpersondocnum", "ownerpersondocdate", "operationname", "operationdate", "operationdepartmentname", "operationattorney", "operationlising", "holdertypeid", "holderpersondoccode", "holderpersondocnum", "holderpersondocdate", "holderpersondocissuer", "holderpersonlastname", "holderpersonfirstname", "holderpersonmiddlename", "holderpersonbirthdate", "holderpersonbirthregionid", "holderpersonsex", "holderpersonbirthplace", "holderpersoninn", "holderpersonsnils", "holderpersonogrnip", "holderaddressguid", "holderaddressregionid", "holderaddressregionname", "holderaddressdistrict", "holderaddressmundistrict", "holderaddresssettlement", "holderaddressstreet", "holderaddressbuilding", "holderaddressstructureid", "holderaddressstructurename", "holderaddressstructure"
    FROM "vehicle_actual"
    WHERE "sys_from" <= 332) AS "t3"
    WHERE "reestrid" = 452842574`
}

export default () => {
    query(client, stressQuery())
}